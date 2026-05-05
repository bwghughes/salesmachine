package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := openStore(path)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { s.db.Close() })
	return s
}

func TestIsStage(t *testing.T) {
	for _, ok := range []string{"lead", "qualified", "proposal", "negotiation", "won", "lost"} {
		if !isStage(ok) {
			t.Errorf("isStage(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "LEAD", "closed", "foo"} {
		if isStage(bad) {
			t.Errorf("isStage(%q) = true, want false", bad)
		}
	}
}

func TestNewIDUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := newID()
		if len(id) != 12 {
			t.Fatalf("len(newID())=%d, want 12", len(id))
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestAppendAndLoad(t *testing.T) {
	s := newTestStore(t)
	id := newID()
	if _, err := s.append(id, "deal", EvtDealCreated, 0, DealCreatedPayload{
		Title: "T", Customer: "C", Amount: 100, Stage: "lead",
	}); err != nil {
		t.Fatalf("append created: %v", err)
	}
	if _, err := s.append(id, "deal", EvtStageChanged, 1,
		StageChangedPayload{From: "lead", To: "qualified"}); err != nil {
		t.Fatalf("append stage: %v", err)
	}
	events, err := s.load(id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Version != 1 || events[1].Version != 2 {
		t.Errorf("versions = %d,%d; want 1,2", events[0].Version, events[1].Version)
	}
	if events[0].Type != EvtDealCreated || events[1].Type != EvtStageChanged {
		t.Errorf("event types wrong: %+v", events)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	s := newTestStore(t)
	id := newID()
	if _, err := s.append(id, "deal", EvtDealCreated, 0, DealCreatedPayload{Stage: "lead"}); err != nil {
		t.Fatal(err)
	}
	// Two writers both believe version is 1.
	if _, err := s.append(id, "deal", EvtNoteAdded, 1, NoteAddedPayload{Note: "a"}); err != nil {
		t.Fatalf("first concurrent append: %v", err)
	}
	if _, err := s.append(id, "deal", EvtNoteAdded, 1, NoteAddedPayload{Note: "b"}); err == nil {
		t.Fatal("second append at same expectedVersion should fail")
	}
}

func TestRehydrateNotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.rehydrate("nope"); err == nil {
		t.Fatal("expected error for missing aggregate")
	}
}

func TestRehydrateReplaysAllEvents(t *testing.T) {
	s := newTestStore(t)
	id := newID()
	mustAppend := func(et string, ver int, p any) {
		t.Helper()
		if _, err := s.append(id, "deal", et, ver, p); err != nil {
			t.Fatal(err)
		}
	}
	mustAppend(EvtDealCreated, 0, DealCreatedPayload{Title: "Big", Customer: "Acme", Amount: 100, Stage: "lead"})
	mustAppend(EvtStageChanged, 1, StageChangedPayload{From: "lead", To: "qualified"})
	mustAppend(EvtAmountUpdated, 2, AmountUpdatedPayload{From: 100, To: 250})
	mustAppend(EvtNoteAdded, 3, NoteAddedPayload{Note: "called CFO"})
	mustAppend(EvtNoteAdded, 4, NoteAddedPayload{Note: "sent proposal"})
	mustAppend(EvtDealWon, 5, struct{}{})

	d, err := s.rehydrate(id)
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if d.ID != id || d.Title != "Big" || d.Customer != "Acme" {
		t.Errorf("identity wrong: %+v", d)
	}
	if d.Amount != 250 {
		t.Errorf("amount=%v, want 250", d.Amount)
	}
	if d.Stage != "won" {
		t.Errorf("stage=%q, want won", d.Stage)
	}
	if d.Version != 6 {
		t.Errorf("version=%d, want 6", d.Version)
	}
	if len(d.Notes) != 2 || d.Notes[0] != "called CFO" || d.Notes[1] != "sent proposal" {
		t.Errorf("notes wrong: %+v", d.Notes)
	}
	if d.CreatedAt.IsZero() || d.UpdatedAt.IsZero() {
		t.Error("timestamps not set")
	}
	if d.UpdatedAt.Before(d.CreatedAt) {
		t.Error("UpdatedAt < CreatedAt")
	}
}

func TestApplyUnknownEvent(t *testing.T) {
	d := &Deal{}
	err := d.apply(Event{Type: "Bogus", Payload: json.RawMessage(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "unknown event") {
		t.Fatalf("got %v, want unknown event error", err)
	}
}

func TestApplyBadPayload(t *testing.T) {
	d := &Deal{}
	if err := d.apply(Event{Type: EvtDealCreated, Payload: json.RawMessage(`not json`)}); err == nil {
		t.Fatal("expected json error")
	}
}

func TestAllAggregateIDs(t *testing.T) {
	s := newTestStore(t)
	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		id := newID()
		ids[id] = true
		if _, err := s.append(id, "deal", EvtDealCreated, 0,
			DealCreatedPayload{Stage: "lead"}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.allAggregateIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d ids, want 3", len(got))
	}
	for _, id := range got {
		if !ids[id] {
			t.Errorf("unexpected id %q", id)
		}
	}
}

func TestLoadOrderedByVersion(t *testing.T) {
	s := newTestStore(t)
	id := newID()
	for i := 0; i < 5; i++ {
		var p any
		et := EvtNoteAdded
		if i == 0 {
			et = EvtDealCreated
			p = DealCreatedPayload{Stage: "lead"}
		} else {
			p = NoteAddedPayload{Note: "n"}
		}
		if _, err := s.append(id, "deal", et, i, p); err != nil {
			t.Fatal(err)
		}
	}
	events, err := s.load(id)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Version != i+1 {
			t.Errorf("events[%d].Version=%d, want %d", i, e.Version, i+1)
		}
	}
}
