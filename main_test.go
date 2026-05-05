package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testCtx returns a context with a generous deadline for the test, cancelled
// when the test ends.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := openStore(testCtx(t), path)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// mustID is a test helper that fails the test if newID errors.
func mustID(t *testing.T) string {
	t.Helper()
	id, err := newID()
	if err != nil {
		t.Fatalf("newID: %v", err)
	}
	return id
}

// envFunc builds a getenv backed by a static map. Useful for run() tests.
func envFunc(m map[string]string) getenv {
	return func(k string) string { return m[k] }
}

// ---------- Pure functions --------------------------------------------------

func TestIsStage(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"lead", true},
		{"qualified", true},
		{"proposal", true},
		{"negotiation", true},
		{"won", true},
		{"lost", true},
		{"", false},
		{"LEAD", false},
		{"closed", false},
		{"foo", false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := isStage(tc.in); got != tc.want {
				t.Errorf("isStage(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"won", true},
		{"lost", true},
		{"lead", false},
		{"", false},
	} {
		if got := isTerminal(tc.in); got != tc.want {
			t.Errorf("isTerminal(%q)=%v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestNewIDUnique(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id, err := newID()
		if err != nil {
			t.Fatalf("newID: %v", err)
		}
		if len(id) != 12 {
			t.Fatalf("len(newID())=%d, want 12", len(id))
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

// ---------- apply (table-driven) -------------------------------------------

func TestApply(t *testing.T) {
	mkRaw := func(t *testing.T, v any) json.RawMessage {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	t0 := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name    string
		seed    Deal
		event   Event
		wantErr string
		check   func(t *testing.T, d *Deal)
	}{
		{
			name: "DealCreated sets identity and stage",
			event: Event{
				AggregateID: "abc", Type: EvtDealCreated, Version: 1, CreatedAt: t0,
				Payload: mkRaw(t, DealCreatedPayload{Title: "T", Customer: "C", Amount: 10, Stage: "lead"}),
			},
			check: func(t *testing.T, d *Deal) {
				if d.ID != "abc" || d.Title != "T" || d.Customer != "C" || d.Amount != 10 || d.Stage != "lead" {
					t.Errorf("bad projection: %+v", d)
				}
				if !d.CreatedAt.Equal(t0) || !d.UpdatedAt.Equal(t0) {
					t.Errorf("timestamps: %+v", d)
				}
			},
		},
		{
			name: "StageChanged updates stage only",
			seed: Deal{Stage: "lead", Amount: 100},
			event: Event{
				Type: EvtStageChanged, Version: 2, CreatedAt: t0,
				Payload: mkRaw(t, StageChangedPayload{From: "lead", To: "qualified"}),
			},
			check: func(t *testing.T, d *Deal) {
				if d.Stage != "qualified" || d.Amount != 100 {
					t.Errorf("bad: %+v", d)
				}
			},
		},
		{
			name: "AmountUpdated",
			event: Event{
				Type: EvtAmountUpdated, Version: 3, CreatedAt: t0,
				Payload: mkRaw(t, AmountUpdatedPayload{From: 0, To: 999}),
			},
			check: func(t *testing.T, d *Deal) {
				if d.Amount != 999 {
					t.Errorf("amount=%v", d.Amount)
				}
			},
		},
		{
			name: "NoteAdded appends",
			seed: Deal{Notes: []string{"first"}},
			event: Event{
				Type: EvtNoteAdded, Version: 4, CreatedAt: t0,
				Payload: mkRaw(t, NoteAddedPayload{Note: "second"}),
			},
			check: func(t *testing.T, d *Deal) {
				if len(d.Notes) != 2 || d.Notes[1] != "second" {
					t.Errorf("notes: %+v", d.Notes)
				}
			},
		},
		{
			name:  "DealWon sets terminal",
			seed:  Deal{Stage: "negotiation"},
			event: Event{Type: EvtDealWon, Version: 5, CreatedAt: t0},
			check: func(t *testing.T, d *Deal) {
				if d.Stage != "won" {
					t.Errorf("stage=%q", d.Stage)
				}
			},
		},
		{
			name:  "DealLost sets terminal",
			seed:  Deal{Stage: "negotiation"},
			event: Event{Type: EvtDealLost, Version: 5, CreatedAt: t0},
			check: func(t *testing.T, d *Deal) {
				if d.Stage != "lost" {
					t.Errorf("stage=%q", d.Stage)
				}
			},
		},
		{
			name:    "unknown event type",
			event:   Event{Type: "Bogus", Payload: json.RawMessage(`{}`)},
			wantErr: "unknown event type",
		},
		{
			name:    "bad JSON in DealCreated",
			event:   Event{Type: EvtDealCreated, Payload: json.RawMessage(`not json`)},
			wantErr: "unmarshal DealCreated",
		},
		{
			name:    "bad JSON in StageChanged",
			event:   Event{Type: EvtStageChanged, Payload: json.RawMessage(`oops`)},
			wantErr: "unmarshal StageChanged",
		},
		{
			name:    "bad JSON in AmountUpdated",
			event:   Event{Type: EvtAmountUpdated, Payload: json.RawMessage(`oops`)},
			wantErr: "unmarshal AmountUpdated",
		},
		{
			name:    "bad JSON in NoteAdded",
			event:   Event{Type: EvtNoteAdded, Payload: json.RawMessage(`oops`)},
			wantErr: "unmarshal NoteAdded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.seed
			err := d.apply(tc.event)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if d.Version != tc.event.Version {
				t.Errorf("version=%d, want %d", d.Version, tc.event.Version)
			}
			if !d.UpdatedAt.Equal(tc.event.CreatedAt) {
				t.Errorf("UpdatedAt=%v, want %v", d.UpdatedAt, tc.event.CreatedAt)
			}
			if tc.check != nil {
				tc.check(t, &d)
			}
		})
	}
}

// ---------- Store -----------------------------------------------------------

func TestAppendAndLoad(t *testing.T) {
	ctx := testCtx(t)
	s := newTestStore(t)
	id := mustID(t)
	if _, err := s.append(ctx, id, "deal", EvtDealCreated, 0, DealCreatedPayload{
		Title: "T", Customer: "C", Amount: 100, Stage: "lead",
	}); err != nil {
		t.Fatalf("append created: %v", err)
	}
	if _, err := s.append(ctx, id, "deal", EvtStageChanged, 1,
		StageChangedPayload{From: "lead", To: "qualified"}); err != nil {
		t.Fatalf("append stage: %v", err)
	}
	events, err := s.load(ctx, id)
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
	ctx := testCtx(t)
	s := newTestStore(t)
	id := mustID(t)
	if _, err := s.append(ctx, id, "deal", EvtDealCreated, 0, DealCreatedPayload{Stage: "lead"}); err != nil {
		t.Fatal(err)
	}
	// Two writers both believe version is 1.
	if _, err := s.append(ctx, id, "deal", EvtNoteAdded, 1, NoteAddedPayload{Note: "a"}); err != nil {
		t.Fatalf("first concurrent append: %v", err)
	}
	if _, err := s.append(ctx, id, "deal", EvtNoteAdded, 1, NoteAddedPayload{Note: "b"}); err == nil {
		t.Fatal("second append at same expectedVersion should fail")
	}
}

func TestAppendCancelledContext(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before use
	_, err := s.append(ctx, "x", "deal", EvtDealCreated, 0, DealCreatedPayload{Stage: "lead"})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v, want context.Canceled wrapped", err)
	}
}

func TestRehydrateNotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.rehydrate(testCtx(t), "nope"); err == nil {
		t.Fatal("expected error for missing aggregate")
	}
}

func TestRehydrateReplaysAllEvents(t *testing.T) {
	ctx := testCtx(t)
	s := newTestStore(t)
	id := mustID(t)
	mustAppend := func(et string, ver int, p any) {
		t.Helper()
		if _, err := s.append(ctx, id, "deal", et, ver, p); err != nil {
			t.Fatal(err)
		}
	}
	mustAppend(EvtDealCreated, 0, DealCreatedPayload{Title: "Big", Customer: "Acme", Amount: 100, Stage: "lead"})
	mustAppend(EvtStageChanged, 1, StageChangedPayload{From: "lead", To: "qualified"})
	mustAppend(EvtAmountUpdated, 2, AmountUpdatedPayload{From: 100, To: 250})
	mustAppend(EvtNoteAdded, 3, NoteAddedPayload{Note: "called CFO"})
	mustAppend(EvtNoteAdded, 4, NoteAddedPayload{Note: "sent proposal"})
	mustAppend(EvtDealWon, 5, struct{}{})

	d, err := s.rehydrate(ctx, id)
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

func TestAllAggregateIDs(t *testing.T) {
	ctx := testCtx(t)
	s := newTestStore(t)
	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		id := mustID(t)
		ids[id] = true
		if _, err := s.append(ctx, id, "deal", EvtDealCreated, 0,
			DealCreatedPayload{Stage: "lead"}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.allAggregateIDs(ctx)
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
	ctx := testCtx(t)
	s := newTestStore(t)
	id := mustID(t)
	for i := 0; i < 5; i++ {
		var p any
		et := EvtNoteAdded
		if i == 0 {
			et = EvtDealCreated
			p = DealCreatedPayload{Stage: "lead"}
		} else {
			p = NoteAddedPayload{Note: "n"}
		}
		if _, err := s.append(ctx, id, "deal", et, i, p); err != nil {
			t.Fatal(err)
		}
	}
	events, err := s.load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Version != i+1 {
			t.Errorf("events[%d].Version=%d, want %d", i, e.Version, i+1)
		}
	}
}

func TestOpenStoreBadPath(t *testing.T) {
	// A directory that doesn't exist should fail at schema-init time.
	_, err := openStore(testCtx(t), filepath.Join(t.TempDir(), "no", "such", "dir", "x.db"))
	if err == nil {
		t.Fatal("expected error opening db in nonexistent dir")
	}
}

// ---------- run() / CLI integration ----------------------------------------

// runCLI invokes run with a fresh DB under t.TempDir() and returns
// (exitCode, stdout, stderr).
func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cli.db")
	return runCLIWithDB(t, dbPath, args...)
}

func runCLIWithDB(t *testing.T, dbPath string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	env := envFunc(map[string]string{"SALESMACHINE_DB": dbPath})
	code := run(testCtx(t), args, env, &out, &errb)
	return code, out.String(), errb.String()
}

func TestRunNoArgsShowsUsage(t *testing.T) {
	code, out, _ := runCLI(t)
	if code != exitUsage {
		t.Errorf("code=%d, want %d", code, exitUsage)
	}
	if !strings.Contains(out, "salesmachine") {
		t.Errorf("stdout missing usage: %q", out)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	code, out, _ := runCLI(t, "bogus")
	if code != exitUsage {
		t.Errorf("code=%d, want %d", code, exitUsage)
	}
	if !strings.Contains(out, "salesmachine") {
		t.Errorf("expected usage on stdout, got %q", out)
	}
}

func TestRunCreateAndShow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cli.db")

	code, out, errb := runCLIWithDB(t, dbPath, "create", "Big Deal", "Acme", "1000")
	if code != exitOK {
		t.Fatalf("create code=%d stderr=%q", code, errb)
	}
	id := strings.TrimSpace(out)
	if len(id) != 12 {
		t.Fatalf("expected 12-char id, got %q", id)
	}

	code, out, errb = runCLIWithDB(t, dbPath, "show", id)
	if code != exitOK {
		t.Fatalf("show code=%d stderr=%q", code, errb)
	}
	for _, want := range []string{id, "Big Deal", "Acme", "1000.00", "lead"} {
		if !strings.Contains(out, want) {
			t.Errorf("show output missing %q:\n%s", want, out)
		}
	}
}

func TestRunFullLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cli.db")

	code, out, _ := runCLIWithDB(t, dbPath, "create", "Big", "Acme", "100")
	if code != exitOK {
		t.Fatal("create failed")
	}
	id := strings.TrimSpace(out)

	cases := [][]string{
		{"move", id, "qualified"},
		{"amount", id, "250"},
		{"amount", id, "250"}, // no-op duplicate
		{"note", id, "called", "CFO"},
		{"move", id, "won"},
	}
	for _, args := range cases {
		if code, _, errb := runCLIWithDB(t, dbPath, args...); code != exitOK {
			t.Fatalf("%v: code=%d stderr=%q", args, code, errb)
		}
	}

	// move on a terminal deal must fail.
	code, _, errb := runCLIWithDB(t, dbPath, "move", id, "lost")
	if code != exitErr {
		t.Errorf("move on terminal: code=%d stderr=%q", code, errb)
	}
	if !strings.Contains(errb, "terminal") {
		t.Errorf("expected terminal error, got %q", errb)
	}

	// list shows the deal in won stage with new amount.
	code, out, _ = runCLIWithDB(t, dbPath, "list")
	if code != exitOK {
		t.Fatal("list failed")
	}
	if !strings.Contains(out, id) || !strings.Contains(out, "won") || !strings.Contains(out, "250.00") {
		t.Errorf("list missing expected fields:\n%s", out)
	}

	// history has the right number of events.
	code, out, _ = runCLIWithDB(t, dbPath, "history", id)
	if code != exitOK {
		t.Fatal("history failed")
	}
	// 1 created + 1 stage + 1 amount + 1 note + 1 won = 5 events + header
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 6 {
		t.Errorf("history lines=%d, want 6:\n%s", len(lines), out)
	}
}

func TestRunInvalidStage(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cli.db")
	code, out, _ := runCLIWithDB(t, dbPath, "create", "T", "C", "1")
	if code != exitOK {
		t.Fatal("create")
	}
	id := strings.TrimSpace(out)

	code, _, errb := runCLIWithDB(t, dbPath, "move", id, "FOO")
	if code != exitErr {
		t.Errorf("code=%d, want %d", code, exitErr)
	}
	if !strings.Contains(errb, "invalid stage") {
		t.Errorf("stderr=%q", errb)
	}
}

func TestRunMoveSameStage(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cli.db")
	code, out, _ := runCLIWithDB(t, dbPath, "create", "T", "C", "1")
	if code != exitOK {
		t.Fatal("create")
	}
	id := strings.TrimSpace(out)
	code, _, errb := runCLIWithDB(t, dbPath, "move", id, "lead")
	if code != exitErr || !strings.Contains(errb, "already in stage") {
		t.Errorf("code=%d stderr=%q", code, errb)
	}
}

func TestRunMoveLost(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cli.db")
	code, out, _ := runCLIWithDB(t, dbPath, "create", "T", "C", "1")
	if code != exitOK {
		t.Fatal("create")
	}
	id := strings.TrimSpace(out)
	if code, _, errb := runCLIWithDB(t, dbPath, "move", id, "lost"); code != exitOK {
		t.Fatalf("move lost: code=%d stderr=%q", code, errb)
	}
	code, out, _ = runCLIWithDB(t, dbPath, "show", id)
	if code != exitOK || !strings.Contains(out, "Stage:     lost") {
		t.Errorf("show after lost: code=%d out=%q", code, out)
	}
}

func TestRunCreateBadAmount(t *testing.T) {
	code, _, errb := runCLI(t, "create", "T", "C", "notanumber")
	if code != exitErr {
		t.Errorf("code=%d, want %d", code, exitErr)
	}
	if !strings.Contains(errb, "parse amount") {
		t.Errorf("stderr=%q", errb)
	}
}

func TestRunArityErrors(t *testing.T) {
	// Each of these is missing required args and should hit errUsage.
	cases := [][]string{
		{"create"},
		{"create", "only-one"},
		{"move"},
		{"amount"},
		{"note", "id-only"},
		{"show"},
		{"history"},
		{"list", "extra"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, out, _ := runCLI(t, args...)
			if code != exitUsage {
				t.Errorf("code=%d, want %d", code, exitUsage)
			}
			if !strings.Contains(out, "salesmachine") {
				t.Errorf("expected usage text, got %q", out)
			}
		})
	}
}

func TestRunShowNotFound(t *testing.T) {
	code, _, errb := runCLI(t, "show", "deadbeef0000")
	if code != exitErr {
		t.Errorf("code=%d, want %d", code, exitErr)
	}
	if !strings.Contains(errb, "not found") {
		t.Errorf("stderr=%q", errb)
	}
}

func TestRunHistoryNotFound(t *testing.T) {
	code, _, errb := runCLI(t, "history", "deadbeef0000")
	if code != exitErr {
		t.Errorf("code=%d, want %d", code, exitErr)
	}
	if !strings.Contains(errb, "no events") {
		t.Errorf("stderr=%q", errb)
	}
}

func TestRunListEmpty(t *testing.T) {
	code, out, _ := runCLI(t, "list")
	if code != exitOK {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(out, "ID") || !strings.Contains(out, "STAGE") {
		t.Errorf("expected header, got %q", out)
	}
}

func TestRunDefaultDBPath(t *testing.T) {
	// When SALESMACHINE_DB is unset, run() falls back to "pipeline.db" in the
	// current directory. Run from a temp dir so we don't pollute the repo.
	tmp := t.TempDir()
	t.Chdir(tmp)

	var out, errb bytes.Buffer
	env := envFunc(nil) // empty -> default path
	code := run(testCtx(t), []string{"list"}, env, &out, &errb)
	if code != exitOK {
		t.Fatalf("code=%d stderr=%q", code, errb)
	}
}
