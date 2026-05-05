// salesmachine: a tiny event-sourced sales pipeline backed by SQLite.
//
// Data model:
//
//	events (append-only)
//	    id              INTEGER PK
//	    aggregate_id    TEXT     -- the deal id (uuid-ish)
//	    aggregate_type  TEXT     -- always "deal" here
//	    version         INTEGER  -- per-aggregate, monotonically increasing, starting at 1
//	    event_type      TEXT     -- e.g. DealCreated, StageChanged, ...
//	    payload         TEXT     -- JSON blob
//	    created_at      DATETIME
//	    UNIQUE(aggregate_id, version)   -- optimistic concurrency
//
// Current state of a deal is derived by replaying its events. There is no
// mutable "deals" table; the event log is the source of truth.
package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	_ "modernc.org/sqlite"
)

// ----- Events ---------------------------------------------------------------

const (
	EvtDealCreated   = "DealCreated"
	EvtStageChanged  = "StageChanged"
	EvtAmountUpdated = "AmountUpdated"
	EvtNoteAdded     = "NoteAdded"
	EvtDealWon       = "DealWon"
	EvtDealLost      = "DealLost"
)

// Valid pipeline stages (terminal stages: won, lost).
var stages = []string{"lead", "qualified", "proposal", "negotiation", "won", "lost"}

func isStage(s string) bool {
	for _, x := range stages {
		if x == s {
			return true
		}
	}
	return false
}

// Event is the on-disk representation of an event.
type Event struct {
	ID            int64
	AggregateID   string
	AggregateType string
	Version       int
	Type          string
	Payload       json.RawMessage
	CreatedAt     time.Time
}

// Payload shapes (one per event type). Kept tiny on purpose.
type DealCreatedPayload struct {
	Title    string  `json:"title"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
	Stage    string  `json:"stage"`
}
type StageChangedPayload struct {
	From string `json:"from"`
	To   string `json:"to"`
}
type AmountUpdatedPayload struct {
	From float64 `json:"from"`
	To   float64 `json:"to"`
}
type NoteAddedPayload struct {
	Note string `json:"note"`
}

// ----- Aggregate ------------------------------------------------------------

// Deal is the projection produced by replaying events for one aggregate.
type Deal struct {
	ID        string
	Title     string
	Customer  string
	Amount    float64
	Stage     string
	Notes     []string
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// apply mutates d for a single event. This is the only place that knows how
// each event type changes state — keep it exhaustive.
func (d *Deal) apply(e Event) error {
	switch e.Type {
	case EvtDealCreated:
		var p DealCreatedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		d.ID = e.AggregateID
		d.Title = p.Title
		d.Customer = p.Customer
		d.Amount = p.Amount
		d.Stage = p.Stage
		d.CreatedAt = e.CreatedAt
	case EvtStageChanged:
		var p StageChangedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		d.Stage = p.To
	case EvtAmountUpdated:
		var p AmountUpdatedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		d.Amount = p.To
	case EvtNoteAdded:
		var p NoteAddedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		d.Notes = append(d.Notes, p.Note)
	case EvtDealWon:
		d.Stage = "won"
	case EvtDealLost:
		d.Stage = "lost"
	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	d.Version = e.Version
	d.UpdatedAt = e.CreatedAt
	return nil
}

// ----- Event store ----------------------------------------------------------

type Store struct{ db *sql.DB }

func openStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			aggregate_id    TEXT     NOT NULL,
			aggregate_type  TEXT     NOT NULL,
			version         INTEGER  NOT NULL,
			event_type      TEXT     NOT NULL,
			payload         TEXT     NOT NULL,
			created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(aggregate_id, version)
		);
		CREATE INDEX IF NOT EXISTS idx_events_agg ON events(aggregate_id, version);
	`); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// append writes one event with optimistic concurrency control: expectedVersion
// is the version the caller believes is current; the new event will be
// written at expectedVersion+1, and the UNIQUE(aggregate_id, version)
// constraint guarantees no two writers can succeed simultaneously.
func (s *Store) append(aggID, aggType, eventType string, expectedVersion int, payload any) (Event, error) {
	buf, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	newVersion := expectedVersion + 1
	now := time.Now().UTC()
	res, err := s.db.Exec(
		`INSERT INTO events (aggregate_id, aggregate_type, version, event_type, payload, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		aggID, aggType, newVersion, eventType, string(buf), now,
	)
	if err != nil {
		return Event{}, fmt.Errorf("append (concurrent write?): %w", err)
	}
	id, _ := res.LastInsertId()
	return Event{
		ID: id, AggregateID: aggID, AggregateType: aggType,
		Version: newVersion, Type: eventType, Payload: buf, CreatedAt: now,
	}, nil
}

// load returns all events for an aggregate, ordered by version.
func (s *Store) load(aggID string) ([]Event, error) {
	rows, err := s.db.Query(
		`SELECT id, aggregate_id, aggregate_type, version, event_type, payload, created_at
		 FROM events WHERE aggregate_id = ? ORDER BY version ASC`, aggID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var payload string
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.AggregateType, &e.Version,
			&e.Type, &payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}

// allAggregateIDs returns every distinct aggregate id ever written.
func (s *Store) allAggregateIDs() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT aggregate_id FROM events WHERE aggregate_type = 'deal'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// rehydrate rebuilds a Deal by replaying its events.
func (s *Store) rehydrate(aggID string) (*Deal, error) {
	events, err := s.load(aggID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("deal %q not found", aggID)
	}
	d := &Deal{}
	for _, e := range events {
		if err := d.apply(e); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// ----- CLI ------------------------------------------------------------------

func newID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func usage() {
	fmt.Println(`salesmachine — event-sourced sales pipeline

Usage:
  salesmachine create <title> <customer> <amount>
  salesmachine move   <id> <stage>           (stages: lead|qualified|proposal|negotiation|won|lost)
  salesmachine amount <id> <amount>
  salesmachine note   <id> <note...>
  salesmachine show   <id>
  salesmachine list
  salesmachine history <id>

Env:
  SALESMACHINE_DB   path to sqlite file (default: ./pipeline.db)`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	dbPath := os.Getenv("SALESMACHINE_DB")
	if dbPath == "" {
		dbPath = "pipeline.db"
	}
	s, err := openStore(dbPath)
	must(err)
	defer s.db.Close()

	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "create":
		if len(args) != 3 {
			usage()
			os.Exit(2)
		}
		amount, err := strconv.ParseFloat(args[2], 64)
		must(err)
		id := newID()
		_, err = s.append(id, "deal", EvtDealCreated, 0, DealCreatedPayload{
			Title: args[0], Customer: args[1], Amount: amount, Stage: "lead",
		})
		must(err)
		fmt.Println(id)

	case "move":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		id, to := args[0], args[1]
		if !isStage(to) {
			must(fmt.Errorf("invalid stage %q", to))
		}
		d, err := s.rehydrate(id)
		must(err)
		if d.Stage == to {
			must(fmt.Errorf("deal already in stage %q", to))
		}
		if d.Stage == "won" || d.Stage == "lost" {
			must(fmt.Errorf("deal is terminal (%s); cannot move", d.Stage))
		}
		switch to {
		case "won":
			_, err = s.append(id, "deal", EvtDealWon, d.Version, struct{}{})
		case "lost":
			_, err = s.append(id, "deal", EvtDealLost, d.Version, struct{}{})
		default:
			_, err = s.append(id, "deal", EvtStageChanged, d.Version,
				StageChangedPayload{From: d.Stage, To: to})
		}
		must(err)

	case "amount":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		id := args[0]
		amount, err := strconv.ParseFloat(args[1], 64)
		must(err)
		d, err := s.rehydrate(id)
		must(err)
		if d.Amount == amount {
			return
		}
		_, err = s.append(id, "deal", EvtAmountUpdated, d.Version,
			AmountUpdatedPayload{From: d.Amount, To: amount})
		must(err)

	case "note":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		id := args[0]
		note := strings.Join(args[1:], " ")
		d, err := s.rehydrate(id)
		must(err)
		_, err = s.append(id, "deal", EvtNoteAdded, d.Version, NoteAddedPayload{Note: note})
		must(err)

	case "show":
		if len(args) != 1 {
			usage()
			os.Exit(2)
		}
		d, err := s.rehydrate(args[0])
		must(err)
		fmt.Printf("ID:        %s\nTitle:     %s\nCustomer:  %s\nAmount:    %.2f\nStage:     %s\nVersion:   %d\nCreated:   %s\nUpdated:   %s\n",
			d.ID, d.Title, d.Customer, d.Amount, d.Stage, d.Version,
			d.CreatedAt.Format(time.RFC3339), d.UpdatedAt.Format(time.RFC3339))
		if len(d.Notes) > 0 {
			fmt.Println("Notes:")
			for i, n := range d.Notes {
				fmt.Printf("  %d. %s\n", i+1, n)
			}
		}

	case "list":
		ids, err := s.allAggregateIDs()
		must(err)
		deals := make([]*Deal, 0, len(ids))
		for _, id := range ids {
			d, err := s.rehydrate(id)
			must(err)
			deals = append(deals, d)
		}
		sort.Slice(deals, func(i, j int) bool { return deals[i].UpdatedAt.After(deals[j].UpdatedAt) })
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tSTAGE\tAMOUNT\tCUSTOMER\tTITLE")
		for _, d := range deals {
			fmt.Fprintf(w, "%s\t%s\t%.2f\t%s\t%s\n", d.ID, d.Stage, d.Amount, d.Customer, d.Title)
		}
		w.Flush()

	case "history":
		if len(args) != 1 {
			usage()
			os.Exit(2)
		}
		events, err := s.load(args[0])
		must(err)
		if len(events) == 0 {
			must(fmt.Errorf("no events for %q", args[0]))
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "VER\tTIME\tEVENT\tPAYLOAD")
		for _, e := range events {
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\n",
				e.Version, e.CreatedAt.Format(time.RFC3339), e.Type, string(e.Payload))
		}
		w.Flush()

	default:
		usage()
		os.Exit(2)
	}
}

func must(err error) {
	if err == nil {
		return
	}
	var sqliteErr interface{ Error() string }
	if errors.As(err, &sqliteErr) {
		fmt.Fprintln(os.Stderr, "error:", err)
	} else {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	os.Exit(1)
}
