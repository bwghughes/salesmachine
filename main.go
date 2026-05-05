// See doc.go for the package overview.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	_ "modernc.org/sqlite"
)

// ----- Events ---------------------------------------------------------------

// Event type names. Stable strings: changing them is a breaking schema change.
const (
	EvtDealCreated   = "DealCreated"
	EvtStageChanged  = "StageChanged"
	EvtAmountUpdated = "AmountUpdated"
	EvtNoteAdded     = "NoteAdded"
	EvtDealWon       = "DealWon"
	EvtDealLost      = "DealLost"
)

// Stages enumerates valid pipeline stages. "won" and "lost" are terminal.
var Stages = []string{"lead", "qualified", "proposal", "negotiation", "won", "lost"}

// isStage reports whether s is a recognized pipeline stage.
func isStage(s string) bool { return slices.Contains(Stages, s) }

// isTerminal reports whether the given stage is a terminal stage.
func isTerminal(s string) bool { return s == "won" || s == "lost" }

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
			return fmt.Errorf("unmarshal %s: %w", e.Type, err)
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
			return fmt.Errorf("unmarshal %s: %w", e.Type, err)
		}
		d.Stage = p.To
	case EvtAmountUpdated:
		var p AmountUpdatedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal %s: %w", e.Type, err)
		}
		d.Amount = p.To
	case EvtNoteAdded:
		var p NoteAddedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal %s: %w", e.Type, err)
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

// Store is a thin wrapper around *sql.DB that speaks the events schema.
type Store struct{ db *sql.DB }

// openStore opens (or creates) the SQLite database at path and ensures the
// schema is present. The returned Store owns the *sql.DB; callers must call
// Close.
func openStore(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", path, err)
	}
	if _, err := db.ExecContext(ctx, `
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
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }

// append writes one event with optimistic concurrency control: expectedVersion
// is the version the caller believes is current; the new event will be
// written at expectedVersion+1, and the UNIQUE(aggregate_id, version)
// constraint guarantees no two writers can succeed simultaneously.
func (s *Store) append(ctx context.Context, aggID, aggType, eventType string, expectedVersion int, payload any) (Event, error) {
	buf, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal payload: %w", err)
	}
	newVersion := expectedVersion + 1
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO events (aggregate_id, aggregate_type, version, event_type, payload, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		aggID, aggType, newVersion, eventType, string(buf), now,
	)
	if err != nil {
		return Event{}, fmt.Errorf("append %s v%d (concurrent write?): %w", eventType, newVersion, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Event{}, fmt.Errorf("last insert id: %w", err)
	}
	return Event{
		ID: id, AggregateID: aggID, AggregateType: aggType,
		Version: newVersion, Type: eventType, Payload: buf, CreatedAt: now,
	}, nil
}

// load returns all events for an aggregate, ordered by version.
func (s *Store) load(ctx context.Context, aggID string) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, aggregate_id, aggregate_type, version, event_type, payload, created_at
		 FROM events WHERE aggregate_id = ? ORDER BY version ASC`, aggID)
	if err != nil {
		return nil, fmt.Errorf("query events for %q: %w", aggID, err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var payload string
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.AggregateType, &e.Version,
			&e.Type, &payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// allAggregateIDs returns every distinct aggregate id ever written for deals.
func (s *Store) allAggregateIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT aggregate_id FROM events WHERE aggregate_type = 'deal'`)
	if err != nil {
		return nil, fmt.Errorf("query aggregate ids: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan aggregate id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate aggregate ids: %w", err)
	}
	return ids, nil
}

// rehydrate rebuilds a Deal by replaying its events.
func (s *Store) rehydrate(ctx context.Context, aggID string) (*Deal, error) {
	events, err := s.load(ctx, aggID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("deal %q not found", aggID)
	}
	d := &Deal{}
	for _, e := range events {
		if err := d.apply(e); err != nil {
			return nil, fmt.Errorf("apply v%d: %w", e.Version, err)
		}
	}
	return d, nil
}

// ----- CLI ------------------------------------------------------------------

// newID returns a 12-char random hex id. Returns an error only if the OS
// random source fails, which on Linux/macOS is effectively never.
func newID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

const usageText = `salesmachine — event-sourced sales pipeline

Usage:
  salesmachine create <title> <customer> <amount>
  salesmachine move   <id> <stage>           (stages: lead|qualified|proposal|negotiation|won|lost)
  salesmachine amount <id> <amount>
  salesmachine note   <id> <note...>
  salesmachine show   <id>
  salesmachine list
  salesmachine history <id>

Env:
  SALESMACHINE_DB   path to sqlite file (default: ./pipeline.db)`

// Exit codes follow the standard convention: 0 ok, 1 runtime error, 2 usage.
const (
	exitOK    = 0
	exitErr   = 1
	exitUsage = 2
)

// getenv abstracts os.Getenv so run can be tested with an injected env.
type getenv func(string) string

// run is the testable entry point. It returns a process exit code.
func run(ctx context.Context, args []string, env getenv, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stdout, usageText)
		return exitUsage
	}
	dbPath := env("SALESMACHINE_DB")
	if dbPath == "" {
		dbPath = "pipeline.db"
	}
	s, err := openStore(ctx, dbPath)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return exitErr
	}
	defer func() { _ = s.Close() }()

	cmd, rest := args[0], args[1:]
	if err := dispatch(ctx, s, cmd, rest, stdout); err != nil {
		if err == errUsage {
			fmt.Fprintln(stdout, usageText)
			return exitUsage
		}
		fmt.Fprintln(stderr, "error:", err)
		return exitErr
	}
	return exitOK
}

// errUsage is a sentinel returned by command handlers when the user invoked
// them with the wrong arity. run translates it into exit code 2 + usage text.
var errUsage = fmt.Errorf("usage")

func dispatch(ctx context.Context, s *Store, cmd string, args []string, out io.Writer) error {
	switch cmd {
	case "create":
		return cmdCreate(ctx, s, args, out)
	case "move":
		return cmdMove(ctx, s, args)
	case "amount":
		return cmdAmount(ctx, s, args)
	case "note":
		return cmdNote(ctx, s, args)
	case "show":
		return cmdShow(ctx, s, args, out)
	case "list":
		return cmdList(ctx, s, args, out)
	case "history":
		return cmdHistory(ctx, s, args, out)
	default:
		return errUsage
	}
}

func cmdCreate(ctx context.Context, s *Store, args []string, out io.Writer) error {
	if len(args) != 3 {
		return errUsage
	}
	amount, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return fmt.Errorf("parse amount: %w", err)
	}
	id, err := newID()
	if err != nil {
		return err
	}
	if _, err := s.append(ctx, id, "deal", EvtDealCreated, 0, DealCreatedPayload{
		Title: args[0], Customer: args[1], Amount: amount, Stage: "lead",
	}); err != nil {
		return err
	}
	fmt.Fprintln(out, id)
	return nil
}

func cmdMove(ctx context.Context, s *Store, args []string) error {
	if len(args) != 2 {
		return errUsage
	}
	id, to := args[0], args[1]
	if !isStage(to) {
		return fmt.Errorf("invalid stage %q", to)
	}
	d, err := s.rehydrate(ctx, id)
	if err != nil {
		return err
	}
	if d.Stage == to {
		return fmt.Errorf("deal already in stage %q", to)
	}
	if isTerminal(d.Stage) {
		return fmt.Errorf("deal is terminal (%s); cannot move", d.Stage)
	}
	switch to {
	case "won":
		_, err = s.append(ctx, id, "deal", EvtDealWon, d.Version, struct{}{})
	case "lost":
		_, err = s.append(ctx, id, "deal", EvtDealLost, d.Version, struct{}{})
	default:
		_, err = s.append(ctx, id, "deal", EvtStageChanged, d.Version,
			StageChangedPayload{From: d.Stage, To: to})
	}
	return err
}

func cmdAmount(ctx context.Context, s *Store, args []string) error {
	if len(args) != 2 {
		return errUsage
	}
	id := args[0]
	amount, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return fmt.Errorf("parse amount: %w", err)
	}
	d, err := s.rehydrate(ctx, id)
	if err != nil {
		return err
	}
	if d.Amount == amount {
		return nil // no-op
	}
	_, err = s.append(ctx, id, "deal", EvtAmountUpdated, d.Version,
		AmountUpdatedPayload{From: d.Amount, To: amount})
	return err
}

func cmdNote(ctx context.Context, s *Store, args []string) error {
	if len(args) < 2 {
		return errUsage
	}
	id := args[0]
	note := strings.Join(args[1:], " ")
	d, err := s.rehydrate(ctx, id)
	if err != nil {
		return err
	}
	_, err = s.append(ctx, id, "deal", EvtNoteAdded, d.Version, NoteAddedPayload{Note: note})
	return err
}

func cmdShow(ctx context.Context, s *Store, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errUsage
	}
	d, err := s.rehydrate(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "ID:        %s\nTitle:     %s\nCustomer:  %s\nAmount:    %.2f\nStage:     %s\nVersion:   %d\nCreated:   %s\nUpdated:   %s\n",
		d.ID, d.Title, d.Customer, d.Amount, d.Stage, d.Version,
		d.CreatedAt.Format(time.RFC3339), d.UpdatedAt.Format(time.RFC3339))
	if len(d.Notes) > 0 {
		fmt.Fprintln(out, "Notes:")
		for i, n := range d.Notes {
			fmt.Fprintf(out, "  %d. %s\n", i+1, n)
		}
	}
	return nil
}

func cmdList(ctx context.Context, s *Store, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errUsage
	}
	ids, err := s.allAggregateIDs(ctx)
	if err != nil {
		return err
	}
	deals := make([]*Deal, 0, len(ids))
	for _, id := range ids {
		d, err := s.rehydrate(ctx, id)
		if err != nil {
			return err
		}
		deals = append(deals, d)
	}
	sort.Slice(deals, func(i, j int) bool { return deals[i].UpdatedAt.After(deals[j].UpdatedAt) })
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTAGE\tAMOUNT\tCUSTOMER\tTITLE")
	for _, d := range deals {
		fmt.Fprintf(w, "%s\t%s\t%.2f\t%s\t%s\n", d.ID, d.Stage, d.Amount, d.Customer, d.Title)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

func cmdHistory(ctx context.Context, s *Store, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errUsage
	}
	events, err := s.load(ctx, args[0])
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return fmt.Errorf("no events for %q", args[0])
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VER\tTIME\tEVENT\tPAYLOAD")
	for _, e := range events {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n",
			e.Version, e.CreatedAt.Format(time.RFC3339), e.Type, string(e.Payload))
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

func main() {
	// Cancel on SIGINT/SIGTERM so in-flight DB ops can return promptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}
