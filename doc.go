// Package main implements salesmachine, an event-sourced sales pipeline
// CLI backed by SQLite.
//
// # Overview
//
// salesmachine models deals as a stream of immutable events. The current
// state of any deal is a projection produced by replaying its events in
// order; there is no mutable "deals" table. The event log is the single
// source of truth.
//
// Each event row records (aggregate_id, version, event_type, payload).
// A UNIQUE(aggregate_id, version) constraint provides optimistic
// concurrency control: two writers that both observed version N will
// race to insert version N+1, and exactly one will succeed.
//
// # Event types
//
// The supported events and their payloads are:
//
//   - DealCreated   — DealCreatedPayload   (title, customer, amount, stage)
//   - StageChanged  — StageChangedPayload  (from, to)
//   - AmountUpdated — AmountUpdatedPayload (from, to)
//   - NoteAdded     — NoteAddedPayload     (note)
//   - DealWon       — no payload (sets Stage = "won", terminal)
//   - DealLost      — no payload (sets Stage = "lost", terminal)
//
// Event constants are wire-format. They are persisted as strings in the
// database and MUST NOT be renamed or repurposed — doing so would silently
// break replay against historical data. Introduce a new event type instead.
//
// # Stages
//
// Valid pipeline stages are listed in [Stages]. "won" and "lost" are
// terminal: once a deal reaches a terminal stage it cannot be moved again.
//
// # Architecture
//
// The package is organised into four layers:
//
//   - Wire types: event constants and *Payload structs.
//   - Projection: [Deal] and (*Deal).apply, which reduce events into state.
//   - Storage:    [Store], a thin wrapper around *sql.DB exposing
//     append, load, rehydrate, and allAggregateIDs — all context-aware.
//   - CLI:        per-subcommand handlers (cmdCreate, cmdMove, ...) wired
//     up by dispatch and invoked from the testable [run] entry point.
//
// [run] is decoupled from os.Args, os.Stdout, and os.Exit so that the
// entire CLI surface can be exercised in tests against a temp-dir
// database. main itself is a thin shim that constructs a
// signal-cancellable context and forwards to run.
//
// # CLI usage
//
//	salesmachine create  <title> <customer> <amount>
//	salesmachine move    <id> <stage>
//	salesmachine amount  <id> <amount>
//	salesmachine note    <id> <note...>
//	salesmachine show    <id>
//	salesmachine list
//	salesmachine history <id>
//
// The database path is taken from the SALESMACHINE_DB environment variable,
// defaulting to "pipeline.db" in the current working directory.
//
// Exit codes: 0 on success, 1 on runtime error, 2 on usage error.
package main
