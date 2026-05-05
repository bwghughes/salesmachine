# salesmachine

A tiny event-sourced sales pipeline backed by SQLite, exposed as a single CLI
binary. Every change to a deal is appended as an immutable event; the deal's
current state is derived by replaying its events. There is no mutable `deals`
table — the event log is the source of truth.

```
$ salesmachine create "Big Deal" Acme 25000
a1b2c3d4e5f6
$ salesmachine move a1b2c3d4e5f6 qualified
$ salesmachine note a1b2c3d4e5f6 sent proposal Friday
$ salesmachine amount a1b2c3d4e5f6 30000
$ salesmachine show a1b2c3d4e5f6
ID:        a1b2c3d4e5f6
Title:     Big Deal
Customer:  Acme
Amount:    30000.00
Stage:     qualified
Version:   4
...
```

## Install / build

```sh
go build ./...
```

Requires Go 1.21+ (uses `slices.Contains`). The project's `go.mod` pins
`go 1.25.0`. The only runtime dependency is the pure-Go SQLite driver
[`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) — no CGO.

## Usage

```
salesmachine create  <title> <customer> <amount>
salesmachine move    <id> <stage>           (lead|qualified|proposal|negotiation|won|lost)
salesmachine amount  <id> <amount>
salesmachine note    <id> <note...>
salesmachine show    <id>
salesmachine list
salesmachine history <id>
```

### Environment

| Variable           | Default        | Purpose                              |
| ------------------ | -------------- | ------------------------------------ |
| `SALESMACHINE_DB`  | `pipeline.db`  | Path to the SQLite database file.    |

### Exit codes

| Code | Meaning                                                              |
| ---- | -------------------------------------------------------------------- |
| `0`  | Success.                                                             |
| `1`  | Runtime error (bad input, not found, terminal-stage move, DB error). |
| `2`  | Usage error (missing or extra arguments, unknown subcommand).        |

## Architecture

### Data model

A single append-only `events` table:

| column           | type      | notes                                              |
| ---------------- | --------- | -------------------------------------------------- |
| `id`             | INTEGER   | autoincrement primary key                          |
| `aggregate_id`   | TEXT      | the deal id (12-char hex)                          |
| `aggregate_type` | TEXT      | always `"deal"` here; reserved for future types    |
| `version`        | INTEGER   | per-aggregate, monotonic, starting at 1            |
| `event_type`     | TEXT      | `DealCreated`, `StageChanged`, …                   |
| `payload`        | TEXT      | event-specific JSON blob                           |
| `created_at`     | DATETIME  | UTC                                                |

A `UNIQUE(aggregate_id, version)` constraint provides **optimistic concurrency
control** — two writers that both believe the deal is at version *N* will
race to insert version *N+1*, and exactly one will win; the other's `INSERT`
fails with a uniqueness violation that bubbles up as an error.

### Event types

| Event           | Payload                                | Effect on the projection         |
| --------------- | -------------------------------------- | -------------------------------- |
| `DealCreated`   | `{title, customer, amount, stage}`     | initializes the deal             |
| `StageChanged`  | `{from, to}`                           | updates `Stage`                  |
| `AmountUpdated` | `{from, to}`                           | updates `Amount`                 |
| `NoteAdded`     | `{note}`                               | appends to `Notes`               |
| `DealWon`       | `{}`                                   | sets `Stage = "won"` (terminal)  |
| `DealLost`      | `{}`                                   | sets `Stage = "lost"` (terminal) |

The full reduction lives in `(*Deal).apply` — it is the single source of
truth for "how does each event change state." Adding a new event type means
adding a constant, a payload struct, and a `case` in `apply`.

### Stages

```
lead → qualified → proposal → negotiation → won
                                          ↘ lost
```

Stages are not enforced as a strict graph; any non-terminal deal can be
moved directly to any other non-terminal stage, or to `won` / `lost`.
Once terminal, a deal cannot be moved again.

### Code layout

Everything lives in `package main`:

| Section             | Responsibility                                                |
| ------------------- | ------------------------------------------------------------- |
| Event constants & payload types | wire format of each event                         |
| `Deal` + `apply`    | the projection (in-memory state derived from events)          |
| `Store`             | thin `*sql.DB` wrapper: `append`, `load`, `rehydrate`, `allAggregateIDs` |
| `cmd*` handlers     | one function per subcommand; pure w.r.t. `io.Writer` + `Store` |
| `run` / `dispatch`  | testable CLI entry point: `run(ctx, args, env, stdout, stderr) int` |
| `main`              | thin shim: builds a signal-cancellable context and calls `run` |

`run` is deliberately decoupled from `os.Args` / `os.Stdout` / `os.Exit` so
that the entire CLI surface is testable end-to-end against a temp-dir
SQLite file. See `TestRunFullLifecycle` in `main_test.go`.

### Concurrency & cancellation

Every blocking DB call takes a `context.Context`. `main` constructs the
top-level context with `signal.NotifyContext(..., os.Interrupt, SIGTERM)`,
so Ctrl-C causes in-flight queries to abort promptly rather than block the
shutdown path. Tests exercise this directly via `TestAppendCancelledContext`.

## Development

```sh
go vet ./...
go test -race -cover ./...
```

The test suite is table-driven where it makes sense (`TestApply`,
`TestIsStage`, `TestRunArityErrors`) and runs the full CLI against a
temp-dir database for each integration test, so tests are isolated and
parallel-safe.

### Adding a new event type

1. Add a `Evt<Name>` constant.
2. Add a `<Name>Payload` struct (or use `struct{}{}` for nullary events).
3. Add a `case` in `(*Deal).apply` that unmarshals the payload and mutates
   the projection. Don't forget to leave `Version` / `UpdatedAt` to the
   common tail of `apply`.
4. Add a `cmd*` handler (and a line to `dispatch`) if the event is
   user-driven.
5. Add a row to `TestApply`'s table.

Because events are persisted as JSON, **never rename or repurpose an
existing event constant** — that would silently break replay against
historical data. Add a new event instead.
