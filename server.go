package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ----- HTTP API -------------------------------------------------------------
//
// The server is a thin REST facade over the same Store/Deal model used by the
// CLI. It deliberately mirrors the CLI command set so behavior stays
// consistent:
//
//   POST   /deals                  create
//   GET    /deals                  list
//   GET    /deals/{id}             show
//   GET    /deals/{id}/history     event history
//   POST   /deals/{id}/move        {"stage": "..."}
//   POST   /deals/{id}/amount      {"amount": 123.45}
//   POST   /deals/{id}/notes       {"note": "..."}
//   GET    /healthz
//
// Errors are returned as JSON: {"error": "..."}.

// apiServer wires a Store to an http.Handler.
type apiServer struct {
	store *Store
	mux   *http.ServeMux
}

func newAPIServer(s *Store) *apiServer {
	a := &apiServer{store: s, mux: http.NewServeMux()}
	a.routes()
	return a
}

func (a *apiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

func (a *apiServer) routes() {
	a.mux.HandleFunc("/healthz", a.handleHealth)
	a.mux.HandleFunc("/deals", a.handleDeals)
	a.mux.HandleFunc("/deals/", a.handleDealByID)
}

// ----- helpers --------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// decodeJSON reads a small JSON body into dst. It caps the body at 1 MiB to
// avoid trivial memory exhaustion.
func decodeJSON(r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	return nil
}

// dealView is the JSON projection of a Deal. Defined explicitly so the API
// shape is independent of internal struct tags/field order.
type dealView struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Customer  string    `json:"customer"`
	Amount    float64   `json:"amount"`
	Stage     string    `json:"stage"`
	Notes     []string  `json:"notes"`
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toView(d *Deal) dealView {
	notes := d.Notes
	if notes == nil {
		notes = []string{}
	}
	return dealView{
		ID: d.ID, Title: d.Title, Customer: d.Customer, Amount: d.Amount,
		Stage: d.Stage, Notes: notes, Version: d.Version,
		CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

// ----- handlers -------------------------------------------------------------

func (a *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *apiServer) handleDeals(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.listDeals(w, r)
	case http.MethodPost:
		a.createDeal(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleDealByID dispatches on the path segments after /deals/.
// Shapes accepted:
//
//	/deals/{id}
//	/deals/{id}/history
//	/deals/{id}/move
//	/deals/{id}/amount
//	/deals/{id}/notes
func (a *apiServer) handleDealByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/deals/")
	if rest == "" {
		writeErr(w, http.StatusNotFound, "missing deal id")
		return
	}
	parts := strings.Split(rest, "/")
	id := parts[0]
	if id == "" {
		writeErr(w, http.StatusNotFound, "missing deal id")
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			a.showDeal(w, r, id)
		default:
			w.Header().Set("Allow", "GET")
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	if len(parts) != 2 || parts[1] == "" {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	action := parts[1]
	switch action {
	case "history":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		a.dealHistory(w, r, id)
	case "move":
		a.requirePOST(w, r, func() { a.moveDeal(w, r, id) })
	case "amount":
		a.requirePOST(w, r, func() { a.amountDeal(w, r, id) })
	case "notes":
		a.requirePOST(w, r, func() { a.noteDeal(w, r, id) })
	default:
		writeErr(w, http.StatusNotFound, "not found")
	}
}

func (a *apiServer) requirePOST(w http.ResponseWriter, r *http.Request, fn func()) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	fn()
}

// --- create -----------------------------------------------------------------

type createReq struct {
	Title    string  `json:"title"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}

func (a *apiServer) createDeal(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.Customer) == "" {
		writeErr(w, http.StatusBadRequest, "title and customer are required")
		return
	}
	id, err := newID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := a.store.append(r.Context(), id, "deal", EvtDealCreated, 0, DealCreatedPayload{
		Title: req.Title, Customer: req.Customer, Amount: req.Amount, Stage: "lead",
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	d, err := a.store.rehydrate(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Location", "/deals/"+id)
	writeJSON(w, http.StatusCreated, toView(d))
}

// --- list -------------------------------------------------------------------

func (a *apiServer) listDeals(w http.ResponseWriter, r *http.Request) {
	ids, err := a.store.allAggregateIDs(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]dealView, 0, len(ids))
	for _, id := range ids {
		d, err := a.store.rehydrate(r.Context(), id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, toView(d))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	writeJSON(w, http.StatusOK, map[string]any{"deals": out})
}

// --- show -------------------------------------------------------------------

func (a *apiServer) showDeal(w http.ResponseWriter, r *http.Request, id string) {
	d, err := a.store.rehydrate(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toView(d))
}

// --- history ----------------------------------------------------------------

type eventView struct {
	Version   int             `json:"version"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

func (a *apiServer) dealHistory(w http.ResponseWriter, r *http.Request, id string) {
	events, err := a.store.load(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(events) == 0 {
		writeErr(w, http.StatusNotFound, "no events for "+id)
		return
	}
	out := make([]eventView, 0, len(events))
	for _, e := range events {
		out = append(out, eventView{
			Version: e.Version, Type: e.Type, Payload: e.Payload, CreatedAt: e.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "events": out})
}

// --- move -------------------------------------------------------------------

type moveReq struct {
	Stage string `json:"stage"`
}

func (a *apiServer) moveDeal(w http.ResponseWriter, r *http.Request, id string) {
	var req moveReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !isStage(req.Stage) {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid stage %q", req.Stage))
		return
	}
	d, err := a.store.rehydrate(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	if d.Stage == req.Stage {
		writeErr(w, http.StatusConflict, fmt.Sprintf("deal already in stage %q", req.Stage))
		return
	}
	if isTerminal(d.Stage) {
		writeErr(w, http.StatusConflict, fmt.Sprintf("deal is terminal (%s); cannot move", d.Stage))
		return
	}
	switch req.Stage {
	case "won":
		_, err = a.store.append(r.Context(), id, "deal", EvtDealWon, d.Version, struct{}{})
	case "lost":
		_, err = a.store.append(r.Context(), id, "deal", EvtDealLost, d.Version, struct{}{})
	default:
		_, err = a.store.append(r.Context(), id, "deal", EvtStageChanged, d.Version,
			StageChangedPayload{From: d.Stage, To: req.Stage})
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.respondWithDeal(w, r, id)
}

// --- amount -----------------------------------------------------------------

type amountReq struct {
	Amount float64 `json:"amount"`
}

func (a *apiServer) amountDeal(w http.ResponseWriter, r *http.Request, id string) {
	var req amountReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	d, err := a.store.rehydrate(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	if d.Amount != req.Amount {
		if _, err := a.store.append(r.Context(), id, "deal", EvtAmountUpdated, d.Version,
			AmountUpdatedPayload{From: d.Amount, To: req.Amount}); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	a.respondWithDeal(w, r, id)
}

// --- note -------------------------------------------------------------------

type noteReq struct {
	Note string `json:"note"`
}

func (a *apiServer) noteDeal(w http.ResponseWriter, r *http.Request, id string) {
	var req noteReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Note) == "" {
		writeErr(w, http.StatusBadRequest, "note is required")
		return
	}
	d, err := a.store.rehydrate(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	if _, err := a.store.append(r.Context(), id, "deal", EvtNoteAdded, d.Version,
		NoteAddedPayload{Note: req.Note}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.respondWithDeal(w, r, id)
}

func (a *apiServer) respondWithDeal(w http.ResponseWriter, r *http.Request, id string) {
	d, err := a.store.rehydrate(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toView(d))
}

// ----- serve command --------------------------------------------------------

// cmdServe runs the HTTP API server until ctx is canceled.
func cmdServe(ctx context.Context, s *Store, args []string, addr string, out io.Writer) error {
	if len(args) > 1 {
		return errUsage
	}
	if len(args) == 1 {
		addr = args[0]
	}
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           newAPIServer(s),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		fmt.Fprintf(out, "salesmachine api listening on %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}
