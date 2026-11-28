package restateplay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	restate "github.com/restatedev/sdk-go"
)

// Event is what flows through. ID doubles as the idempotency key.
type Event struct {
	ID  string `json:"id"`
	Key string `json:"key"`
	At  int64  `json:"at"`
}

// Events is a plain service. Record writes to the ledger inside restate.Run,
// so while the ledger is down restate keeps retrying that journaled step and
// the event isn't lost, then hands the event to the Tally object for its key.
type Events struct {
	ledgerURL string
}

func (e *Events) Record(ctx restate.Context, ev Event) error {
	err := restate.RunVoid(ctx, func(restate.RunContext) error {
		body, _ := json.Marshal(ev)
		res, err := http.Post(e.ledgerURL+"/events", "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		res.Body.Close()
		if res.StatusCode >= 300 {
			return fmt.Errorf("ledger: %s", res.Status)
		}
		return nil
	}, restate.WithName("ledger"), restate.WithInitialRetryInterval(200*time.Millisecond), restate.WithMaxRetryInterval(time.Second))
	if err != nil {
		return err
	}
	restate.ObjectSend(ctx, "Tally", ev.Key, "Add").Send(ev)
	return nil
}

// TallyState is what one Tally keeps.
type TallyState struct {
	Count   int64 `json:"count"`
	Reports int   `json:"reports"`
}

// Tally is a virtual object. Restate runs one exclusive handler per key at
// a time, so Add needs no locks, and the state lives in restate.
type Tally struct{}

var reportAfter = 3 * time.Second

func (Tally) Add(ctx restate.ObjectContext, ev Event) error {
	t, err := restate.Get[TallyState](ctx, "tally")
	if err != nil {
		return err
	}
	if t.Count == 0 {
		// durable timer: survives this process and restate restarting
		restate.ObjectSend(ctx, "Tally", restate.Key(ctx), "Report").Send(nil, restate.WithDelay(reportAfter))
	}
	t.Count++
	restate.Set(ctx, "tally", t)
	return nil
}

func (Tally) Report(ctx restate.ObjectContext) error {
	t, err := restate.Get[TallyState](ctx, "tally")
	if err != nil {
		return err
	}
	t.Reports++
	restate.Set(ctx, "tally", t)
	return restate.RunVoid(ctx, func(restate.RunContext) error {
		slog.Debug("report", "key", restate.Key(ctx), "count", t.Count)
		return nil
	})
}

func (Tally) Get(ctx restate.ObjectSharedContext) (TallyState, error) {
	return restate.Get[TallyState](ctx, "tally")
}
