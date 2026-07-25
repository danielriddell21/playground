package dapr

import (
	"time"

	dapr "github.com/dapr/go-sdk/client"
)

func init() {
	set.Add("state", "one api in front of any state store, with etags and transactions", state)
	set.Add("pubsub", "publish without knowing who subscribes", pubsub)
	set.Add("invoke", "call another service by name, not by address", invoke)
}

func state(s *Session) {
	s.Note("save and read a key")
	if err := s.Client().SaveState(s.Ctx(), stateStore, "greeting", []byte("hello"), nil); err != nil {
		s.Fail(err)
		return
	}
	item, err := s.Client().GetState(s.Ctx(), stateStore, "greeting", nil)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"key", "value", "etag"}, [][]any{{item.Key, string(item.Value), item.Etag}})

	s.Note("save several at once, then read them in bulk")
	for k, v := range map[string]string{"a": "one", "b": "two", "c": "three"} {
		if err := s.Client().SaveState(s.Ctx(), stateStore, k, []byte(v), nil); err != nil {
			s.Fail(err)
			return
		}
	}
	items, err := s.Client().GetBulkState(s.Ctx(), stateStore, []string{"a", "b", "c"}, nil, 10)
	if err != nil {
		s.Fail(err)
		return
	}
	var rows [][]any
	for _, i := range items {
		rows = append(rows, []any{i.Key, string(i.Value)})
	}
	s.Table([]string{"key", "value"}, rows)

	s.Note("etags stop you overwriting someone else's change")
	current, err := s.Client().GetState(s.Ctx(), stateStore, "greeting", nil)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("  current etag is %s", current.Etag)

	firstWrite := dapr.WithConcurrency(dapr.StateConcurrencyFirstWrite)

	err = s.Client().SaveStateWithETag(s.Ctx(), stateStore, "greeting", []byte("goodbye"), current.Etag, nil, firstWrite)
	s.Note("  writing with the right etag: %v", errText(err))

	err = s.Client().SaveStateWithETag(s.Ctx(), stateStore, "greeting", []byte("stale write"), current.Etag, nil, firstWrite)
	s.Note("  writing again with the same, now stale, etag: %v", errText(err))

	after, err := s.Client().GetState(s.Ctx(), stateStore, "greeting", nil)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"key", "value after both attempts"}, [][]any{{after.Key, string(after.Value)}})

	s.Note("a transaction applies together or not at all")
	ops := []*dapr.StateOperation{
		{Type: dapr.StateOperationTypeUpsert, Item: &dapr.SetStateItem{Key: "tx1", Value: []byte("first")}},
		{Type: dapr.StateOperationTypeUpsert, Item: &dapr.SetStateItem{Key: "tx2", Value: []byte("second")}},
		{Type: dapr.StateOperationTypeDelete, Item: &dapr.SetStateItem{Key: "a"}},
	}
	if err := s.Client().ExecuteStateTransaction(s.Ctx(), stateStore, nil, ops); err != nil {
		s.Fail(err)
		return
	}
	items, err = s.Client().GetBulkState(s.Ctx(), stateStore, []string{"tx1", "tx2", "a"}, nil, 10)
	if err != nil {
		s.Fail(err)
		return
	}
	rows = nil
	for _, i := range items {
		value := string(i.Value)
		if value == "" {
			value = "(deleted)"
		}
		rows = append(rows, []any{i.Key, value})
	}
	s.Table([]string{"key", "value"}, rows)

	s.Note("delete")
	if err := s.Client().DeleteState(s.Ctx(), stateStore, "greeting", nil); err != nil {
		s.Fail(err)
		return
	}
	gone, err := s.Client().GetState(s.Ctx(), stateStore, "greeting", nil)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("  value is now %q", string(gone.Value))
}

func pubsub(s *Session) {
	s.warmUp()

	s.Note("the publisher names a topic, not a subscriber")
	for _, msg := range []string{"first", "second", "third"} {
		if err := s.Client().PublishEvent(s.Ctx(), pubsubName, topicName, []byte(msg)); err != nil {
			s.Fail(err)
			return
		}
		s.Note("  published %q", msg)
	}

	s.Note("the same process is subscribed, and dapr delivers to it")
	var rows [][]any
	for i := range 3 {
		msg, ok := s.Await(15 * time.Second)
		if !ok {
			s.Note("  no more messages arrived")
			break
		}
		rows = append(rows, []any{i + 1, msg})
	}
	s.Table([]string{"#", "delivered to handler"}, rows)

	s.Note("publishing structured data works the same way")
	if err := s.Client().PublishEvent(s.Ctx(), pubsubName, topicName,
		[]byte(`{"order":42,"status":"shipped"}`),
		dapr.PublishEventWithContentType("application/json")); err != nil {
		s.Fail(err)
		return
	}
	if msg, ok := s.Await(15 * time.Second); ok {
		s.Table([]string{"delivered"}, [][]any{{msg}})
	} else {
		s.Note("  nothing arrived")
	}
}

func invoke(s *Session) {
	s.Note("call an app by its id, dapr works out where it lives")
	resp, err := s.Client().InvokeMethodWithContent(s.Ctx(), "playground", "echo", "post",
		&dapr.DataContent{Data: []byte("hello from the playground"), ContentType: "text/plain"})
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"sent", "received"}, [][]any{{"hello from the playground", string(resp)}})

	s.Note("same call again, with json")
	resp, err = s.Client().InvokeMethodWithContent(s.Ctx(), "playground", "echo", "post",
		&dapr.DataContent{Data: []byte(`{"id":1}`), ContentType: "application/json"})
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"received"}, [][]any{{string(resp)}})

	s.Note("a method nobody registered")
	_, err = s.Client().InvokeMethodWithContent(s.Ctx(), "playground", "nope", "post",
		&dapr.DataContent{Data: []byte("x"), ContentType: "text/plain"})
	s.Note("  %v", errText(err))
}

func errText(err error) string {
	if err == nil {
		return "accepted"
	}
	return "rejected: " + err.Error()
}
