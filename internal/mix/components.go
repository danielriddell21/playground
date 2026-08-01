package mix

import (
	"context"
	"encoding/json"
	"time"

	dapr "github.com/dapr/go-sdk/client"
	daprgrpc "github.com/dapr/go-sdk/service/grpc"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("components", "the same dapr code over redis, postgres and kafka", components)
}

const daprTopic = "mix.dapr"

func components(s *Session) {
	svc, err := daprgrpc.NewService(":50002")
	if err != nil {
		s.Fail(err)
		return
	}
	go svc.Start()
	defer svc.Stop()

	client, err := waitForDapr(s)
	if err != nil {
		s.Fail(err)
		return
	}
	defer client.Close()

	s.Note("=== one state api, two completely different databases")
	s.Note("identical calls, the only thing that changes is the store name")
	var rows [][]any
	for _, store := range []string{"statestore", "statestore-postgres"} {
		if err := client.SaveState(s.Ctx(), store, "greeting", []byte("hello from "+store), nil); err != nil {
			s.Fail(err)
			return
		}
		item, err := client.GetState(s.Ctx(), store, "greeting", nil)
		if err != nil {
			s.Fail(err)
			return
		}
		rows = append(rows, []any{store, string(item.Value), item.Etag})
	}
	s.Table([]string{"store name", "value read back", "etag"}, rows)

	s.Note("the postgres one is a real table you can query yourself")
	s.Show(`select key, left(value::text, 40) as value from dapr_state order by key`)

	s.Note("=== one pubsub api, two brokers")
	admin := s.Kafka()
	if admin == nil {
		return
	}
	defer admin.Close()
	adm := kadm.NewClient(admin)
	adm.DeleteTopics(s.Ctx(), daprTopic)
	if _, err := adm.CreateTopic(s.Ctx(), 1, 1, nil, daprTopic); err != nil {
		s.Fail(err)
		return
	}

	for _, name := range []string{"pubsub", "pubsub-kafka"} {
		if err := client.PublishEvent(s.Ctx(), name, daprTopic, []byte(`{"order":42}`),
			dapr.PublishEventWithContentType("application/json")); err != nil {
			s.Fail(err)
			return
		}
		s.Note("  published to %s", name)
	}

	s.Note("=== now read the kafka topic with a plain kafka client, no dapr involved")
	raw := readOne(s, daprTopic)
	if raw == "" {
		s.Note("  nothing on the topic")
		return
	}

	s.Note("you published 13 bytes, here is what actually landed on the log")
	var envelope map[string]any
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		s.Note("%s", raw)
		return
	}
	var fields [][]any
	for _, k := range []string{"id", "source", "type", "specversion", "datacontenttype", "pubsubname", "topic", "data"} {
		if v, ok := envelope[k]; ok {
			fields = append(fields, []any{k, v})
		}
	}
	s.Table([]string{"cloudevents field", "value"}, fields)

	s.Note("that is the cost of the abstraction: portable code, but the wire format is dapr's")
	s.Note("anything not speaking dapr sees the envelope, not your payload")
	s.Note("set rawPayload on the component if you need the bare bytes")
}

func waitForDapr(s *Session) (dapr.Client, error) {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := dapr.NewClientWithPort(env("DAPR_GRPC_PORT", "50001"))
		if err == nil {
			if _, err := client.GetState(s.Ctx(), "statestore", "ping", nil); err == nil {
				return client, nil
			}
			lastErr = err
			client.Close()
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	return nil, lastErr
}

func readOne(s *Session, topic string) string {
	cl := s.Kafka(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if cl == nil {
		return ""
	}
	defer cl.Close()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(s.Ctx(), 2*time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		var out string
		fetches.EachRecord(func(r *kgo.Record) {
			if out == "" {
				out = string(r.Value)
			}
		})
		if out != "" {
			return out
		}
	}
	return ""
}
