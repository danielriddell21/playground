package dapr

import (
	"context"
	"fmt"
	"time"

	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprgrpc "github.com/dapr/go-sdk/service/grpc"
	"github.com/spf13/cobra"

	"playground/internal/play"
)

const (
	stateStore = "statestore"
	pubsubName = "pubsub"
	topicName  = "play.topic"
	appPort    = ":50002"
)

type Session struct {
	ctx      context.Context
	client   dapr.Client
	service  common.Service
	received chan string
	err      error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	if s.client != nil {
		s.client.Close()
	}
	if s.service != nil {
		s.service.Stop()
	}
}

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Client() dapr.Client { return s.client }

func (s *Session) Fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Session) Note(format string, args ...any) {
	if s.err != nil {
		return
	}
	fmt.Printf(format+"\n", args...)
}

func (s *Session) Table(headers []string, rows [][]any) {
	if s.err != nil {
		return
	}
	play.Table(headers, rows)
}

func (s *Session) warmUp() {
	if s.err != nil {
		return
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.client.PublishEvent(s.ctx, pubsubName, topicName, []byte("__warmup__")); err != nil {
			s.err = err
			return
		}
		if _, ok := s.Await(2 * time.Second); ok {
			break
		}
	}
	for {
		if _, ok := s.Await(500 * time.Millisecond); !ok {
			return
		}
	}
}

func (s *Session) Await(timeout time.Duration) (string, bool) {
	select {
	case msg := <-s.received:
		return msg, true
	case <-time.After(timeout):
		return "", false
	}
}

var set = &play.Set[*Session]{
	Use:        "dapr",
	Short:      "dapr playgrounds",
	DSNEnv:     "DAPR_GRPC_PORT",
	DefaultDSN: "50001",
	Connect:    connect,
}

func connect(ctx context.Context, port string) (*Session, error) {
	s := &Session{ctx: ctx, received: make(chan string, 64)}

	svc, err := daprgrpc.NewService(appPort)
	if err != nil {
		return nil, err
	}
	s.service = svc

	if err := svc.AddTopicEventHandler(&common.Subscription{
		PubsubName: pubsubName,
		Topic:      topicName,
		Route:      "/" + topicName,
	}, func(ctx context.Context, e *common.TopicEvent) (bool, error) {
		s.received <- fmt.Sprintf("%v", e.Data)
		return false, nil
	}); err != nil {
		return nil, err
	}

	if err := svc.AddServiceInvocationHandler("echo", func(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
		return &common.Content{
			Data:        []byte("echo: " + string(in.Data)),
			ContentType: in.ContentType,
			DataTypeURL: in.DataTypeURL,
		}, nil
	}); err != nil {
		return nil, err
	}

	go svc.Start()

	client, err := waitForDapr(ctx, port)
	if err != nil {
		svc.Stop()
		return nil, err
	}
	s.client = client
	return s, nil
}

func waitForDapr(ctx context.Context, port string) (dapr.Client, error) {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := dapr.NewClientWithPort(port)
		if err == nil {
			if _, err := client.GetState(ctx, stateStore, "ping", nil); err == nil {
				return client, nil
			} else {
				lastErr = err
				client.Close()
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("dapr sidecar not ready: %w", lastErr)
}

func Command() *cobra.Command { return set.Command() }
