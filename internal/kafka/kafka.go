package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"playground/internal/play"
)

type Session struct {
	ctx     context.Context
	brokers []string
	admin   *kadm.Client
	base    *kgo.Client
	err     error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	s.admin.Close()
	s.base.Close()
}

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Brokers() []string { return s.brokers }

func (s *Session) Admin() *kadm.Client { return s.admin }

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

func (s *Session) ResetTopic(topic string, partitions int32, configs map[string]*string) {
	if s.err != nil {
		return
	}
	s.admin.DeleteTopics(s.ctx, topic)
	if _, err := s.admin.CreateTopic(s.ctx, partitions, 1, configs, topic); err != nil {
		if !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			s.err = err
		}
	}
}

func (s *Session) Client(opts ...kgo.Opt) *kgo.Client {
	if s.err != nil {
		return nil
	}
	opts = append([]kgo.Opt{kgo.SeedBrokers(s.brokers...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		s.err = err
		return nil
	}
	return cl
}

var set = &play.Set[*Session]{
	Use:        "kafka",
	Short:      "kafka playgrounds",
	DSNEnv:     "KAFKA_BROKERS",
	DefaultDSN: "localhost:9092",
	Connect:    connect,
}

func connect(ctx context.Context, dsn string) (*Session, error) {
	brokers := strings.Split(dsn, ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, err
	}
	return &Session{ctx: ctx, brokers: brokers, admin: kadm.NewClient(cl), base: cl}, nil
}

func Command() *cobra.Command { return set.Command() }
