package grpcplay

import (
	"context"
	"fmt"
	"net"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"playground/internal/grpcplay/pb"
	"playground/internal/play"
)

type Session struct {
	ctx    context.Context
	conn   *grpc.ClientConn
	srv    *grpc.Server
	client pb.LibraryClient
	err    error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	s.conn.Close()
	s.srv.Stop()
}

func (s *Session) Ctx() context.Context { return s.ctx }

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
	play.Table(headers, rows)
}

func logUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	fmt.Printf("  [interceptor] calling %s\n", method)
	err := invoker(ctx, method, req, reply, cc, opts...)
	fmt.Printf("  [interceptor] %s returned err=%v\n", method, err)
	return err
}

var set = &play.Set[*Session]{
	Use:        "grpc",
	Short:      "grpc playgrounds",
	DSNEnv:     "GRPC_TARGET",
	DefaultDSN: "in-process",
	Connect:    connect,
}

func connect(ctx context.Context, _ string) (*Session, error) {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterLibraryServer(srv, &server{})
	go srv.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(logUnary),
	)
	if err != nil {
		srv.Stop()
		return nil, err
	}

	return &Session{ctx: ctx, conn: conn, srv: srv, client: pb.NewLibraryClient(conn)}, nil
}

func Command() *cobra.Command { return set.Command() }
