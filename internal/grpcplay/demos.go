package grpcplay

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"playground/internal/grpcplay/pb"
)

func init() {
	set.Add("unary", "one request, one response, plus typed errors and deadlines", unary)
	set.Add("streams", "server, client and bidirectional streaming", streams)
	set.Add("metadata", "headers, trailers and interceptors", meta)
}

func unary(s *Session) {
	s.Note("a plain call")
	book, err := s.client.GetBook(s.Ctx(), &pb.BookRequest{Id: "b1"})
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"id", "title", "year", "tags"},
		[][]any{{book.Id, book.Title, book.Year, book.Tags}})

	s.Note("errors are codes, not strings")
	_, err = s.client.GetBook(s.Ctx(), &pb.BookRequest{Id: "nope"})
	st, _ := status.FromError(err)
	s.Note("  code=%s message=%q", st.Code(), st.Message())
	s.Note("  is it NotFound? %v", st.Code() == codes.NotFound)

	s.Note("the caller sets the deadline and the server sees it")
	ctx, cancel := context.WithTimeout(s.Ctx(), 300*time.Millisecond)
	defer cancel()
	_, err = s.client.GetBook(ctx, &pb.BookRequest{Id: "slow"})
	st, _ = status.FromError(err)
	s.Note("  code=%s message=%q", st.Code(), st.Message())
}

func streams(s *Session) {
	s.Note("server streaming, results arrive as they are ready")
	stream, err := s.client.Search(s.Ctx(), &pb.SearchRequest{Term: "on"})
	if err != nil {
		s.Fail(err)
		return
	}
	start := time.Now()
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.Fail(err)
			return
		}
		s.Note("  +%-6s %s", time.Since(start).Round(10*time.Millisecond), b.Title)
	}

	s.Note("client streaming, push many and get one summary")
	up, err := s.client.Upload(s.Ctx())
	if err != nil {
		s.Fail(err)
		return
	}
	for _, b := range []*pb.Book{
		{Id: "b5", Title: "on compilers", Year: 1980},
		{Id: "b6", Title: ""},
		{Id: "b7", Title: "on caches", Year: 1990},
	} {
		if err := up.Send(b); err != nil {
			s.Fail(err)
			return
		}
	}
	summary, err := up.CloseAndRecv()
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"accepted", "rejected"}, [][]any{{summary.Accepted, summary.Rejected}})

	s.Note("bidirectional, both sides talk at once")
	chat, err := s.client.Chat(s.Ctx())
	if err != nil {
		s.Fail(err)
		return
	}
	done := make(chan error, 1)
	go func() {
		for {
			msg, err := chat.Recv()
			if err == io.EOF {
				done <- nil
				return
			}
			if err != nil {
				done <- err
				return
			}
			s.Note("  <- %s: %s", msg.From, msg.Text)
		}
	}()
	for _, text := range []string{"hello", "still there?", "bye"} {
		s.Note("  -> client: %s", text)
		if err := chat.Send(&pb.ChatMessage{From: "client", Text: text}); err != nil {
			s.Fail(err)
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	chat.CloseSend()
	if err := <-done; err != nil {
		s.Fail(err)
	}
}

func meta(s *Session) {
	s.Note("the unary interceptor wraps every call")
	ctx := metadata.AppendToOutgoingContext(s.Ctx(), "x-caller", "playground")

	var header, trailer metadata.MD
	book, err := s.client.GetBook(ctx, &pb.BookRequest{Id: "b2"},
		grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("  got %q", book.Title)

	s.Note("metadata the server sent back")
	s.Note("  x-greeted=%v", header.Get("x-greeted"))
	s.Note("  content-type=%v", header.Get("content-type"))

	s.Note("metadata travels with failures too")
	_, err = s.client.GetBook(ctx, &pb.BookRequest{Id: "missing"})
	st, _ := status.FromError(err)
	s.Note("  code=%s message=%q", st.Code(), st.Message())
}
