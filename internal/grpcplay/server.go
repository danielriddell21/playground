package grpcplay

import (
	"context"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"playground/internal/grpcplay/pb"
)

type server struct {
	pb.UnimplementedLibraryServer
}

var catalogue = map[string]*pb.Book{
	"b1": {Id: "b1", Title: "notes on the engine", Year: 1843, Tags: []string{"maths"}},
	"b2": {Id: "b2", Title: "on looms", Year: 1845, Tags: []string{"maths", "craft"}},
	"b3": {Id: "b3", Title: "compiler design", Year: 1952, Tags: []string{"code"}},
	"b4": {Id: "b4", Title: "on networks", Year: 1974, Tags: []string{"code"}},
}

func (s *server) GetBook(ctx context.Context, req *pb.BookRequest) (*pb.Book, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-caller"); len(v) > 0 {
			grpc.SetHeader(ctx, metadata.Pairs("x-greeted", v[0]))
		}
	}
	if req.Id == "slow" {
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "took too long")
		}
	}
	b, ok := catalogue[req.Id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no book with id %q", req.Id)
	}
	return b, nil
}

func (s *server) Search(req *pb.SearchRequest, stream pb.Library_SearchServer) error {
	for _, id := range slices.Sorted(maps.Keys(catalogue)) {
		b := catalogue[id]
		if !strings.Contains(strings.ToLower(b.Title), strings.ToLower(req.Term)) {
			continue
		}
		if err := stream.Send(b); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (s *server) Upload(stream pb.Library_UploadServer) error {
	var accepted, rejected int32
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.UploadSummary{Accepted: accepted, Rejected: rejected})
		}
		if err != nil {
			return err
		}
		if b.Title == "" {
			rejected++
			continue
		}
		catalogue[b.Id] = b
		accepted++
	}
}

func (s *server) Chat(stream pb.Library_ChatServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		reply := &pb.ChatMessage{From: "server", Text: "you said: " + msg.Text}
		if err := stream.Send(reply); err != nil {
			return err
		}
	}
}
