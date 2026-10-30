package bigdata

import (
	"io"

	"github.com/jackc/pgx/v5/pgxpool"

	"playground/internal/bigdata/pb"
)

// server is the grpc service, every handler is backed by postgres
type server struct {
	pb.UnimplementedBigdataServer
	pool *pgxpool.Pool
}

// fromStream turns a stream of columnar batches into the row source COPY
// pulls from, so rows go from the grpc frame to the copy buffer with no
// slice of the whole load in between. pgx encodes each row before asking
// for the next, so one row slice is reused throughout
func fromStream[B any](cols int, recv func() (B, error), size func(B) int, row func(B, int, []any)) func() ([]any, error) {
	var cur B
	var i, n int
	vals := make([]any, cols)
	return func() ([]any, error) {
		for i >= n {
			b, err := recv()
			if err == io.EOF {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			cur, i, n = b, 0, size(b)
		}
		row(cur, i, vals)
		i++
		return vals, nil
	}
}
