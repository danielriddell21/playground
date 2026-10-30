package bigdata

import (
	"io"
	"iter"

	"google.golang.org/grpc"

	"playground/internal/bigdata/pb"
)

// batches reuses one message for the whole run: grpc has marshalled it by
// the time Send returns, so refilling it is safe
func batches[B any](lo, hi int, fill func(*B, int, int)) iter.Seq[*B] {
	return func(yield func(*B) bool) {
		b := new(B)
		for blo, bhi := range chunks(hi-lo, batchSize) {
			fill(b, lo+blo, lo+bhi)
			if !yield(b) {
				return
			}
		}
	}
}

func sendAll[B any](st grpc.ClientStreamingClient[B, pb.Summary], bs iter.Seq[*B]) (*pb.Summary, error) {
	for b := range bs {
		if err := st.Send(b); err != nil {
			return nil, err
		}
	}
	return st.CloseAndRecv()
}

// sendAcked keeps at most window batches unacknowledged, reading acks on
// the side so sending never waits on a round trip unless it is that far ahead
func sendAcked[B any](st grpc.BidiStreamingClient[B, pb.Ack], window int, bs iter.Seq[*B]) error {
	slots := make(chan struct{}, window)
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := st.Recv(); err != nil {
				if err == io.EOF {
					err = nil
				}
				done <- err
				return
			}
			<-slots
		}
	}()
	for b := range bs {
		select {
		case slots <- struct{}{}:
		case err := <-done:
			return err
		}
		if err := st.Send(b); err != nil {
			return err
		}
	}
	if err := st.CloseSend(); err != nil {
		return err
	}
	return <-done
}

// recvAll drains a server stream
func recvAll[R any](st grpc.ServerStreamingClient[R], each func(*R)) error {
	for {
		r, err := st.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		each(r)
	}
}
