package otel

import (
	"errors"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func init() {
	set.Add("traces", "spans nest, carry attributes and remember what went wrong", traces)
}

func traces(s *Session) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(s.Ctx())

	tracer := provider.Tracer("playground")

	ctx, root := tracer.Start(s.Ctx(), "GET /orders")
	root.SetAttributes(
		attribute.String("http.route", "/orders"),
		attribute.Int("http.status_code", 200),
	)

	lookupCtx, lookup := tracer.Start(ctx, "postgres query")
	lookup.SetAttributes(attribute.String("db.system", "postgresql"))
	lookup.AddEvent("acquired connection from pool")
	time.Sleep(15 * time.Millisecond)
	lookup.End()

	_, cache := tracer.Start(lookupCtx, "redis lookup")
	cache.AddEvent("cache miss")
	time.Sleep(5 * time.Millisecond)
	cache.End()

	_, upstream := tracer.Start(ctx, "call payments")
	time.Sleep(8 * time.Millisecond)
	upstream.RecordError(errors.New("upstream refused the connection"))
	upstream.SetStatus(codes.Error, "payments unreachable")
	upstream.End()

	root.End()

	if err := provider.ForceFlush(s.Ctx()); err != nil {
		s.Fail(err)
		return
	}

	spans := recorder.Ended()
	slices.SortFunc(spans, func(a, b sdktrace.ReadOnlySpan) int {
		return a.StartTime().Compare(b.StartTime())
	})

	byID := map[string]string{}
	for _, sp := range spans {
		byID[sp.SpanContext().SpanID().String()] = sp.Name()
	}

	s.Note("one trace, four spans, all sharing a trace id")
	var rows [][]any
	for _, sp := range spans {
		parent := "(root)"
		if p := sp.Parent().SpanID(); p.IsValid() {
			parent = byID[p.String()]
		}
		rows = append(rows, []any{
			sp.Name(),
			parent,
			sp.EndTime().Sub(sp.StartTime()).Round(time.Millisecond),
			sp.Status().Code.String(),
		})
	}
	s.Table([]string{"span", "child of", "took", "status"}, rows)

	s.Note("every span in that table shares one trace id")
	ids := map[string]bool{}
	for _, sp := range spans {
		ids[sp.SpanContext().TraceID().String()] = true
	}
	var traceIDs []string
	for id := range ids {
		traceIDs = append(traceIDs, id)
	}
	s.Table([]string{"distinct trace ids", "value"}, [][]any{{len(traceIDs), strings.Join(traceIDs, ",")}})

	s.Note("attributes and events ride along with the span that recorded them")
	rows = nil
	for _, sp := range spans {
		for _, kv := range sp.Attributes() {
			rows = append(rows, []any{sp.Name(), "attribute", string(kv.Key) + "=" + kv.Value.Emit()})
		}
		for _, ev := range sp.Events() {
			rows = append(rows, []any{sp.Name(), "event", ev.Name})
		}
	}
	s.Table([]string{"span", "kind", "detail"}, rows)

	s.Note("an error is data on the span, it does not stop anything")
	s.Note("the parent still finished with status Unset, only the child is Error")
}
