package otel

import (
	"context"
	"slices"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	set.Add("propagation", "how a trace survives the jump between two services", propagate)
}

func propagate(s *Session) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(s.Ctx())
	tracer := provider.Tracer("playground")

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	s.Note("service A starts a span and attaches some baggage")
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		s.Fail(err)
		return
	}
	bag, err := baggage.New(member)
	if err != nil {
		s.Fail(err)
		return
	}

	ctxA, spanA := tracer.Start(baggage.ContextWithBaggage(s.Ctx(), bag), "service-a handler")

	s.Note("it writes the context into headers, which is all the wire ever carries")
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctxA, carrier)

	var rows [][]any
	for _, key := range slices.Sorted(slices.Values(carrier.Keys())) {
		rows = append(rows, []any{key, carrier.Get(key)})
	}
	s.Table([]string{"header", "value"}, rows)
	s.Note("traceparent is version-traceid-spanid-flags, and that is the whole handoff")

	s.Note("service B starts from a bare context and extracts those headers")
	ctxB := propagator.Extract(context.Background(), carrier)
	_, spanB := tracer.Start(ctxB, "service-b handler")

	extracted := baggage.FromContext(ctxB)
	s.Table([]string{"tenant seen by service B"}, [][]any{{extracted.Member("tenant").Value()}})

	spanB.End()
	spanA.End()

	if err := provider.ForceFlush(s.Ctx()); err != nil {
		s.Fail(err)
		return
	}

	s.Note("two spans created in two different contexts, one trace")
	rows = nil
	for _, sp := range recorder.Ended() {
		parent := "(root)"
		if sp.Parent().SpanID().IsValid() {
			parent = sp.Parent().SpanID().String()
		}
		rows = append(rows, []any{sp.Name(), sp.SpanContext().TraceID().String(), parent})
	}
	s.Table([]string{"span", "trace id", "parent span id"}, rows)

	s.Note("now the same thing, but service B forgets to extract")
	_, orphan := tracer.Start(context.Background(), "service-b without extract")
	orphan.End()
	if err := provider.ForceFlush(s.Ctx()); err != nil {
		s.Fail(err)
		return
	}

	var lost trace.SpanContext
	for _, sp := range recorder.Ended() {
		if sp.Name() == "service-b without extract" {
			lost = sp.SpanContext()
		}
	}
	s.Table([]string{"span", "trace id"}, [][]any{{"service-b without extract", lost.TraceID().String()}})
	s.Note("a brand new trace id, and the request is split in two in your backend")
	s.Note("that one missing Extract is most of the broken distributed tracing you will ever meet")
}
