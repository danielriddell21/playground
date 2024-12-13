package otel

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func init() {
	set.Add("metrics", "counters, histograms and a gauge read on demand", metrics)
}

func metrics(s *Session) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(s.Ctx())

	meter := provider.Meter("playground")

	processed, err := meter.Int64Counter("orders.processed",
		metric.WithDescription("orders that made it through"))
	if err != nil {
		s.Fail(err)
		return
	}
	latency, err := meter.Float64Histogram("order.latency",
		metric.WithUnit("ms"), metric.WithExplicitBucketBoundaries(5, 25, 100))
	if err != nil {
		s.Fail(err)
		return
	}

	depth := 7
	if _, err := meter.Int64ObservableGauge("queue.depth",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(depth))
			return nil
		})); err != nil {
		s.Fail(err)
		return
	}

	s.Note("record some work, split by a label")
	rng := rand.New(rand.NewSource(7))
	for i := range 40 {
		region := "north"
		if i%3 == 0 {
			region = "south"
		}
		set := metric.WithAttributes(attribute.String("region", region))
		processed.Add(s.Ctx(), 1, set)
		latency.Record(s.Ctx(), rng.Float64()*120, set)
	}

	s.Note("a manual reader collects only when you ask it to")
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(s.Ctx(), &collected); err != nil {
		s.Fail(err)
		return
	}

	var rows [][]any
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					rows = append(rows, []any{m.Name, "counter", labels(dp.Attributes), fmt.Sprint(dp.Value)})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					rows = append(rows, []any{m.Name, "gauge", labels(dp.Attributes), fmt.Sprint(dp.Value)})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					rows = append(rows, []any{m.Name, "histogram", labels(dp.Attributes),
						fmt.Sprintf("count=%d sum=%.0f", dp.Count, dp.Sum)})
				}
			}
		}
	}
	slices.SortFunc(rows, func(a, b []any) int {
		return strings.Compare(a[0].(string)+a[2].(string), b[0].(string)+b[2].(string))
	})
	s.Table([]string{"instrument", "kind", "labels", "value"}, rows)

	s.Note("the histogram keeps buckets, not just a total")
	rows = nil
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				for i, bound := range dp.Bounds {
					rows = append(rows, []any{labels(dp.Attributes), fmt.Sprintf("<= %.0fms", bound), dp.BucketCounts[i]})
				}
				rows = append(rows, []any{labels(dp.Attributes), "the rest", dp.BucketCounts[len(dp.Bounds)]})
			}
		}
	}
	s.Table([]string{"labels", "bucket", "count"}, rows)

	s.Note("every label combination is its own series, which is how cardinality gets away from you")
}

func labels(set attribute.Set) string {
	var parts []string
	for _, kv := range set.ToSlice() {
		parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, ",")
}
