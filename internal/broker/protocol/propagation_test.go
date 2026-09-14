package protocol

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestTraceContextRoundTrip verifies the whole propagation contract without
// a broker or Jaeger: inject the W3C context into record headers at produce
// time, push the record through the real PRODUCE wire codec, extract on the
// consumer side, and assert the "job execute" span lands as the child of
// "job publish", which is itself the child of the incoming RPC span — one
// trace from Gateway to Worker.
//
// Not parallel: it swaps the global tracer provider and propagator (and
// restores them before returning).
func TestTraceContextRoundTrip(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	defer otel.SetTracerProvider(prevTP)
	defer otel.SetTextMapPropagator(prevProp)

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	tracer := tp.Tracer("raven/test")

	// Producer side: the CreateJob RPC span is the parent the gateway
	// started; "job publish" is its child, and its context goes into the
	// record headers.
	rpcCtx, rpcSpan := tracer.Start(context.Background(), "CreateJob rpc")
	publishCtx, publishSpan := tracer.Start(rpcCtx, "job publish")
	headers := InjectTraceContext(publishCtx, nil)

	if GetHeader(headers, "traceparent") == "" {
		t.Fatal("inject wrote no traceparent header")
	}

	// Over the wire: encode a real PRODUCE request, decode it back.
	wire := EncodeProduceRequest(&ProduceRequest{
		Topic:     "jobs",
		Partition: -1,
		Records: []Message{{
			Key:     []byte("job-123"),
			Value:   []byte(`{"id":"job-123","type":"send_email"}`),
			Headers: headers,
		}},
	})
	decoded, err := DecodeProduceRequest(wire)
	if err != nil {
		t.Fatalf("decode produce request: %v", err)
	}
	if len(decoded.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decoded.Records))
	}
	gotHeaders := decoded.Records[0].Headers
	if got := GetHeader(gotHeaders, "traceparent"); got == "" {
		t.Fatal("traceparent header lost through the wire codec")
	}

	// Consumer side: extract and start "job execute".
	consumerCtx := ExtractTraceContext(context.Background(), gotHeaders)
	_, execSpan := tracer.Start(consumerCtx, "job execute")

	execSpan.End()
	publishSpan.End()
	rpcSpan.End()

	spans := sr.Ended()
	if len(spans) != 3 {
		t.Fatalf("expected 3 ended spans, got %d", len(spans))
	}
	byName := make(map[string]sdktrace.ReadOnlySpan, len(spans))
	for _, s := range spans {
		byName[s.Name()] = s
	}
	rpc, ok := byName["CreateJob rpc"]
	if !ok {
		t.Fatal("CreateJob rpc span not recorded")
	}
	pub, ok := byName["job publish"]
	if !ok {
		t.Fatal("job publish span not recorded")
	}
	exec, ok := byName["job execute"]
	if !ok {
		t.Fatal("job execute span not recorded")
	}

	if pub.SpanContext().TraceID() != rpc.SpanContext().TraceID() {
		t.Error("job publish left the RPC trace")
	}
	if exec.SpanContext().TraceID() != rpc.SpanContext().TraceID() {
		t.Error("job execute left the original trace (propagation broken)")
	}
	if pub.Parent().SpanID() != rpc.SpanContext().SpanID() {
		t.Error("job publish is not a child of CreateJob rpc")
	}
	if exec.Parent().SpanID() != pub.SpanContext().SpanID() {
		t.Error("job execute is not a child of job publish")
	}
	if !exec.Parent().IsRemote() {
		t.Error("job execute parent should be remote (it crossed the broker)")
	}
}

// TestExtractWithoutHeaders: records produced before tracing existed (or by
// clients that never inject) must still be consumable; the extracted context
// simply starts a new root span.
func TestExtractWithoutHeaders(t *testing.T) {
	prevProp := otel.GetTextMapPropagator()
	defer otel.SetTextMapPropagator(prevProp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx := ExtractTraceContext(context.Background(), nil)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		t.Errorf("expected no span context from empty headers, got %v", sc)
	}
}

// TestInjectUpsertsTraceparent: re-injecting (retry republish) must replace
// the traceparent header, never accumulate duplicates.
func TestInjectUpsertsTraceparent(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	defer otel.SetTracerProvider(prevTP)
	defer otel.SetTextMapPropagator(prevProp)

	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tp.Tracer("raven/test")
	_, span1 := tracer.Start(context.Background(), "first")
	h := InjectTraceContext(trace.ContextWithSpanContext(context.Background(), span1.SpanContext()), nil)
	_, span2 := tracer.Start(context.Background(), "second")
	h = InjectTraceContext(trace.ContextWithSpanContext(context.Background(), span2.SpanContext()), h)

	count := 0
	for _, hdr := range h {
		if hdr.Key == "traceparent" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 traceparent header after re-injection, got %d", count)
	}
	span1.End()
	span2.End()
}
