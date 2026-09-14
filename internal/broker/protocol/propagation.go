package protocol

import (
	"context"

	"go.opentelemetry.io/otel"
)

// W3C tracecontext over record headers.
//
// The broker protocol carries per-record headers end to end (PRODUCE
// write path, segment files, FETCH read path), which makes them the
// natural trace-context carrier: the producer injects `traceparent`
// (and `tracestate` when present) into the record, the broker stores and
// returns them untouched, and the consumer extracts them to continue the
// trace. No wire-protocol change is needed — headers existed from v1.
//
// Attribute discipline: only propagation keys are ever written here.
// Payloads, emails and other high-cardinality or sensitive data must
// never become headers or span attributes.

// HeaderCarrier adapts a record header list to
// propagation.TextMapCarrier. Set replaces an existing key (headers are
// a map semantically, not a multiset), so re-injecting never
// accumulates duplicate traceparent entries.
type HeaderCarrier struct {
	Headers *[]Header
}

// Get returns the first header value for key, case-insensitively as the
// W3C spec requires for traceparent/tracestate.
func (c HeaderCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if len(h.Key) == len(key) && equalFoldASCII(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

// Set upserts key. Header keys and values are capped at 64 KiB on disk;
// W3C propagation values are far below that.
func (c HeaderCarrier) Set(key, value string) {
	for i, h := range *c.Headers {
		if len(h.Key) == len(key) && equalFoldASCII(h.Key, key) {
			(*c.Headers)[i].Value = []byte(value)
			return
		}
	}
	*c.Headers = append(*c.Headers, Header{Key: key, Value: []byte(value)})
}

// Keys lists the header keys, as TextMapCarrier requires.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectTraceContext writes the active span context of ctx into headers
// using the process-wide propagator (pkg/tracing.Setup installs W3C
// TraceContext + Baggage). It returns the extended header list; the
// input slice is never mutated in place beyond upserting in place when
// capacity allows, so callers should always use the returned slice.
// With no active span (or a no-op provider) the headers come back
// unchanged, so producing stays free of tracing overhead when OTEL is
// disabled.
func InjectTraceContext(ctx context.Context, headers []Header) []Header {
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier{Headers: &headers})
	return headers
}

// ExtractTraceContext reads the W3C trace context out of record headers
// and returns a context carrying it. With no traceparent header the
// input context comes back unchanged and the consumer starts a root
// span — messages produced before tracing existed stay consumable.
func ExtractTraceContext(ctx context.Context, headers []Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier{Headers: &headers})
}

// GetHeader returns the first header value for key (ASCII
// case-insensitive), or "" when absent. Convenience for consumers that
// want one header without a carrier.
func GetHeader(headers []Header, key string) string {
	return HeaderCarrier{Headers: &headers}.Get(key)
}

// equalFoldASCII compares two ASCII strings without allocating. Header
// keys in practice are lowercase ASCII ("traceparent"); this keeps the
// hot path free of strings.ToLower allocations.
func equalFoldASCII(a, b string) bool {
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
