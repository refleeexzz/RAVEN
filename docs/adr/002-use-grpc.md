# ADR 002: gRPC between services, REST at the edge

Status: accepted

## Context

Services need to call each other: gateway → auth/users/jobs. The question
was whether to use one protocol everywhere or split: REST for the public
edge, something else inside.

## Decision

REST + JSON at the public edge (gateway `:8080`), gRPC + protobuf for every
service-to-service call (`:9081`, `:9082`, `:9083`). The gateway is the
translator. The contracts live in `proto/`, and generated code lands in
`internal/gen/` (`make proto` regenerates).

## Alternatives

- **REST everywhere.** Simplest, one less toolchain. Rejected because the
  internal contracts benefit from being typed: the proto file is
  documentation that cannot drift from the implementation, and codegen
  removes a whole class of "the JSON field was renamed" bugs.
- **gRPC everywhere (with grpc-gateway for the edge).** Tempting — the
  `grpc-gateway` dependency is already in the tree transitively. Rejected
  because the gateway does much more than transcoding: it owns auth
  caching, rate limiting, circuit breakers, the WebSocket proxy and the
  Redis-backed worker registry. Hand-written REST handlers make that logic
  explicit; a transcoder would hide it.

## Consequences

Positive:

- Typed contracts with compile-time checking on both ends. Changing a proto
  breaks the build everywhere it matters — exactly what you want.
- Real deadline propagation: the gateway's 5 s per-call timeout travels as
  gRPC metadata, so a slow upstream call dies on both sides instead of
  leaking goroutines.
- gRPC status codes map cleanly onto the platform error kinds, and the
  gateway translates them into the public error envelope
  (`services/gateway/errors.go`). One error vocabulary end to end.
- Streaming is there when we want it (e.g. a future "watch job" RPC)
  without changing the transport.
- Every service gets `otelgrpc` tracing for free via the client stats
  handler.

Negative:

- Two protocols to understand, and a translation layer (the gateway
  handlers) that is pure boilerplate in both directions.
- Protobuf codegen adds a build step and a checked-in `internal/gen/`
  directory that can go stale if someone edits a proto and forgets
  `make proto`.
- gRPC is invisible to curl and browsers, so debugging internal calls needs
  `grpcurl` or logs. REST inside would have been easier to poke by hand.
