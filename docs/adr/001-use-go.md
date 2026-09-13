# ADR 001: Use Go for every service

Status: accepted

## Context

The platform is eight services plus a custom broker. They all need the same
things: lots of concurrent connections, small deployable artifacts, and one
language so shared packages (`pkg/`, `internal/`) actually stay shared.

## Decision

Write everything in Go 1.27. One module (`github.com/raven/platform`), one
binary per service under `cmd/`, shared code in `pkg/` and `internal/`.

## Alternatives

- **Node.js/TypeScript.** The console already uses TypeScript, so it would
  have meant one language for the whole repo. But the broker wants real
  control over sockets, files and goroutine-level concurrency, and Node's
  single-threaded event loop plus native-module story for things like CRC
  and `ReadAt`/`WriteAt` file I/O is exactly the kind of friction I did not
  want in the most interesting part of the project.
- **Java/Kotlin.** Battle-tested for exactly this class of system (Kafka
  itself is JVM). Rejected because the artifact story is heavier (JVM
  images, slower cold starts in tiny containers) and because I wanted the
  concurrency model to be visible in the code, not hidden behind framework
  magic. Also: personal preference, honestly.

## Consequences

Positive:

- Goroutines map directly onto the design: one goroutine per broker
  connection, one writer goroutine per partition, one semaphore-bounded
  goroutine per job. The code reads like the architecture diagram.
- Single static binaries make the Dockerfiles trivial multi-stage builds —
  the final images carry no runtime.
- The standard library covers most of what we need (HTTP server with
  graceful shutdown, `httputil.ReverseProxy` for the WS proxy, `slog` for
  structured logs). Dependencies stay few: pgx, go-redis, grpc, jwt,
  gorilla/websocket, prometheus, otel, bcrypt, uuid, and test libraries.
- `go test`, `-race`, `-bench`, `-coverprofile` are all built in. The whole
  test/benchmark story in [../testing.md](../testing.md) is stock tooling.

Negative:

- Generics are still awkward, so some shared code (e.g. the middleware
  chain) is more repetitive than the TypeScript equivalent would be.
- Error handling is verbose. Every service has visible `if err != nil`
  blocks everywhere; you either make peace with it or fight the language.
- Windows developers hit the cgo/gcc wall for `-race` (documented in
  testing.md). Not Go's fault, but it is a real onboarding bump.
