# Broker security: TLS/mTLS, API-key auth, topic ACLs

Everything here protects the broker's client TCP port (default `9100`) —
the one `worker` and `jobs` talk to. It is all **opt-in**: out of the box
the broker stays plaintext and open so local dev and the existing test
suite keep working untouched. Turn the knobs on for anything real.

Three independent layers, each useful alone, strongest together:

1. **TLS** — encrypts the wire. Env: `BROKER_TLS_CERT_FILE` / `BROKER_TLS_KEY_FILE`.
2. **API-key authentication** — clients prove who they are. Env: `BROKER_API_KEYS`.
3. **Topic ACLs** — once known, keys are limited to their topics. Same env.

> Honest advice: run auth **with** TLS. The API key's secret travels inside
> the AUTH frame payload — on a plaintext port anyone sniffing can read it.
> TLS-first is how the pieces are meant to compose.

---

## TLS

Set both of these and the client listener goes TLS:

```bash
BROKER_TLS_CERT_FILE=/etc/raven/broker.crt
BROKER_TLS_KEY_FILE=/etc/raven/broker.key
```

Policy, pinned in code (`internal/broker/tls.go`):

- **TLS 1.2 floor, 1.3 preferred.** A modern client negotiates 1.3; a
  1.2-only client still connects; anything older is refused.
- TLS 1.2 cipher suites are pinned to ECDHE + AEAD only
  (AES-128/256-GCM, ChaCha20-Poly1305). TLS 1.3 suites are fixed by the
  Go runtime and always AEAD.
- The TLS handshake runs per-connection with a **10s deadline**, so a
  slow-loris peer can't park a goroutine before the first frame.

Self-signed playground cert:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout broker.key -out broker.crt -days 30 -nodes \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

### Hot-reload (no restart on rotation)

The broker **polls** the cert/key files every `BROKER_TLS_RELOAD_SEC`
(default **5s**). When either mtime changes it reloads the pair; new
handshakes get the new certificate, live connections keep theirs.

Why polling and not stat-per-handshake (a deliberate choice):

- Handshakes are the hot path; two syscalls per handshake buy nothing
  when rotations are operator-paced.
- A half-written rotation at handshake time would **fail new
  connections**. With polling, a bad reload just keeps the serving cert
  (and logs a warning); the next tick retries, so a healed rotation
  loads without a restart.

### mTLS (client certificates)

```bash
BROKER_TLS_CLIENT_CA_FILE=/etc/raven/client-ca.crt
```

With this set the server requires **and verifies** a client certificate
signed by that CA (`RequireAndVerifyClientCert`). No cert, wrong CA →
handshake fails. Note mTLS today only gates *entry* — the client cert's
identity is logged but **not** mapped to API-key principals; ACLs still
come from `BROKER_API_KEYS` (see limitations).

---

## Authentication (AUTH frame, protocol v1.1)

New wire opcode: **`AUTH` (0x0A)**, JSON payload:

```json
{"id": "worker", "secret": "the-plaintext-secret"}
```

Success answers `AUTH` with the key's grants echoed back. Failure answers
`ERROR` with code **`UNAUTHENTICATED`**. Two rules when keys are
configured:

- Until AUTH succeeds, **only AUTH frames pass** — anything else gets
  `UNAUTHENTICATED`.
- **5 strikes and you're out** (`server.WithMaxAuthFailures`): failed
  AUTHs *and* pre-auth frames count; the final error frame is flushed,
  then the connection closes. Unauthenticated connections also get a
  **10s** auth timeout instead of the 5-minute idle one.

### Wire compatibility (no negotiation needed)

v1.1 is purely additive — the frame format is untouched:

| Client | Broker | What happens |
|---|---|---|
| v1.1, no creds | auth on | First op → `UNAUTHENTICATED` error frame. Clear. |
| v1.1, with creds | auth on | AUTH → ops. Normal. |
| v1.1, with creds | open / v1 | AUTH → `UNKNOWN_OPCODE` → client logs "open mode" and proceeds. |
| v1 (no AUTH) | open / v1 | Exactly as before. |

### Configuring keys

`BROKER_API_KEYS` is a JSON array. The broker never sees the plaintext
secret at rest — you store **its SHA-256 (hex)**:

```bash
SECRET_SHA256=$(printf '%s' 'the-plaintext-secret' | sha256sum | cut -d' ' -f1)
```

```json
[
  {"id": "worker", "secret_sha256": "<hex>",
   "topics_read": ["jobs", "jobs.*"], "topics_write": ["jobs", "jobs.*"]},
  {"id": "jobs-svc", "secret_sha256": "<hex>",
   "topics_read": [], "topics_write": ["jobs", "jobs-dlq"]},
  {"id": "admin", "secret_sha256": "<hex>", "admin": true}
]
```

Guarantees and guardrails:

- Comparison is **constant-time** (sha256 + `subtle.ConstantTimeCompare`),
  and unknown ids compare against a fixed dummy hash — timing can't
  reveal which key ids exist.
- Secrets are **never logged**; failures log the key id only.
- Malformed `BROKER_API_KEYS` **fails the boot** (fail closed). A typo in
  security config must never silently boot an open broker.
- **No keys configured = open mode** + a loud WARN at boot. That's the
  dev-compatible default, not a posture.

---

## Topic ACLs

ACLs are enforced **per frame**, after authentication, by operation:

| Operation | Required grant |
|---|---|
| `PRODUCE` | `topics_write` on the topic |
| `FETCH` | `topics_read` on the topic |
| `COMMIT_OFFSET`, `FETCH_OFFSET` | `topics_read` on the topic |
| `JOIN_GROUP` | `topics_read` on **every** listed topic (one denied topic sinks the join) |
| `CREATE_TOPIC`, `LIST_TOPICS` | `admin: true` |
| `LEAVE_GROUP`, `HEARTBEAT` | any authenticated key |

Patterns: `"jobs"` exact, `"*"` everything, `"jobs.*"` prefix (matches
`jobs.p1`, **not** bare `jobs`). `admin` implies every grant.

Denial → `ERROR` frame with code **`UNAUTHORIZED`**. The Go client
surfaces typed checks:

```go
if client.IsUnauthenticated(err) { /* fix credentials */ }
if client.IsUnauthorized(err)    { /* fix grants, don't retry */ }
```

## Using the Go client

```go
tlsCfg := &tls.Config{RootCAs: pool} // pool trusts the broker's CA

p := client.NewProducer("broker:9100",
    client.WithProducerTLS(tlsCfg),
    client.WithProducerAuth("worker", os.Getenv("BROKER_API_SECRET")),
)
c := client.NewConsumer("broker:9100", "workers", topics, handler,
    client.WithConsumerTLS(tlsCfg),
    client.WithConsumerAuth("worker", os.Getenv("BROKER_API_SECRET")),
)
a := client.NewAdmin("broker:9100",
    client.WithAdminTLS(tlsCfg),
    client.WithAdminAuth("admin", os.Getenv("BROKER_ADMIN_SECRET")),
)
```

All options are additive — existing `NewProducer(addr)` style code keeps
compiling and behaves exactly as before.

## Metrics

- `raven_broker_auth_failures_total` — rejected AUTHs + pre-auth frames.
- `raven_broker_acl_denied_total` — authenticated but not allowed.

Both are deliberately **label-free**: key ids in labels would be a
cardinality leak; the structured logs carry the detail (key id, remote
addr, access, topic — never the secret).

## Limitations (being honest)

1. **Key rotation requires a broker restart.** `BROKER_API_KEYS` is read
   once at boot (the TLS *certificate* hot-reloads; API keys do not).
   Roll keys by overlapping deploys: add the new key, restart, retire the
   old one.
2. **Static, env-file keys only.** This is bootstrap auth. The SaaS phase
   replaces it with DB-backed keys behind the same
   `server.Authenticator` interface — the wire protocol and ACL model
   won't change.
3. **Secrets live in env vars.** Use your platform's secret mounting;
   never commit the JSON. Prefer per-service keys so revocation is
   surgical.
4. **mTLS identity ≠ principal.** A verified client cert gates entry but
   doesn't select grants; ACLs come from the API key. Cert-to-principal
   mapping is future work.
5. **No per-IP rate limiting** beyond the 5-strikes-per-connection
   budget. Pair with network-level controls (the port should be
   cluster-private anyway).
6. **Over-capacity rejection is silent under TLS.** Plaintext brokers
   send a clean `BROKER_BUSY` frame before close; under TLS there is no
   safe cleartext channel mid-handshake, so the connection just closes.
7. **AUTH secret in the payload.** It's hashed server-side and compared
   in constant time, but on the wire it's plaintext — that's why TLS is
   strongly recommended (see top of page).
