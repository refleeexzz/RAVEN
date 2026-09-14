package client

import "github.com/refleeexzz/RAVEN/internal/broker/protocol"

// IsUnauthenticated reports whether err is the broker's
// UNAUTHENTICATED error: the connection needs (valid) credentials.
// Typed wrapper over protocol.IsCode so services don't hardcode the
// wire code.
func IsUnauthenticated(err error) bool {
	return protocol.IsCode(err, protocol.CodeUnauthenticated)
}

// IsUnauthorized reports whether err is the broker's UNAUTHORIZED
// error: the API key is valid but its ACL does not allow the
// operation on that topic. Retrying with the same key never succeeds —
// fix the grants (or the topic), don't loop.
func IsUnauthorized(err error) bool {
	return protocol.IsCode(err, protocol.CodeUnauthorized)
}
