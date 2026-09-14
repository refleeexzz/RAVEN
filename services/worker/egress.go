package worker

import (
	"context"
	stderrors "errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

// Egress guard for the webhook handler (JOBS-01).
//
// A webhook job carries an arbitrary payload.url. Without a guard the
// cluster itself becomes an attack proxy: an authenticated user with
// jobs:create can make the worker POST to the broker ops port, the Postgres
// port, cloud instance metadata (169.254.169.254) or any internal /debug
// endpoint. The guard refuses private/loopback/link-local/reserved targets
// at three layers:
//
//  1. Check: scheme, userinfo and DNS resolution are validated before the
//     request is built.
//  2. Dial: the transport resolves the hostname itself and dials only an IP
//     that passed the range check, so DNS rebinding between Check and dial
//     cannot smuggle an internal address in.
//  3. Redirect: every redirect target is re-validated and the chain is
//     capped at MaxWebhookRedirects hops, so a public URL cannot bounce the
//     worker into the internal network.
//
// The response body drain is capped at MaxWebhookResponseBody.
//
// Escape hatch for development and tests (httptest servers live on
// loopback): WORKER_WEBHOOK_ALLOW_PRIVATE=true, or Params.AllowPrivateWebhooks.
const (
	// MaxWebhookRedirects is the redirect budget for one webhook call.
	MaxWebhookRedirects = 2
	// MaxWebhookResponseBody caps how much of the response we drain
	// (1 MiB). The body is never parsed; the drain only lets the
	// connection be reused.
	MaxWebhookResponseBody = 1 << 20
)

// ErrRedirectLimit is returned (via CheckRedirect) when a webhook target
// bounces more than MaxWebhookRedirects times.
var ErrRedirectLimit = stderrors.New("webhook: too many redirects")

// blockedTargetError marks a target the egress policy refuses. The handler
// maps it to a PermanentError: retrying can never make a private address
// public.
type blockedTargetError struct {
	host   string
	reason string
}

func (e *blockedTargetError) Error() string {
	return fmt.Sprintf("webhook target %q is not allowed (%s)", e.host, e.reason)
}

// IsBlockedTarget reports whether err is an egress-policy refusal, including
// refusals wrapped by the HTTP client on a redirect.
func IsBlockedTarget(err error) bool {
	var bt *blockedTargetError
	return stderrors.As(err, &bt)
}

// resolverFunc resolves a hostname to IPs; replaceable in tests so the
// blocked/allowed matrix is deterministic without DNS.
type resolverFunc func(ctx context.Context, host string) ([]net.IP, error)

// EgressGuard decides which targets the webhook handler may reach.
// The zero-value question does not arise: build one with NewEgressGuard.
type EgressGuard struct {
	allowPrivate bool
	resolve      resolverFunc
}

// NewEgressGuard builds a guard. allowPrivate=true disables the range
// checks (development, integration tests); the scheme, userinfo, redirect
// and body limits always apply.
func NewEgressGuard(allowPrivate bool) *EgressGuard {
	return &EgressGuard{allowPrivate: allowPrivate}
}

// AllowPrivate reports whether private-range targets are permitted.
func (g *EgressGuard) AllowPrivate() bool { return g.allowPrivate }

// WithResolver returns a copy of the guard using a custom resolver.
// Test hook; production guards use the system resolver.
func (g *EgressGuard) WithResolver(fn resolverFunc) *EgressGuard {
	c := *g
	c.resolve = fn
	return &c
}

// blockedPrefixes are refused on top of the stdlib range classifiers
// (loopback, private, link-local, multicast, unspecified).
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "this host" — source of 0.0.0.0 tricks
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT (RFC 6598): carrier/cloud internals
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking (RFC 2544)
	netip.MustParsePrefix("224.0.0.0/4"),   // multicast
	netip.MustParsePrefix("240.0.0.0/4"),   // reserved (includes 255.255.255.255)
}

// disallowedReason classifies an IP, "" when the address is fine to egress
// to. IPv4-mapped IPv6 forms are unmapped first so ::ffff:127.0.0.1 cannot
// slip past the IPv4 rules.
func disallowedReason(ip net.IP) string {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return "unparseable address"
	}
	addr = addr.Unmap()
	switch {
	case addr.IsLoopback():
		return "loopback address"
	case addr.IsPrivate():
		return "private address"
	case addr.IsLinkLocalUnicast():
		return "link-local address"
	case addr.IsUnspecified():
		return "unspecified address"
	case addr.IsMulticast(), addr.IsLinkLocalMulticast(), addr.IsInterfaceLocalMulticast():
		return "multicast address"
	}
	for _, p := range blockedPrefixes {
		if p.Contains(addr) {
			return "reserved address range " + p.String()
		}
	}
	return ""
}

// lookupIPs resolves host through the guard's resolver (system DNS by
// default).
func (g *EgressGuard) lookupIPs(ctx context.Context, host string) ([]net.IP, error) {
	if g.resolve != nil {
		return g.resolve(ctx, host)
	}
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// CheckURL validates one target URL: scheme, userinfo and — unless private
// targets are allowed — every IP the hostname resolves to. A single blocked
// IP fails the whole target (fail-closed). DNS failures come back as plain
// errors so the handler can treat them as retryable.
func (g *EgressGuard) CheckURL(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return &blockedTargetError{host: rawURL, reason: "unparseable URL"}
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return &blockedTargetError{host: u.Host, reason: "scheme must be http or https"}
	}
	if u.User != nil {
		// http://public@127.0.0.1/ tricks URL-string eyeballing; nobody
		// legitimately needs credentials in a webhook URL.
		return &blockedTargetError{host: u.Host, reason: "userinfo is not allowed"}
	}
	host := u.Hostname()
	if host == "" {
		return &blockedTargetError{host: rawURL, reason: "missing host"}
	}
	if g.allowPrivate {
		return nil
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if reason := disallowedReason(net.IP(ip.AsSlice())); reason != "" {
			return &blockedTargetError{host: host, reason: reason}
		}
		return nil
	}
	ips, err := g.lookupIPs(ctx, host)
	if err != nil {
		return fmt.Errorf("webhook: resolve %s: %w", host, err) // retryable
	}
	if len(ips) == 0 {
		return fmt.Errorf("webhook: resolve %s: no addresses", host)
	}
	for _, ip := range ips {
		if reason := disallowedReason(ip); reason != "" {
			return &blockedTargetError{host: host, reason: reason}
		}
	}
	return nil
}

// CheckRedirect plugs into http.Client.CheckRedirect: every hop is
// re-validated and the chain is capped.
func (g *EgressGuard) CheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= MaxWebhookRedirects {
		return ErrRedirectLimit
	}
	return g.CheckURL(req.Context(), req.URL.String())
}

// dialContext resolves the hostname through the guard and dials only an IP
// that passed the range check. This closes the DNS-rebinding gap between
// the pre-flight CheckURL and the actual connection.
func (g *EgressGuard) dialContext(d *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if g.allowPrivate {
			return d.DialContext(ctx, network, addr)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		var ips []net.IP
		if ip, perr := netip.ParseAddr(host); perr == nil {
			ips = []net.IP{net.IP(ip.AsSlice())}
		} else if ips, err = g.lookupIPs(ctx, host); err != nil {
			return nil, err
		}
		var lastErr error
		for _, ip := range ips {
			if reason := disallowedReason(ip); reason != "" {
				lastErr = &blockedTargetError{host: host, reason: reason}
				continue
			}
			conn, derr := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if derr == nil {
				return conn, nil
			}
			lastErr = derr
		}
		if lastErr == nil {
			lastErr = &blockedTargetError{host: host, reason: "no usable address"}
		}
		return nil, lastErr
	}
}

// guardedTransport clones the default transport and pins its dialer to the
// guard.
func (g *EgressGuard) guardedTransport() *http.Transport {
	tr, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		tr = &http.Transport{}
	}
	t := tr.Clone()
	t.DialContext = g.dialContext(&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second})
	return t
}

// HTTPClient builds a webhook client: guarded dialer, redirect re-validation
// and an overall timeout.
func (g *EgressGuard) HTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		Transport:     g.guardedTransport(),
		CheckRedirect: g.CheckRedirect,
	}
}

// WrapClient enforces the guard on a caller-supplied client (the Params
// override): redirect policy always, guarded dialer when the transport is a
// standard *http.Transport. Custom transports keep their dialer; the
// pre-flight CheckURL and the redirect policy still apply.
func (g *EgressGuard) WrapClient(base *http.Client) *http.Client {
	c := *base
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	c.CheckRedirect = g.CheckRedirect
	switch tr := c.Transport.(type) {
	case nil:
		c.Transport = g.guardedTransport()
	case *http.Transport:
		t := tr.Clone()
		if !g.allowPrivate {
			t.DialContext = g.dialContext(&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second})
		}
		c.Transport = t
	}
	return &c
}
