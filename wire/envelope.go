package wire

import (
	"context"
	"encoding/json"
)

// Request is the decoded input handed to a Handler: the routing Op, the
// per-tenant serialization key (empty opts out of per-tenant serialization), the
// OS-authenticated Peer, and the raw request frame the handler unmarshals its own
// payload from. wire never interprets Frame beyond the Router hook, so the
// consumer's payload shape stays opaque to this package.
type Request struct {
	Op     Op
	Tenant string
	Peer   Peer
	Frame  []byte
}

// Response is the server's single reply frame per request. The server authors it
// even when no handler runs — a Rejected non-dispatch — so wire owns this shape
// while leaving the request frame to the consumer's Router. Version carries
// Server.Version verbatim (never wire's own). On success Payload holds the
// handler's marshaled result; Err holds a handler or dispatch error string;
// Rejected marks a proven non-dispatch (the handler never ran — safe to retry),
// with Reason naming the admission bound that fired.
type Response struct {
	Version  string          `json:"version,omitempty"`
	Rejected bool            `json:"rejected,omitempty"`
	Reason   string          `json:"reason,omitempty"`
	Err      string          `json:"err,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

// Handler runs one request and returns a value the server marshals into
// Response.Payload, or an error surfaced in Response.Err. ctx is cancelled when
// the op deadline elapses or the peer disconnects.
type Handler func(ctx context.Context, req Request) (any, error)

// Router extracts the routing Op and the per-tenant serialization key from a raw
// request frame. A tenant of "" opts the request out of per-tenant
// serialization. Its error is reported to the client as a route failure and no
// handler runs. The consumer owns the frame's JSON shape; this hook is wire's
// only window into it.
type Router func(frame []byte) (op Op, tenant string, err error)
