package wire

import (
	"context"
	"fmt"
	"net"
)

// Outcome classifies a request by what the transport proved about server-side
// execution, which decides whether a retry is safe.
//
// The replay rules: PreSendFailure and Rejected are Replayable — the server
// provably did not act, so re-firing the same request is safe. Delivered needs no
// retry. PostSendFailure is NEVER auto-replayed: the request was sent but its
// outcome is unknown, so re-firing a non-idempotent op could double-apply it.
// Resolve a PostSendFailure by re-probing the server's state — a lifecycle
// request carries a generation key so the probe can ask "did generation G take
// effect?" idempotently instead of re-firing.
type Outcome int

const (
	// Delivered means the server sent a complete response — success or a handler
	// error; inspect Response for which.
	Delivered Outcome = iota
	// PreSendFailure means the request never reached the server: dial, or the
	// frame write, failed. A partial write that never carried its terminating LF
	// leaves the server unable to complete a frame, so it never dispatches —
	// still a non-dispatch, still retryable.
	PreSendFailure
	// Rejected means the server proved non-dispatch with a typed Rejected reply:
	// the handler never ran (an admission bound fired) — safe to retry.
	Rejected
	// PostSendFailure means the request was sent but the outcome is unknown: the
	// response read failed after the write completed. Never auto-replay a
	// non-idempotent op — re-probe by generation instead.
	PostSendFailure
)

// String names the outcome for diagnostics.
func (o Outcome) String() string {
	switch o {
	case Delivered:
		return "delivered"
	case PreSendFailure:
		return "pre-send-failure"
	case Rejected:
		return "rejected"
	case PostSendFailure:
		return "post-send-failure"
	default:
		return fmt.Sprintf("outcome(%d)", int(o))
	}
}

// Replayable reports whether re-firing the same request is safe. True for
// PreSendFailure and Rejected (the server provably did not act); false for
// Delivered (no retry needed) and PostSendFailure (unknown — re-probe by
// generation, never re-fire).
func (o Outcome) Replayable() bool {
	return o == PreSendFailure || o == Rejected
}

// Result pairs a classified Outcome with the server's Response. Response is the
// zero value unless Outcome is Delivered or Rejected.
type Result struct {
	Outcome  Outcome
	Response Response
}

// Dialer opens a fresh connection to the server. Each Do needs its own conn — a
// conn that carried a failed request is not reusable — so Send and Call dial once
// per attempt.
type Dialer func(ctx context.Context) (net.Conn, error)

// Do writes reqFrame over conn and reads one Response, classifying the outcome. A
// write failure is PreSendFailure (retryable); a Response with Rejected set is
// the Rejected outcome (retryable); a read failure after the write completed is
// PostSendFailure (unknown — never auto-replayed). Do does not close conn.
func Do(conn net.Conn, reqFrame []byte) (Result, error) {
	f := NewFraming(conn)
	if err := f.WriteFrame(reqFrame); err != nil {
		return Result{Outcome: PreSendFailure}, fmt.Errorf("wire: send: %w", err)
	}
	var resp Response
	if err := f.ReadJSON(&resp); err != nil {
		return Result{Outcome: PostSendFailure}, fmt.Errorf("wire: read response: %w", err)
	}
	if resp.Rejected {
		return Result{Outcome: Rejected, Response: resp}, nil
	}
	return Result{Outcome: Delivered, Response: resp}, nil
}

// Send dials a fresh conn and does one request. A dial failure is PreSendFailure
// (the request never left the client). The conn is closed before Send returns.
func Send(ctx context.Context, dial Dialer, reqFrame []byte) (Result, error) {
	conn, err := dial(ctx)
	if err != nil {
		return Result{Outcome: PreSendFailure}, fmt.Errorf("wire: dial: %w", err)
	}
	defer conn.Close()
	return Do(conn, reqFrame)
}

// Probe idempotently resolves a PostSendFailure by asking the server whether a
// request's generation already took effect, on a fresh conn. It must be safe to
// call any number of times and never re-fires the original non-idempotent
// request.
type Probe func(ctx context.Context) (Result, error)

// Call sends reqFrame and applies the replay rules. A Replayable outcome
// (PreSendFailure or Rejected) is retried on a fresh conn up to retries further
// attempts; Delivered returns at once. A PostSendFailure is never re-fired: if
// probe is non-nil Call invokes it to resolve the unknown by generation,
// otherwise it returns the PostSendFailure for the caller to resolve. ctx
// cancellation ends the retry loop.
func Call(ctx context.Context, dial Dialer, reqFrame []byte, retries int, probe Probe) (Result, error) {
	var (
		res Result
		err error
	)
	for attempt := 0; ; attempt++ {
		res, err = Send(ctx, dial, reqFrame)
		if res.Outcome == PostSendFailure {
			if probe != nil {
				return probe(ctx)
			}
			return res, err
		}
		if !res.Outcome.Replayable() || attempt >= retries {
			return res, err
		}
		if cerr := ctx.Err(); cerr != nil {
			return res, cerr
		}
	}
}
