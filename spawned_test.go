package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	spawnedSchema    = "spawned.v1"
	spawnedEchoOp    = "child.echo.v1"
	spawnedClaimOp   = "child.claim.v1"
	spawnedSessionOp = "child.session.v1"
	spawnedFailOp    = "child.fail.v1"
	spawnedHoldOp    = "child.hold.v1"
)

var spawnedLimits = Limits{MaxFrame: 1 << 20, Concurrency: 4}

// takeCollision is D8(ii)'s one message: Conn and Business share a single take,
// so the collision reads the same whichever verb arrives second.
const takeCollision = "daemonkit: Child.Conn and Child.Business consume the one channel end, once between them"

func childServeSpawned(contract Contract) int {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := ServeSpawned(ctx, contract, func(_ context.Context, req Request) (Reply, error) {
		switch req.Op {
		case spawnedEchoOp:
			return Reply{Body: req.Body}, nil
		case spawnedClaimOp:
			if _, err := ClaimHandoff(); err != nil {
				return Reply{Body: []byte("second-claim-refused")}, nil
			}
			return Reply{Body: []byte("second-claim-admitted")}, nil
		case spawnedSessionOp:
			return Reply{Body: fmt.Appendf(nil, "session=%d caller=%d", req.Session.ID(), req.Caller.PID)}, nil
		case spawnedFailOp:
			return Reply{}, &ProductError{Code: "child_refused", Message: "the child said no"}
		}
		return Reply{}, fmt.Errorf("child: unknown op %q", req.Op)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: %v\n", err)
		return 68
	}
	return 0
}

// childServeSpawnedDisconnect serves one session whose handler blocks on its
// own Session.Disconnected, then reports whether the edge fired while the
// handler was still in flight — Done still open — carrying the verdict out as
// the exit code.
func childServeSpawnedDisconnect() int {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	verdicts := make(chan string, 1)
	served := ServeSpawned(ctx, Contract{Schema: spawnedSchema}, func(_ context.Context, req Request) (Reply, error) {
		if req.Op != spawnedHoldOp {
			return Reply{}, fmt.Errorf("child: unknown op %q", req.Op)
		}
		fmt.Fprintln(os.Stderr, "child: holding")
		select {
		case <-req.Session.Disconnected():
		case <-ctx.Done():
			verdicts <- "disconnected-never-closed"
			return Reply{}, ctx.Err()
		}
		select {
		case <-req.Session.Done():
			verdicts <- "done-closed-mid-handler"
		default:
			verdicts <- "disconnected-before-return"
		}
		return Reply{}, errors.New("peer gone")
	})
	select {
	case verdict := <-verdicts:
		if verdict != "disconnected-before-return" {
			fmt.Fprintf(os.Stderr, "child: %s\n", verdict)
			return 69
		}
		return 0
	default:
		fmt.Fprintf(os.Stderr, "child: no handler observed the disconnect (ServeSpawned = %v)\n", served)
		return 70
	}
}

// childClaimThenServe is D8(ii)'s child side in the ClaimHandoff-first order:
// the claim it already holds carries the verdict out.
func childClaimThenServe() int {
	conn, err := ClaimHandoff()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: first claim: %v\n", err)
		return 65
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	served := ServeSpawned(ctx, Contract{Schema: spawnedSchema}, func(context.Context, Request) (Reply, error) {
		return Reply{}, nil
	})
	verdict := "serve-after-claim-admitted"
	if errors.Is(served, errHandoffClaimed) {
		verdict = "serve-after-claim-refused"
	}
	if _, err := fmt.Fprintf(conn, "%s\n", verdict); err != nil {
		return 66
	}
	return 0
}

func spawnBusinessChild(t *testing.T, role string) (*Child, *Capture) {
	t.Helper()
	stderr := NewCapture(4 << 10)
	spawn := childCmd(t, role)
	spawn.Limits = spawnedLimits
	child, err := ownedScope(t).Spawn(bounded(t, 30*time.Second), spawn, ChannelHandoff, stderr)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	return child, stderr
}

func TestChildBusinessRoundTripsOverTheSpawnedHandoff(t *testing.T) {
	child, stderr := spawnBusinessChild(t, "serve-spawned")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lane, err := child.Business(ctx, Contract{Schema: spawnedSchema})
	if err != nil {
		t.Fatalf("Business() = %v (stderr %q)", err, stderr.Bytes())
	}
	reply, err := lane.Call(ctx, spawnedEchoOp, []byte("ping"))
	if err != nil || string(reply.Body) != "ping" {
		t.Fatalf("Call(%s) = %q, %v (stderr %q)", spawnedEchoOp, reply.Body, err, stderr.Bytes())
	}

	session, err := lane.Call(ctx, spawnedSessionOp, nil)
	if err != nil {
		t.Fatalf("Call(%s) = %v", spawnedSessionOp, err)
	}
	if want := fmt.Sprintf("session=1 caller=%d", os.Getpid()); string(session.Body) != want {
		t.Fatalf("the child saw %q, want %q", session.Body, want)
	}

	failed, err := lane.Call(ctx, spawnedFailOp, nil)
	var product *ProductError
	if !errors.As(err, &product) || product.Code != "child_refused" {
		t.Fatalf("Call(%s) = %q, %v, want the child's coded product failure", spawnedFailOp, failed.Body, err)
	}
	if Undispatched(err) {
		t.Error("Undispatched() = true for a product failure the child delivered")
	}

	taken, err := child.Conn()
	if err == nil {
		_ = taken.Close()
		t.Fatal("Conn() handed out a channel end Business already took")
	}
	if !strings.Contains(err.Error(), takeCollision) {
		t.Fatalf("Conn() after Business() = %v, want the single-take refusal", err)
	}
	if err := lane.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if exit := <-child.Done(); exit.Code != 0 {
		t.Fatalf("Exit = %+v, stderr %q", exit, stderr.Bytes())
	}
}

// TestSpawnedSessionDisconnectedFiresMidHandler pins the spawned lane to the
// daemon lane's ordering: the single session's Disconnected closes when the
// handoff channel ends, while a blocked handler is still in flight and before
// Done settles. The child carries the verdict out as its exit code.
func TestSpawnedSessionDisconnectedFiresMidHandler(t *testing.T) {
	child, stderr := spawnBusinessChild(t, "serve-spawned-disconnect")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lane, err := child.Business(ctx, Contract{Schema: spawnedSchema})
	if err != nil {
		t.Fatalf("Business() = %v (stderr %q)", err, stderr.Bytes())
	}
	go func() { _, _ = lane.Call(ctx, spawnedHoldOp, nil) }()
	awaitCapture(t, stderr, "child: holding")

	severCtx, severCancel := context.WithDeadline(context.Background(), time.Now())
	defer severCancel()
	_ = lane.Close(severCtx)

	select {
	case exit := <-child.Done():
		if exit.Code != 0 {
			t.Fatalf("Exit = %+v, stderr %q", exit, stderr.Bytes())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the child did not exit after the handoff channel was severed")
	}
}

func awaitCapture(t *testing.T, capture *Capture, marker string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(string(capture.Bytes()), marker) {
		if time.Now().After(deadline) {
			t.Fatalf("child never wrote %q (stderr %q)", marker, capture.Bytes())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestServeSpawnedIsSingleTakeAfterClaimHandoff is D8(ii)'s child side; the
// ServeSpawned-first order rides TestChildBusinessRoundTripsOverTheSpawnedHandoff's
// spawnedClaimOp, where the claim runs behind a session ServeSpawned already owns.
func TestServeSpawnedIsSingleTakeAfterClaimHandoff(t *testing.T) {
	child, stderr := spawnBusinessChild(t, "claim-then-serve")
	conn, err := child.Conn()
	if err != nil {
		t.Fatalf("Conn() = %v", err)
	}
	defer conn.Close()
	if line := readLine(t, conn); line != "serve-after-claim-refused" {
		t.Fatalf("child reported %q (stderr %q)", line, stderr.Bytes())
	}
	<-child.Done()
}

func TestServeSpawnedRefusesLimitsTheSpawnDidNotConvey(t *testing.T) {
	child, stderr := spawnBusinessChild(t, "serve-spawned-skew")
	exit := <-child.Done()
	if exit.Code != 68 {
		t.Fatalf("Exit = %+v, want the contract refusal 68 (stderr %q)", exit, stderr.Bytes())
	}
	want := fmt.Sprintf("Contract.MaxFrame %d disagrees with the %d the spawn conveyed", 2<<20, spawnedLimits.MaxFrame)
	if !strings.Contains(string(stderr.Bytes()), want) {
		t.Fatalf("child stderr %q, want a refusal naming %q", stderr.Bytes(), want)
	}
}

func TestContractAdoptsOrRefusesTheConveyedLimits(t *testing.T) {
	conveyed := Limits{MaxFrame: 1 << 20, Concurrency: 4}
	tests := []struct {
		name     string
		contract Contract
		want     Limits
		wantErr  string
	}{
		{"zero adopts", Contract{Schema: spawnedSchema}, conveyed, ""},
		{"equal agrees", Contract{Schema: spawnedSchema, MaxFrame: 1 << 20, Concurrency: 4}, conveyed, ""},
		{
			"frame disagrees",
			Contract{Schema: spawnedSchema, MaxFrame: 2 << 20},
			Limits{},
			"Contract.MaxFrame 2097152 disagrees with the 1048576 the spawn conveyed",
		},
		{
			"concurrency disagrees",
			Contract{Schema: spawnedSchema, Concurrency: 9},
			Limits{},
			"Contract.Concurrency 9 disagrees with the 4 the spawn conveyed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.contract.adopt(conveyed)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("adopt() = %v", err)
				}
			} else if err == nil || err.Error() != "daemonkit: "+tt.wantErr {
				t.Fatalf("adopt() = %v, want %q", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("adopt() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestChildBusinessRefusals is directive D8's three named refusals, each
// carrying its own message: a child spawned off the handoff channel names
// ChannelHandoff and the channel it got, the one channel end taken twice names
// both verbs, and a channel outside the established set names the set. The
// Business-first order of the second is
// TestChildBusinessRoundTripsOverTheSpawnedHandoff's Conn() after Business().
func TestChildBusinessRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owned := ownedScope(t)
	spawn := Cmd{Path: "/bin/cat", Exec: ServingSameUser()}

	_, unestablished := owned.Spawn(bounded(t, 30*time.Second), spawn, channelLimit, nil)
	if unestablished == nil {
		t.Fatal("Spawn() accepted a channel outside the established set")
	}
	wantChannel := fmt.Sprintf(
		"daemonkit: channel %d is not one of ChannelNone, ChannelHandoff, ChannelStdio", channelLimit,
	)
	if unestablished.Error() != wantChannel {
		t.Fatalf("Spawn(channelLimit) = %v, want %q", unestablished, wantChannel)
	}

	stdio, err := owned.Spawn(bounded(t, 30*time.Second), spawn, ChannelStdio, nil)
	if err != nil {
		t.Fatalf("Spawn(ChannelStdio) = %v", err)
	}
	_, offChannel := stdio.Business(ctx, Contract{Schema: spawnedSchema})
	if offChannel == nil {
		t.Fatal("Business() attached to a child spawned off the handoff channel")
	}
	wantOffChannel := fmt.Sprintf(
		"daemonkit: Child.Business rides the ChannelHandoff socketpair; this child was spawned on channel %d",
		ChannelStdio,
	)
	if offChannel.Error() != wantOffChannel {
		t.Fatalf("Business() = %v, want %q", offChannel, wantOffChannel)
	}

	held, err := owned.Spawn(bounded(t, 30*time.Second), spawn, ChannelHandoff, nil)
	if err != nil {
		t.Fatalf("Spawn(ChannelHandoff) = %v", err)
	}
	conn, err := held.Conn()
	if err != nil {
		t.Fatalf("Conn() = %v", err)
	}
	defer conn.Close()
	_, collision := held.Business(ctx, Contract{Schema: spawnedSchema})
	if collision == nil {
		t.Fatal("Business() took a channel end Conn already holds")
	}
	if !strings.Contains(collision.Error(), takeCollision) {
		t.Fatalf("Business() after Conn() = %v, want the single-take refusal", collision)
	}

	distinct := map[string]bool{
		unestablished.Error(): true,
		offChannel.Error():    true,
		collision.Error():     true,
	}
	if len(distinct) != 3 {
		t.Fatalf("D8's three refusals produced %d distinct messages", len(distinct))
	}
}

// TestChildBusinessIsTerminalOnItsSingleSession is single-session terminality
// on the spawned lane: the one socketpair end the spawn handed over is the only
// session there will ever be, so its terminal failure closes the lane and every
// later Call is ErrLaneClosed rather than a second attach.
func TestChildBusinessIsTerminalOnItsSingleSession(t *testing.T) {
	child, stderr := spawnBusinessChild(t, "serve-spawned")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lane, err := child.Business(ctx, Contract{Schema: spawnedSchema})
	if err != nil {
		t.Fatalf("Business() = %v (stderr %q)", err, stderr.Bytes())
	}
	if _, err := lane.Call(ctx, spawnedEchoOp, []byte("first")); err != nil {
		t.Fatalf("Call() = %v (stderr %q)", err, stderr.Bytes())
	}
	if _, err := child.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if _, err := lane.Call(ctx, spawnedEchoOp, []byte("second")); err == nil {
		t.Fatal("Call() over a session whose server exited succeeded")
	}
	_, closed := lane.Call(ctx, spawnedEchoOp, []byte("third"))
	if !errors.Is(closed, ErrLaneClosed) {
		t.Fatalf("Call() = %v, want ErrLaneClosed", closed)
	}
	if !Undispatched(closed) {
		t.Error("Undispatched() = false for a lane that refused before writing")
	}
}
