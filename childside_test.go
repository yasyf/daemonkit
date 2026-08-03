package daemonkit

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"golang.org/x/sys/unix"
)

// childRoleEnv names the one job a spawned copy of this test binary performs.
// It is read in init, before TestMain and before any test function exists, so
// a child can never re-enter the suite and re-enter the spawn that made it —
// the exponential re-exec scripts/test.sh backstops with a per-UID process cap.
const childRoleEnv = "DAEMONKIT_TEST_CHILD_ROLE"

func init() {
	role := os.Getenv(childRoleEnv)
	if role == "" {
		return
	}
	os.Exit(runChildRole(role))
}

func runChildRole(role string) int {
	switch role {
	case "claim-handoff":
		return childClaimHandoff()
	case "claim-twice":
		return childClaimTwice()
	case "close-fds":
		return childCloseInheritedFDs()
	case "coprocess":
		return childCoprocess()
	case "serve-spawned":
		return childServeSpawned(Contract{Schema: spawnedSchema})
	case "serve-spawned-skew":
		return childServeSpawned(Contract{Schema: spawnedSchema, MaxFrame: 2 << 20})
	case "serve-spawned-disconnect":
		return childServeSpawnedDisconnect()
	case "claim-then-serve":
		return childClaimThenServe()
	}
	fmt.Fprintf(os.Stderr, "child: unknown role %q\n", role)
	return 64
}

// childClaimHandoff proves the claim and reports, over the claimed channel,
// whether the conveyance was unset behind it.
func childClaimHandoff() int {
	claim, err := claimSpawnedHandoff()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: %v\n", err)
		return 65
	}
	defer claim.conn.Close()
	residue := "unset"
	if _, ok := os.LookupEnv(wire.SpawnedNonceEnv); ok {
		residue = "nonce-remains"
	}
	if _, ok := os.LookupEnv(spawnLimitsEnv); ok {
		residue = "limits-remain"
	}
	if _, err := claimSpawnedHandoff(); !errors.Is(err, errHandoffClaimed) {
		fmt.Fprintf(os.Stderr, "child: a second claim returned %v\n", err)
		return 67
	}
	_, err = fmt.Fprintf(claim.conn, "claimed %s %s\n", residue, conveyance(claim.nonce, claim.limits))
	if err != nil {
		return 66
	}
	return 0
}

// conveyance renders what the exec carried, so the parent can compare the
// values it minted against the ones the child read.
func conveyance(nonce []byte, limits Limits) string {
	return fmt.Sprintf("nonce=%x limits=%d,%d", nonce, limits.MaxFrame, limits.Concurrency)
}

func childClaimTwice() int {
	conn, err := ClaimHandoff()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: first claim: %v\n", err)
		return 65
	}
	defer conn.Close()
	verdict := "second-claim-admitted"
	again, err := ClaimHandoff()
	if err != nil {
		verdict = "second-claim-refused"
	} else {
		_ = again.Close()
	}
	if _, err := fmt.Fprintf(conn, "%s\n", verdict); err != nil {
		return 66
	}
	return 0
}

// childCloseInheritedFDs reports which of its inherited descriptors survive,
// so the parent can see that a non-CLOEXEC lease fd is gone.
func childCloseInheritedFDs() int {
	before := openDescriptors()
	if err := CloseInheritedFDs(); err != nil {
		fmt.Fprintf(os.Stderr, "child: %v\n", err)
		return 65
	}
	fmt.Printf("before=%s after=%s\n", before, openDescriptors())
	return 0
}

func openDescriptors() string {
	var open []string
	for fd := 3; fd < 32; fd++ {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
			open = append(open, strconv.Itoa(fd))
		}
	}
	return strings.Join(open, ",")
}

// childCoprocess is the long-lived co-process shape captain-hook's hook engine
// runs: it answers requests on the handoff channel for its whole life, writes
// its diagnostics to stderr, and leaves when the channel closes.
func childCoprocess() int {
	conn, err := ClaimHandoff()
	if err != nil {
		fmt.Fprintf(os.Stderr, "coprocess: %v\n", err)
		return 65
	}
	defer conn.Close()
	fmt.Fprintln(os.Stderr, "coprocess: ready")
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		request := scanner.Text()
		fmt.Fprintf(os.Stderr, "coprocess: handling %s\n", request)
		if _, err := fmt.Fprintf(conn, "reply:%s\n", request); err != nil {
			return 66
		}
	}
	return 0
}

func childCmd(t *testing.T, role string, extra ...string) Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() = %v", err)
	}
	return Cmd{
		Path: executable,
		Env:  append(append(os.Environ(), childRoleEnv+"="+role), extra...),
		Exec: ServingSameUser(),
	}
}

// TestTheChildRoleBranchFiresBeforeTheSuite is the load-bearing check behind
// every self-exec test here: a copy of this binary carrying the role variable
// performs its role and exits without ever reaching m.Run. A branch that did
// not fire would re-enter the suite and re-enter the spawn.
func TestTheChildRoleBranchFiresBeforeTheSuite(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() = %v", err)
	}
	child := exec.Command(executable, "-test.run", "TestTheChildRoleBranchFiresBeforeTheSuite", "-test.v")
	child.Env = append(os.Environ(), childRoleEnv+"=unknown-role-on-purpose")
	out, err := child.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 64 {
		t.Fatalf("child exited %v with output %q, want the unknown-role exit 64", err, out)
	}
	if strings.Contains(string(out), "=== RUN") {
		t.Fatalf("the child reached the test runner: %q", out)
	}
}

func TestClaimHandoffProvesTheParentAndUnsetsTheConveyance(t *testing.T) {
	owned := ownedScope(t)
	stderr := NewCapture(4 << 10)
	spawn := childCmd(t, "claim-handoff")
	spawn.Limits = Limits{MaxFrame: 1 << 20, Concurrency: 5}
	child, err := owned.Spawn(bounded(t, 30*time.Second), spawn, ChannelHandoff, stderr)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	if len(child.nonce) != spawnNonceBytes {
		t.Fatalf("the parent minted a %d-byte nonce, want %d", len(child.nonce), spawnNonceBytes)
	}
	if child.limits != spawn.Limits {
		t.Fatalf("Child.limits = %+v, want the declaration both ends adopt", child.limits)
	}
	conn, err := child.Conn()
	if err != nil {
		t.Fatalf("Conn() = %v", err)
	}
	defer conn.Close()
	line := readLine(t, conn)
	want := "claimed unset " + conveyance(child.nonce, child.limits)
	if line != want {
		t.Fatalf("child reported %q (stderr %q), want %q", line, stderr.Bytes(), want)
	}
	if exit := <-child.Done(); exit.Code != 0 {
		t.Fatalf("Exit = %+v, stderr %q", exit, stderr.Bytes())
	}
}

// TestClaimHandoffIsSingleTake is D8(ii)'s child-side mirror.
func TestClaimHandoffIsSingleTake(t *testing.T) {
	owned := ownedScope(t)
	stderr := NewCapture(4 << 10)
	child, err := owned.Spawn(bounded(t, 30*time.Second), childCmd(t, "claim-twice"), ChannelHandoff, stderr)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	conn, err := child.Conn()
	if err != nil {
		t.Fatalf("Conn() = %v", err)
	}
	defer conn.Close()
	if line := readLine(t, conn); line != "second-claim-refused" {
		t.Fatalf("child reported %q (stderr %q)", line, stderr.Bytes())
	}
	<-child.Done()
}

func TestCloseInheritedFDsDropsANonCloexecLease(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() = %v", err)
	}
	lease, err := os.CreateTemp(t.TempDir(), "lease")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer lease.Close()

	child := exec.Command(executable)
	child.Env = append(os.Environ(), childRoleEnv+"=close-fds")
	child.ExtraFiles = []*os.File{lease}
	out, err := child.Output()
	if err != nil {
		t.Fatalf("child = %v, output %q", err, out)
	}
	report := strings.TrimSpace(string(out))
	before, after, ok := strings.Cut(report, " after=")
	if !ok {
		t.Fatalf("child reported %q", report)
	}
	before = strings.TrimPrefix(before, "before=")
	if !strings.Contains(before, "3") {
		t.Fatalf("the inherited lease was not at fd 3: %q", report)
	}
	if strings.Contains(after, "3") {
		t.Fatalf("CloseInheritedFDs left the inherited lease pinned: %q", report)
	}
}

func readLine(t *testing.T, conn net.Conn) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var line []byte
	buf := make([]byte, 1)
	for {
		n, err := conn.Read(buf)
		if n == 1 {
			if buf[0] == '\n' {
				return string(line)
			}
			line = append(line, buf[0])
			continue
		}
		if err != nil {
			t.Fatalf("read line (%q so far): %v", line, err)
		}
	}
}
