//go:build !daemonkit_unsigned

package trust

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	peerSubSockEnv = "DAEMONKIT_PEERSUB_SOCK"
	peerSubExecEnv = "DAEMONKIT_PEERSUB_EXEC"
	peerSubArgsEnv = "DAEMONKIT_PEERSUB_ARGS"
)

func TestMain(m *testing.M) {
	if os.Getenv(peerSubSockEnv) != "" {
		runPeerSub()
		os.Exit(70)
	}
	os.Exit(m.Run())
}

func runPeerSub() {
	conn, err := net.Dial("unix", os.Getenv(peerSubSockEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "peersub dial: %v\n", err)
		os.Exit(71)
	}
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		fmt.Fprintf(os.Stderr, "peersub read: %v\n", err)
		os.Exit(72)
	}
	target := os.Getenv(peerSubExecEnv)
	if target == "" {
		os.Exit(0)
	}
	// Go opens every descriptor CLOEXEC, so the connection has to be duplicated
	// to survive into the exec'd image and keep the peer socket open.
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		os.Exit(73)
	}
	var dupErr error
	if err := raw.Control(func(fd uintptr) { _, dupErr = syscall.Dup(int(fd)) }); err != nil || dupErr != nil {
		os.Exit(74)
	}
	argv := append([]string{target}, strings.Fields(os.Getenv(peerSubArgsEnv))...)
	_ = syscall.Exec(target, argv, os.Environ())
	os.Exit(75)
}

func spawnPeerSub(t *testing.T, execTarget, execArgs string) (*net.UnixConn, *exec.Cmd) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dk-sub")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	command := exec.Command(os.Args[0])
	command.Env = append(os.Environ(),
		peerSubSockEnv+"="+sock, peerSubExecEnv+"="+execTarget, peerSubArgsEnv+"="+execArgs)
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})

	type accepted struct {
		conn net.Conn
		err  error
	}
	results := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		results <- accepted{conn, err}
	}()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("accept: %v", result.err)
		}
		t.Cleanup(func() { _ = result.conn.Close() })
		return result.conn.(*net.UnixConn), command
	case <-time.After(5 * time.Second):
		t.Fatal("helper never connected")
		return nil, nil
	}
}

func requireKernelReads(t *testing.T) {
	t.Helper()
	csopsOnce.Do(loadCsops)
	if csopsErr != nil {
		t.Fatalf("csops_audittoken unavailable: %v — it is in libsystem_kernel on every darwin that builds this module, and without it there is no verifier to test", csopsErr)
	}
}

func TestPeerCredentialsBindTheConnectingExecution(t *testing.T) {
	conn, command := spawnPeerSub(t, "", "")
	peer, err := PeerCredentials(conn)
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	if peer.UID != os.Geteuid() {
		t.Errorf("peer UID = %d, want %d", peer.UID, os.Geteuid())
	}
	if peer.Token.PID() != command.Process.Pid {
		t.Errorf("audit token PID = %d, want %d", peer.Token.PID(), command.Process.Pid)
	}
	if !peer.Token.Valid() {
		t.Errorf("audit token %x is not valid", peer.Token)
	}
	if err := Floor(peer.UID); err != nil {
		t.Errorf("Floor(peer) = %v, want nil", err)
	}
}

// The test binary is ad-hoc signed, so it is exactly the peer the posture
// floor exists to refuse.
func TestVerifyDeniesAnAdHocSignedPeer(t *testing.T) {
	requireKernelReads(t)
	conn, _ := spawnPeerSub(t, "", "")
	peer, err := PeerCredentials(conn)
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	err = Verify(peer, &req)
	if err == nil {
		t.Fatal("Verify(ad-hoc peer) = nil, want a denial")
	}
	if !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Verify(ad-hoc peer) = %v, want ErrUntrustedPeer", err)
	}
}

// ErrPeerGone, not ErrUntrustedPeer and not ErrNoVerifier: the execution
// generation the token names ended, which is a per-connection race the caller
// logs at DEBUG rather than as a policy denial.
func TestVerifyReportsPeerGoneForADeadExecutionGeneration(t *testing.T) {
	requireKernelReads(t)
	conn, command := spawnPeerSub(t, "", "")
	peer, err := PeerCredentials(conn)
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill peer: %v", err)
	}
	_ = command.Wait()

	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err = Verify(peer, &req)
		if errors.Is(err, ErrPeerGone) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Verify(dead peer) = %v, want ErrPeerGone", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The token binds {pid, pidversion}: a token carrying the live pid with any
// other pidversion resolves no execution at all.
func TestAuditTokenBindsThePIDVersionNotThePID(t *testing.T) {
	requireKernelReads(t)
	conn, _ := spawnPeerSub(t, "", "")
	peer, err := PeerCredentials(conn)
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	recycled := peer
	recycled.Token[28] ^= 1
	if recycled.Token.PID() != peer.Token.PID() {
		t.Fatalf("flipping the pidversion changed the PID")
	}
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	err = Verify(recycled, &req)
	if !errors.Is(err, ErrPeerGone) {
		t.Fatalf("Verify(wrong pidversion, live pid) = %v, want ErrPeerGone", err)
	}
}

// A named limitation, pinned so it fails loudly if someone "fixes" it: the
// verdict is a query-time property of one execution generation. A peer that
// execs after connecting keeps the connection and becomes a different program;
// the token captured before the exec resolves nothing afterwards.
func TestPeerIdentityIsQueryTimeLive_TF1(t *testing.T) {
	requireKernelReads(t)
	conn, _ := spawnPeerSub(t, "/bin/sleep", "86400")
	before, err := PeerCredentials(conn)
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	identity, err := readSigningString(kernelReads(before.Token), opIdentity)
	if err != nil {
		t.Fatalf("read pre-exec identity: %v", err)
	}
	if identity == "com.apple.sleep" {
		t.Fatal("baseline: the peer is /bin/sleep before the exec")
	}
	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("signal helper: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		after, err := PeerCredentials(conn)
		if err == nil && after.Token.PIDVersion() != before.Token.PIDVersion() {
			identity, err := readSigningString(kernelReads(after.Token), opIdentity)
			if err != nil {
				t.Fatalf("read post-exec identity: %v", err)
			}
			if identity != "com.apple.sleep" {
				t.Fatalf("post-exec identity = %q, want com.apple.sleep", identity)
			}
			if _, err := readSigningString(kernelReads(before.Token), opIdentity); !errors.Is(err, ErrPeerGone) {
				t.Fatalf("pre-exec token after the exec = %v, want ErrPeerGone", err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("peer identity never became /bin/sleep after the exec — substitution not observed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPeerTokenFDDelegation_TF5(t *testing.T) {
	t.Skip("TF5 (fd delegation / SCM_RIGHTS) needs a privileged multi-process harness; documented limitation — Verify never re-judges a delegated descriptor")
}
