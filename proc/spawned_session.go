package proc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/spawnedsession"
	"golang.org/x/sys/unix"
)

const (
	spawnedSessionFD               = 5
	spawnedSessionBootstrapVersion = 1
	spawnedSessionBootstrapLimit   = 16 << 10
	spawnedSessionBootstrapTimeout = 10 * time.Second
)

var (
	// ErrSpawnedSessionUnavailable means the request has no claimable session.
	ErrSpawnedSessionUnavailable = errors.New("proc: spawned session is unavailable")
	// ErrSpawnedSessionClaimed means the one-shot session authority was consumed.
	ErrSpawnedSessionClaimed = errors.New("proc: spawned session is already claimed")
	// ErrSpawnedSessionIdentity means the inherited endpoint or bootstrap is foreign.
	ErrSpawnedSessionIdentity = errors.New("proc: spawned session identity mismatch")

	spawnedSessionRandom = rand.Read
	spawnedIdentityClaim struct {
		sync.Mutex
		used bool
	}
)

type spawnedSessionProcess struct {
	PID        int    `json:"pid"`
	UID        int    `json:"uid"`
	StartTime  string `json:"start_time"`
	Boot       string `json:"boot"`
	Comm       string `json:"comm"`
	Executable string `json:"executable"`
}

type spawnedSessionBootstrap struct {
	Version            int                   `json:"version"`
	Nonce              []byte                `json:"nonce"`
	ReceiptDigest      []byte                `json:"receipt_digest"`
	RequestDigest      []byte                `json:"request_digest"`
	Signature          []byte                `json:"signature"`
	OwnerGeneration    OwnerGeneration       `json:"owner_generation"`
	ExpectedExecutable string                `json:"expected_executable"`
	Child              spawnedSessionProcess `json:"child"`
	Parent             spawnedSessionProcess `json:"parent"`
}

type spawnedSessionAck struct {
	Version       int    `json:"version"`
	Nonce         []byte `json:"nonce"`
	ReceiptDigest []byte `json:"receipt_digest"`
}

type spawnedSessionParent struct {
	mu            sync.Mutex
	file          *os.File
	owned         *spawnedsession.OnceConn
	claimed       bool
	endpointTaken bool
	nonce         [sha256.Size]byte
	receiptDigest [sha256.Size]byte
	peer          spawnedSessionProcess
}

// SpawnedSessionEndpoint is one opaque, receipt-bound parent session endpoint.
type SpawnedSessionEndpoint struct{ state *spawnedSessionParent }

type spawnedSessionChild struct {
	owned         *spawnedsession.OnceConn
	nonce         [sha256.Size]byte
	receiptDigest [sha256.Size]byte
	peer          spawnedSessionProcess
}

// SpawnedSessionIdentity is one opaque inherited child session identity.
type SpawnedSessionIdentity struct{ state *spawnedSessionChild }

func newSpawnedSessionFiles() (*os.File, *os.File, error) {
	syscall.ForkLock.Lock()
	defer syscall.ForkLock.Unlock()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("proc: create spawned session socketpair: %w", err)
	}
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
			_ = unix.Close(fds[0])
			_ = unix.Close(fds[1])
			return nil, nil, fmt.Errorf("proc: secure spawned session socketpair: %w", err)
		}
	}
	parent := os.NewFile(uintptr(fds[0]), "daemonkit-spawned-parent")
	child := os.NewFile(uintptr(fds[1]), "daemonkit-spawned-child")
	if parent == nil || child == nil {
		if parent != nil {
			_ = parent.Close()
		} else {
			_ = unix.Close(fds[0])
		}
		if child != nil {
			_ = child.Close()
		} else {
			_ = unix.Close(fds[1])
		}
		return nil, nil, errors.New("proc: wrap spawned session socketpair")
	}
	return parent, child, nil
}

func newSpawnedSessionParent(file *os.File) *spawnedSessionParent {
	if file == nil {
		return nil
	}
	return &spawnedSessionParent{file: file}
}

func (s *spawnedSessionParent) claim(
	ctx context.Context,
	receipt ProcessReceipt,
	parent Identity,
) (SpawnedSessionEndpoint, error) {
	if !receipt.Prepared() || !receipt.spawnedSession || !receipt.hasSignature ||
		receipt.signature == (SignatureDigest{}) || receipt.generation == (OwnerGeneration{}) ||
		receipt.executable == "" || receipt.process.PID <= 0 ||
		receipt.requestDigest == (SpawnRequestDigest{}) {
		return SpawnedSessionEndpoint{}, ErrSpawnedSessionIdentity
	}
	s.mu.Lock()
	if s.claimed || s.file == nil {
		s.mu.Unlock()
		return SpawnedSessionEndpoint{}, ErrSpawnedSessionClaimed
	}
	s.claimed = true
	file := s.file
	s.file = nil
	s.mu.Unlock()

	conn, err := net.FileConn(file)
	closeErr := file.Close()
	if err != nil {
		_ = s.close()
		return SpawnedSessionEndpoint{}, errors.Join(
			fmt.Errorf("proc: open spawned session endpoint: %w", err), closeErr,
		)
	}
	if closeErr != nil {
		_ = conn.Close()
		return SpawnedSessionEndpoint{}, fmt.Errorf("proc: close spawned session descriptor: %w", closeErr)
	}
	owned := &spawnedsession.OnceConn{Conn: conn}
	s.mu.Lock()
	s.owned = owned
	s.mu.Unlock()

	var nonce [sha256.Size]byte
	written, err := spawnedSessionRandom(nonce[:])
	if err != nil || written != len(nonce) || nonce == ([sha256.Size]byte{}) {
		_ = s.close()
		if err == nil {
			err = errors.New("short or zero entropy")
		}
		return SpawnedSessionEndpoint{}, errors.Join(
			ErrSpawnedSessionIdentity,
			fmt.Errorf("proc: generate spawned session nonce: wrote %d: %w", written, err),
		)
	}
	parentProcess := spawnedSessionProcess{
		PID: parent.PID, UID: os.Geteuid(), StartTime: parent.StartTime,
		Boot: parent.Boot, Comm: parent.Comm, Executable: parent.Executable,
	}
	receiptDigest := digestSpawnedSessionReceipt(receipt, parentProcess)
	bootstrap := spawnedSessionBootstrap{
		Version: spawnedSessionBootstrapVersion,
		Nonce:   append([]byte(nil), nonce[:]...), ReceiptDigest: append([]byte(nil), receiptDigest[:]...),
		RequestDigest:   append([]byte(nil), receipt.requestDigest[:]...),
		Signature:       append([]byte(nil), receipt.signature[:]...),
		OwnerGeneration: receipt.generation, ExpectedExecutable: receipt.executable,
		Child: spawnedSessionProcess{
			PID: receipt.process.PID, UID: os.Geteuid(), StartTime: receipt.process.StartTime,
			Boot: receipt.process.Boot, Comm: receipt.process.Comm, Executable: receipt.executable,
		},
		Parent: parentProcess,
	}
	if err := writeSpawnedSessionObject(ctx, conn, bootstrap); err != nil {
		_ = s.close()
		return SpawnedSessionEndpoint{}, fmt.Errorf("proc: write spawned session bootstrap: %w", err)
	}
	s.mu.Lock()
	s.nonce = nonce
	s.receiptDigest = receiptDigest
	s.peer = bootstrap.Child
	s.mu.Unlock()
	return SpawnedSessionEndpoint{state: s}, nil
}

func digestSpawnedSessionReceipt(receipt ProcessReceipt, parent spawnedSessionProcess) [sha256.Size]byte {
	h := sha256.New()
	writeSpawnDigestBytes(h, []byte("daemonkit.proc.spawned-session-receipt.v1"))
	writeSpawnDigestBytes(h, []byte(receipt.executable))
	writeSpawnDigestBytes(h, receipt.requestDigest[:])
	writeSpawnDigestBytes(h, receipt.generation[:])
	writeSpawnDigestBytes(h, []byte(receipt.process.StartTime))
	writeSpawnDigestBytes(h, []byte(receipt.process.Boot))
	writeSpawnDigestBytes(h, []byte(strconv.Itoa(receipt.process.PID)))
	writeSpawnDigestBytes(h, receipt.signature[:])
	writeSpawnDigestBytes(h, []byte{boolByte(receipt.hasSignature), boolByte(receipt.spawnedSession)})
	writeSpawnDigestBytes(h, []byte(strconv.Itoa(parent.PID)))
	writeSpawnDigestBytes(h, []byte(strconv.Itoa(parent.UID)))
	writeSpawnDigestBytes(h, []byte(parent.StartTime))
	writeSpawnDigestBytes(h, []byte(parent.Boot))
	writeSpawnDigestBytes(h, []byte(parent.Comm))
	writeSpawnDigestBytes(h, []byte(parent.Executable))
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func (s *spawnedSessionParent) open(
	ctx context.Context,
	authority spawnedsession.Authority,
) (spawnedsession.Opened, error) {
	if !authority.Valid() {
		return spawnedsession.Opened{}, ErrSpawnedSessionUnavailable
	}
	s.mu.Lock()
	if s.endpointTaken || s.owned == nil || s.nonce == ([sha256.Size]byte{}) {
		s.mu.Unlock()
		return spawnedsession.Opened{}, ErrSpawnedSessionClaimed
	}
	s.endpointTaken = true
	owned := s.owned
	nonce := s.nonce
	receiptDigest := s.receiptDigest
	peer := s.peer
	s.mu.Unlock()
	conn, ok := owned.Claim()
	if !ok {
		return spawnedsession.Opened{}, ErrSpawnedSessionClaimed
	}
	var ack spawnedSessionAck
	if err := readSpawnedSessionObject(ctx, conn, &ack); err != nil {
		_ = s.close()
		return spawnedsession.Opened{}, fmt.Errorf("proc: read spawned session acknowledgement: %w", err)
	}
	if ack.Version != spawnedSessionBootstrapVersion ||
		len(ack.Nonce) != len(nonce) || len(ack.ReceiptDigest) != len(receiptDigest) ||
		subtle.ConstantTimeCompare(ack.Nonce, nonce[:]) != 1 ||
		subtle.ConstantTimeCompare(ack.ReceiptDigest, receiptDigest[:]) != 1 {
		_ = s.close()
		return spawnedsession.Opened{}, ErrSpawnedSessionIdentity
	}
	if err := clearSpawnedSessionDeadline(conn); err != nil {
		_ = s.close()
		return spawnedsession.Opened{}, err
	}
	return spawnedsession.Opened{
		Conn: conn, Peer: spawnedSessionInternalProcess(peer),
		Nonce: nonce, ReceiptDigest: receiptDigest,
	}, nil
}

// OpenForWire consumes the endpoint only for daemonkit's sealed wire API.
func (e SpawnedSessionEndpoint) OpenForWire(
	ctx context.Context,
	authority spawnedsession.Authority,
) (spawnedsession.Opened, error) {
	if e.state == nil {
		return spawnedsession.Opened{}, ErrSpawnedSessionUnavailable
	}
	return e.state.open(ctx, authority)
}

func (s *spawnedSessionParent) close() error {
	s.mu.Lock()
	file := s.file
	s.file = nil
	owned := s.owned
	s.mu.Unlock()
	var err error
	if file != nil {
		err = file.Close()
	}
	if owned != nil {
		err = errors.Join(err, owned.Close())
	}
	return err
}

// ClaimSpawnedSessionIdentity consumes the fixed inherited spawned-session endpoint.
func ClaimSpawnedSessionIdentity(ctx context.Context) (SpawnedSessionIdentity, error) {
	spawnedIdentityClaim.Lock()
	defer spawnedIdentityClaim.Unlock()
	if spawnedIdentityClaim.used {
		return SpawnedSessionIdentity{}, ErrSpawnedSessionClaimed
	}
	identity, err := claimSpawnedSessionIdentity(ctx, spawnedSessionFD)
	if err != nil {
		return SpawnedSessionIdentity{}, err
	}
	spawnedIdentityClaim.used = true
	return identity, nil
}

func claimSpawnedSessionIdentity(ctx context.Context, fd int) (SpawnedSessionIdentity, error) {
	parent, err := spawnedSessionProcessIdentity(os.Getppid())
	if err != nil {
		return SpawnedSessionIdentity{}, errors.Join(ErrSpawnedSessionIdentity, err)
	}
	return claimSpawnedSessionIdentityForParent(ctx, fd, parent)
}

func claimSpawnedSessionIdentityForParent(
	ctx context.Context,
	fd int,
	parent spawnedSessionProcess,
) (SpawnedSessionIdentity, error) {
	duplicateFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return SpawnedSessionIdentity{}, errors.Join(ErrSpawnedSessionIdentity, err)
	}
	file := os.NewFile(uintptr(duplicateFD), "daemonkit-spawned-session-candidate")
	if file == nil {
		_ = unix.Close(duplicateFD)
		return SpawnedSessionIdentity{}, ErrSpawnedSessionIdentity
	}
	defer file.Close()
	kind, err := unix.GetsockoptInt(duplicateFD, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil || kind != unix.SOCK_STREAM {
		return SpawnedSessionIdentity{}, errors.Join(ErrSpawnedSessionIdentity, err)
	}
	address, err := unix.Getsockname(duplicateFD)
	if err != nil {
		return SpawnedSessionIdentity{}, errors.Join(ErrSpawnedSessionIdentity, err)
	}
	if _, ok := address.(*unix.SockaddrUnix); !ok {
		return SpawnedSessionIdentity{}, ErrSpawnedSessionIdentity
	}
	peerAddress, err := unix.Getpeername(duplicateFD)
	if err != nil {
		return SpawnedSessionIdentity{}, errors.Join(ErrSpawnedSessionIdentity, err)
	}
	if _, ok := peerAddress.(*unix.SockaddrUnix); !ok {
		return SpawnedSessionIdentity{}, ErrSpawnedSessionIdentity
	}
	peer, err := spawnedSessionPeerCredentials(duplicateFD)
	if err != nil || peer.PID != parent.PID || peer.UID != parent.UID {
		return SpawnedSessionIdentity{}, errors.Join(ErrSpawnedSessionIdentity, err)
	}
	conn, err := net.FileConn(file)
	if err != nil {
		return SpawnedSessionIdentity{}, fmt.Errorf("proc: open inherited spawned session duplicate: %w", err)
	}
	owned := &spawnedsession.OnceConn{Conn: conn}
	fail := func(err error) (SpawnedSessionIdentity, error) {
		return SpawnedSessionIdentity{}, errors.Join(err, owned.Close())
	}
	var bootstrap spawnedSessionBootstrap
	if err := readSpawnedSessionObject(ctx, conn, &bootstrap); err != nil {
		return fail(fmt.Errorf("proc: read spawned session bootstrap: %w", err))
	}
	self, err := spawnedSessionProcessIdentity(os.Getpid())
	if err != nil {
		return fail(fmt.Errorf("proc: probe spawned session process: %w", err))
	}
	if err := validateSpawnedSessionBootstrap(bootstrap, self, parent); err != nil {
		return fail(err)
	}
	var nonce, receiptDigest [sha256.Size]byte
	copy(nonce[:], bootstrap.Nonce)
	copy(receiptDigest[:], bootstrap.ReceiptDigest)
	ack := spawnedSessionAck{
		Version:       spawnedSessionBootstrapVersion,
		Nonce:         append([]byte(nil), nonce[:]...),
		ReceiptDigest: append([]byte(nil), receiptDigest[:]...),
	}
	if err := writeSpawnedSessionObject(ctx, conn, ack); err != nil {
		return fail(fmt.Errorf("proc: write spawned session acknowledgement: %w", err))
	}
	if err := clearSpawnedSessionDeadline(conn); err != nil {
		return fail(err)
	}
	unix.CloseOnExec(fd)
	if err := unix.Close(fd); err != nil {
		return fail(fmt.Errorf("proc: close inherited spawned session descriptor: %w", err))
	}
	return SpawnedSessionIdentity{state: &spawnedSessionChild{
		owned: owned, nonce: nonce, receiptDigest: receiptDigest, peer: bootstrap.Parent,
	}}, nil
}

func spawnedCurrentIdentity() (Identity, error) {
	identity, err := Probe(os.Getpid())
	if err != nil {
		return Identity{}, err
	}
	identity.Executable, err = ExecutablePath(identity.PID)
	if err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func spawnedSessionProcessIdentity(pid int) (spawnedSessionProcess, error) {
	identity, err := Probe(pid)
	if err != nil {
		return spawnedSessionProcess{}, err
	}
	identity.Executable, err = ExecutablePath(identity.PID)
	if err != nil {
		return spawnedSessionProcess{}, err
	}
	return spawnedSessionProcess{
		PID: identity.PID, UID: os.Geteuid(), StartTime: identity.StartTime,
		Boot: identity.Boot, Comm: identity.Comm, Executable: identity.Executable,
	}, nil
}

func validateSpawnedSessionBootstrap(
	bootstrap spawnedSessionBootstrap,
	self spawnedSessionProcess,
	parent spawnedSessionProcess,
) error {
	if bootstrap.Version != spawnedSessionBootstrapVersion ||
		len(bootstrap.Nonce) != sha256.Size || len(bootstrap.ReceiptDigest) != sha256.Size ||
		len(bootstrap.RequestDigest) != sha256.Size || len(bootstrap.Signature) != sha256.Size ||
		bytes.Equal(bootstrap.Nonce, make([]byte, sha256.Size)) ||
		bytes.Equal(bootstrap.ReceiptDigest, make([]byte, sha256.Size)) ||
		bytes.Equal(bootstrap.RequestDigest, make([]byte, sha256.Size)) ||
		bytes.Equal(bootstrap.Signature, make([]byte, sha256.Size)) ||
		bootstrap.OwnerGeneration == (OwnerGeneration{}) || bootstrap.ExpectedExecutable == "" {
		return ErrSpawnedSessionIdentity
	}
	if bootstrap.Child.PID != self.PID || bootstrap.Child.UID != self.UID ||
		bootstrap.Child.StartTime != self.StartTime || bootstrap.Child.Boot != self.Boot ||
		bootstrap.Child.Executable != self.Executable || bootstrap.ExpectedExecutable != self.Executable {
		return ErrSpawnedSessionIdentity
	}
	if bootstrap.Parent != parent {
		return ErrSpawnedSessionIdentity
	}
	return nil
}

// OpenForWire consumes the identity only for daemonkit's sealed wire API.
func (i SpawnedSessionIdentity) OpenForWire(
	authority spawnedsession.Authority,
) (spawnedsession.Opened, error) {
	if !authority.Valid() || i.state == nil {
		return spawnedsession.Opened{}, ErrSpawnedSessionUnavailable
	}
	conn, ok := i.state.owned.Claim()
	if !ok {
		return spawnedsession.Opened{}, ErrSpawnedSessionClaimed
	}
	return spawnedsession.Opened{
		Conn: conn, Peer: spawnedSessionInternalProcess(i.state.peer),
		Nonce: i.state.nonce, ReceiptDigest: i.state.receiptDigest,
	}, nil
}

func spawnedSessionInternalProcess(process spawnedSessionProcess) spawnedsession.Process {
	return spawnedsession.Process{
		PID: process.PID, UID: process.UID, StartTime: process.StartTime,
		Boot: process.Boot, Comm: process.Comm, Executable: process.Executable,
	}
}

func writeSpawnedSessionObject(ctx context.Context, conn net.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > spawnedSessionBootstrapLimit {
		return errors.New("proc: spawned session bootstrap exceeds limit")
	}
	if err := conn.SetWriteDeadline(spawnedSessionDeadline(ctx)); err != nil {
		return err
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload))) //nolint:gosec // capped at 16 KiB above.
	if err := writeFull(conn, prefix[:]); err != nil {
		return err
	}
	return writeFull(conn, payload)
}

func readSpawnedSessionObject(ctx context.Context, conn net.Conn, value any) error {
	if err := conn.SetReadDeadline(spawnedSessionDeadline(ctx)); err != nil {
		return err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 || length > spawnedSessionBootstrapLimit {
		return errors.New("proc: invalid spawned session bootstrap length")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("proc: trailing spawned session bootstrap")
	}
	return nil
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func spawnedSessionDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(spawnedSessionBootstrapTimeout)
	if current, ok := ctx.Deadline(); ok && current.Before(deadline) {
		return current
	}
	return deadline
}

func clearSpawnedSessionDeadline(conn net.Conn) error {
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("proc: clear spawned session deadline: %w", err)
	}
	return nil
}
