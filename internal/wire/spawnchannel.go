package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"golang.org/x/sys/unix"
)

// SpawnChannelMaxPayload caps one spawn-channel frame's JSON payload.
const SpawnChannelMaxPayload = 1024

// ErrSpawnChannelFrame reports a spawn-channel frame outside the protocol:
// an oversize or empty payload, or a descriptor where none is allowed.
var ErrSpawnChannelFrame = errors.New("wire: invalid spawn channel frame")

const (
	// SpawnChannelOpMint asks the parent for a fresh minted connection.
	SpawnChannelOpMint = "mint"
	// SpawnChannelOpAdopt delivers a connected descriptor for full admission.
	SpawnChannelOpAdopt = "adopt"
)

// SpawnChannelRequest is one child request on the spawn channel, echoing the
// spawn nonce as proof the requester holds the exec's conveyance.
type SpawnChannelRequest struct {
	Op    string `json:"op"`
	Nonce []byte `json:"nonce"`
}

// SpawnChannelMinted answers a mint with the fresh attach nonce; the minted
// descriptor rides the frame's SCM_RIGHTS.
type SpawnChannelMinted struct {
	Nonce []byte `json:"nonce"`
}

// SpawnChannelAdopted answers an adopt with the admission outcome.
type SpawnChannelAdopted struct {
	Adopted bool   `json:"adopted"`
	Reason  string `json:"reason,omitempty"`
}

// DecodeSpawnChannelRequest decodes and validates one child request.
func DecodeSpawnChannelRequest(payload []byte) (SpawnChannelRequest, error) {
	var request SpawnChannelRequest
	if err := decodeStrict(payload, &request); err != nil {
		return SpawnChannelRequest{}, fmt.Errorf("%w: request: %w", ErrSpawnChannelFrame, err)
	}
	if request.Op != SpawnChannelOpMint && request.Op != SpawnChannelOpAdopt {
		return SpawnChannelRequest{}, fmt.Errorf("%w: op %q", ErrSpawnChannelFrame, request.Op)
	}
	if len(request.Nonce) != spawnedNonceBytes {
		return SpawnChannelRequest{}, fmt.Errorf("%w: nonce length %d", ErrSpawnChannelFrame, len(request.Nonce))
	}
	return request, nil
}

// ReadSpawnChannelFrame reads one length-prefixed spawn-channel frame. At most
// one SCM_RIGHTS descriptor may ride the frame's first byte, legal only when
// withRights says so, returned CLOEXEC; fd is -1 when the frame carried none.
// A clean close between frames returns io.EOF.
func ReadSpawnChannelFrame(conn *net.UnixConn, withRights bool) (payload []byte, fd int, err error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, -1, fmt.Errorf("wire: raw spawn channel: %w", err)
	}
	var received []int
	fail := func(err error) ([]byte, int, error) {
		closeDescriptors(received)
		return nil, -1, err
	}
	var prefix [4]byte
	if err := recvmsgFull(raw, prefix[:], withRights, &received); err != nil {
		if errors.Is(err, io.EOF) && len(received) == 0 {
			return nil, -1, io.EOF
		}
		return fail(fmt.Errorf("wire: read spawn channel prefix: %w", err))
	}
	length := int(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 || length > SpawnChannelMaxPayload {
		return fail(fmt.Errorf("%w: payload length %d", ErrSpawnChannelFrame, length))
	}
	payload = make([]byte, length)
	if err := recvmsgFull(raw, payload, false, &received); err != nil {
		return fail(fmt.Errorf("wire: read spawn channel payload: %w", err))
	}
	if len(received) > 1 || (!withRights && len(received) != 0) {
		return fail(fmt.Errorf("%w: unexpected descriptors", ErrSpawnChannelFrame))
	}
	fd = -1
	if len(received) == 1 {
		fd = received[0]
	}
	return payload, fd, nil
}

// WriteSpawnChannelFrame writes one length-prefixed spawn-channel frame,
// attaching fd via SCM_RIGHTS on the frame's first byte when fd is
// non-negative. The descriptor stays owned by the caller.
func WriteSpawnChannelFrame(conn *net.UnixConn, payload []byte, fd int) error {
	if len(payload) == 0 || len(payload) > SpawnChannelMaxPayload {
		return fmt.Errorf("%w: payload length %d", ErrSpawnChannelFrame, len(payload))
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload))) //nolint:gosec // bounded by SpawnChannelMaxPayload
	copy(frame[4:], payload)
	var rights []byte
	if fd >= 0 {
		rights = unix.UnixRights(fd)
	}
	n, _, err := conn.WriteMsgUnix(frame, rights, nil)
	if err != nil {
		return fmt.Errorf("wire: write spawn channel frame: %w", err)
	}
	for n < len(frame) {
		written, err := conn.Write(frame[n:])
		if err != nil {
			return fmt.Errorf("wire: write spawn channel frame: %w", err)
		}
		n += written
	}
	return nil
}
