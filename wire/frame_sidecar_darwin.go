//go:build darwin

package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const maxSCMRightsDescriptors = 253

type scmRightsCodec struct {
	raw syscall.RawConn
}

type scmRightsSidecar struct {
	file *os.File
}

func newFrameRightsCodec(conn net.Conn) (frameRightsCodec, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, nil
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("wire: raw Unix connection: %w", err)
	}
	if err := validateConnectedUnixStream(raw); err != nil {
		return nil, err
	}
	return &scmRightsCodec{raw: raw}, nil
}

func (c *scmRightsCodec) readFrame(maxFrame int) (frame Frame, sidecar frameSidecar, err error) {
	var received []int
	defer func() {
		if err != nil {
			closeDescriptors(received)
		}
	}()

	var prefix [4]byte
	if err := recvmsgFull(c.raw, prefix[:], true, &received); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, nil, ErrFrameTruncated
		}
		return Frame{}, nil, err
	}
	bodyLength := int(binary.BigEndian.Uint32(prefix[:]))
	limit := maxFrame
	if limit <= 0 {
		limit = DefaultMaxFrame
	}
	if bodyLength > limit {
		return Frame{}, nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, bodyLength, limit)
	}
	if bodyLength < frameHeaderSize {
		return Frame{}, nil, fmt.Errorf("%w: body length %d", ErrInvalidFrame, bodyLength)
	}
	body := make([]byte, bodyLength)
	if err := recvmsgFull(c.raw, body, false, &received); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, nil, ErrFrameTruncated
		}
		return Frame{}, nil, err
	}
	frame, err = decodeFrame(body)
	if err != nil {
		return Frame{}, nil, err
	}

	if frame.Kind == FrameRequest && frame.Op == brokerHandoffOp &&
		frame.Flags == FlagEnd && frame.Tenant == "" {
		if len(received) != 1 {
			return Frame{}, nil, fmt.Errorf("%w: handoff requires exactly one descriptor", errInvalidFrameSidecar)
		}
		if len(frame.Payload) > brokerHandoffMaximumPayloadBytes {
			return Frame{}, nil, fmt.Errorf(
				"%w: handoff payload %d exceeds %d",
				errInvalidFrameSidecar, len(frame.Payload), brokerHandoffMaximumPayloadBytes,
			)
		}
		fd := received[0]
		received = nil
		file := os.NewFile(uintptr(fd), "daemonkit-broker-handoff")
		if file == nil {
			_ = unix.Close(fd)
			return Frame{}, nil, fmt.Errorf("%w: own received descriptor", errInvalidFrameSidecar)
		}
		return frame, &scmRightsSidecar{file: file}, nil
	}
	if len(received) != 0 {
		return Frame{}, nil, fmt.Errorf("%w: descriptor on operation %q", errInvalidFrameSidecar, frame.Op)
	}
	return frame, nil, nil
}

func (s *scmRightsSidecar) close() error {
	if s == nil || s.file == nil {
		return nil
	}
	file := s.file
	s.file = nil
	return file.Close()
}

func (s *scmRightsSidecar) takeUnixConn() (*net.UnixConn, error) {
	if s == nil || s.file == nil {
		return nil, fmt.Errorf("%w: descriptor already consumed", errInvalidFrameSidecar)
	}
	file := s.file
	s.file = nil
	conn, err := net.FileConn(file)
	closeErr := file.Close()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("wire: adopt handoff descriptor: %w", err), closeErr)
	}
	if closeErr != nil {
		_ = conn.Close()
		return nil, closeErr
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: adopted descriptor is %T", errInvalidFrameSidecar, conn)
	}
	return unixConn, nil
}

func recvmsgFull(raw syscall.RawConn, dst []byte, allowRightAtFirstByte bool, received *[]int) error {
	read := 0
	for read < len(dst) {
		receivedBefore := len(*received)
		var n, oobn, flags int
		var recvErr, controlErr error
		oob := make([]byte, unix.CmsgSpace(maxSCMRightsDescriptors*4))
		err := raw.Read(func(fd uintptr) bool {
			syscall.ForkLock.RLock()
			n, oobn, flags, _, recvErr = unix.Recvmsg(int(fd), dst[read:], oob, 0)
			if isWouldBlock(recvErr) {
				syscall.ForkLock.RUnlock()
				return false
			}
			if recvErr == nil && (n != 0 || oobn != 0 || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0) {
				allowRight := n != 0 && allowRightAtFirstByte && read == 0
				controlErr = consumeControl(oob[:oobn], flags, allowRight, received)
			}
			syscall.ForkLock.RUnlock()
			return true
		})
		if err != nil {
			return fmt.Errorf("wire: wait for frame: %w", err)
		}
		if recvErr != nil {
			return fmt.Errorf("wire: receive frame: %w", recvErr)
		}
		if controlErr != nil {
			closeDescriptors(*received)
			*received = nil
			return controlErr
		}
		for _, fd := range (*received)[receivedBefore:] {
			if err := validateReceivedDescriptor(fd); err != nil {
				closeDescriptors((*received)[receivedBefore:])
				*received = (*received)[:receivedBefore]
				return fmt.Errorf("%w: received descriptor: %w", errInvalidFrameSidecar, err)
			}
		}
		if n == 0 {
			if read == 0 {
				return io.EOF
			}
			return io.ErrUnexpectedEOF
		}
		read += n
	}
	return nil
}

func consumeControl(oob []byte, flags int, allowRight bool, received *[]int) error {
	if flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 {
		closeParsedRights(oob, received)
		return fmt.Errorf("%w: truncated ancillary data", errInvalidFrameSidecar)
	}
	if len(oob) == 0 {
		return nil
	}
	messages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return fmt.Errorf("%w: parse ancillary data: %w", errInvalidFrameSidecar, err)
	}
	valid := allowRight && len(*received) == 0 && len(messages) == 1
	for _, message := range messages {
		if message.Header.Level != unix.SOL_SOCKET || message.Header.Type != unix.SCM_RIGHTS {
			valid = false
			continue
		}
		fds, parseErr := unix.ParseUnixRights(&message)
		if parseErr != nil {
			valid = false
			continue
		}
		for _, fd := range fds {
			if err := setCloseOnExec(fd); err != nil {
				_ = unix.Close(fd)
				valid = false
				continue
			}
			*received = append(*received, fd)
		}
		if len(fds) != 1 {
			valid = false
		}
	}
	if !valid {
		return fmt.Errorf("%w: ancillary data must contain one descriptor at frame byte zero", errInvalidFrameSidecar)
	}
	return nil
}

func closeParsedRights(oob []byte, received *[]int) {
	messages, parseErr := unix.ParseSocketControlMessage(oob)
	for _, message := range messages {
		fds, err := unix.ParseUnixRights(&message)
		if err != nil {
			continue
		}
		closeDescriptors(fds)
	}
	if parseErr != nil && len(messages) == 0 {
		closeTruncatedRights(oob)
	}
	closeDescriptors(*received)
	*received = nil
}

func closeTruncatedRights(oob []byte) {
	if len(oob) < unix.SizeofCmsghdr {
		return
	}
	var level, typeID int32
	if _, err := binary.Decode(oob[4:8], binary.NativeEndian, &level); err != nil {
		return
	}
	if _, err := binary.Decode(oob[8:12], binary.NativeEndian, &typeID); err != nil {
		return
	}
	if level != unix.SOL_SOCKET || typeID != unix.SCM_RIGHTS {
		return
	}
	for offset := unix.SizeofCmsghdr; offset+4 <= len(oob); offset += 4 {
		var descriptor int32
		if _, err := binary.Decode(oob[offset:offset+4], binary.NativeEndian, &descriptor); err != nil {
			return
		}
		_ = unix.Close(int(descriptor))
	}
}

func setCloseOnExec(fd int) error {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return err
	}
	_, err = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC)
	return err
}

func closeDescriptors(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

func validateConnectedUnixStream(raw syscall.RawConn) error {
	var validationErr error
	if err := raw.Control(func(fd uintptr) {
		validationErr = validateReceivedDescriptor(int(fd))
	}); err != nil {
		return fmt.Errorf("wire: inspect Unix stream: %w", err)
	}
	if validationErr != nil {
		return fmt.Errorf("wire: connected AF_UNIX SOCK_STREAM required: %w", validationErr)
	}
	return nil
}

func validateReceivedDescriptor(fd int) error {
	socketType, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil {
		return fmt.Errorf("SO_TYPE: %w", err)
	}
	if socketType != unix.SOCK_STREAM {
		return fmt.Errorf("socket type %d", socketType)
	}
	accepting, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
	if err != nil && !errors.Is(err, unix.ENOPROTOOPT) {
		return fmt.Errorf("SO_ACCEPTCONN: %w", err)
	}
	if err == nil && accepting != 0 {
		return errors.New("listening socket")
	}
	local, err := unix.Getsockname(fd)
	if err != nil {
		return fmt.Errorf("getsockname: %w", err)
	}
	if _, ok := local.(*unix.SockaddrUnix); !ok {
		return fmt.Errorf("local family %T", local)
	}
	peer, err := unix.Getpeername(fd)
	if err != nil {
		return fmt.Errorf("getpeername: %w", err)
	}
	if _, ok := peer.(*unix.SockaddrUnix); !ok {
		return fmt.Errorf("peer family %T", peer)
	}
	return nil
}

func isWouldBlock(err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK)
}
