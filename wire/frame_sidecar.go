package wire

import (
	"errors"
	"net"
)

var errInvalidFrameSidecar = errors.New("wire: invalid frame descriptor sidecar")

type frameSidecar interface {
	close() error
	takeUnixConn() (*net.UnixConn, error)
}

type frameRightsCodec interface {
	readFrame(int) (Frame, frameSidecar, error)
}
