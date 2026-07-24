//go:build !darwin

package wire

import "net"

func newFrameRightsCodec(net.Conn) (frameRightsCodec, error) { return nil, nil }
