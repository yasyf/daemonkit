package state

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const magic = "dkstate\n"

const (
	schemaAt  = len(magic)
	envLenAt  = schemaAt + 4
	payLenAt  = envLenAt + 8
	headerLen = payLenAt + 8
)

type envelope struct {
	Cores []Core `json:"cores"`
}

func frame(schema Schema, cores []Core, payload []byte) ([]byte, error) {
	env, err := json.Marshal(envelope{Cores: cores})
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	raw := make([]byte, headerLen, headerLen+len(env)+len(payload))
	copy(raw, magic)
	binary.BigEndian.PutUint32(raw[schemaAt:], uint32(schema))
	binary.BigEndian.PutUint64(raw[envLenAt:], uint64(len(env)))
	binary.BigEndian.PutUint64(raw[payLenAt:], uint64(len(payload)))
	raw = append(raw, env...)
	return append(raw, payload...), nil
}

func scan(raw []byte) (Schema, []Core, []byte) {
	if len(raw) < headerLen || string(raw[:len(magic)]) != magic {
		return 0, nil, nil
	}
	schema := Schema(binary.BigEndian.Uint32(raw[schemaAt:]))
	envLen := binary.BigEndian.Uint64(raw[envLenAt:])
	payLen := binary.BigEndian.Uint64(raw[payLenAt:])
	rest := raw[headerLen:]
	if envLen > uint64(len(rest)) {
		return schema, nil, nil
	}
	var env envelope
	if err := json.Unmarshal(rest[:envLen], &env); err != nil {
		return schema, nil, nil
	}
	if rest = rest[envLen:]; payLen != uint64(len(rest)) {
		return schema, env.Cores, nil
	}
	return schema, env.Cores, rest
}
