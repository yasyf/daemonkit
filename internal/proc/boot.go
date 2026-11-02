package proc

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

func parseBootSession(raw, source string) (uint64, error) {
	id, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(raw), "-", ""))
	if err != nil || len(id) != 16 {
		return 0, fmt.Errorf("parse %s: %q is not a uuid", source, raw)
	}
	boot := binary.BigEndian.Uint64(id[:8])
	if boot == 0 {
		return 0, fmt.Errorf("parse %s: %q folds to a zero boot stamp", source, raw)
	}
	return boot, nil
}
