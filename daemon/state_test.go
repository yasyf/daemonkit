package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

// TestStateFilePreservesForeignKeys: an Update that touches one key leaves every
// unrelated key byte-for-byte intact, including a foreign value's internal key
// order (a RawMessage is never re-parsed).
func TestStateFilePreservesForeignKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	// A foreign value carrying HTML-escapable bytes (<, >, &), internal
	// whitespace, and an out-of-order object — every drift json.Marshal of a
	// RawMessage would introduce.
	const foreign = `{"z": 1, "a": "x<y>&z"}`
	if err := os.WriteFile(path, []byte(`{"foreign":`+foreign+`,"pid":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sf := StateFile{Path: path}

	err := sf.Update(context.Background(), func(state map[string]json.RawMessage) error {
		state["pid"] = json.RawMessage("2")
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(foreign)) {
		t.Errorf("foreign value drifted; file = %s, want a verbatim %s", data, foreign)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("re-read state: %v", err)
	}
	if string(got["pid"]) != "2" {
		t.Errorf("pid = %s, want 2", got["pid"])
	}
}

// TestStateFileRejectsInvalidValue: a mutation that stores non-JSON bytes fails
// the write rather than corrupting the file.
func TestStateFileRejectsInvalidValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	sf := StateFile{Path: path}
	err := sf.Update(context.Background(), func(state map[string]json.RawMessage) error {
		state["pid"] = json.RawMessage("not-json")
		return nil
	})
	if err == nil {
		t.Fatal("Update accepted invalid JSON value; want an error")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("state file written despite invalid value: stat err = %v", statErr)
	}
}

// TestStateFileCreatesMissing: Update on a missing file starts from an empty
// object and writes the mutation.
func TestStateFileCreatesMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "state.json")
	sf := StateFile{Path: path}

	err := sf.Update(context.Background(), func(state map[string]json.RawMessage) error {
		state["pid"] = json.RawMessage("42")
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"pid":42}` {
		t.Errorf("state = %s, want {\"pid\":42}", data)
	}
}

// TestStateFileUpdateLockBusy: Update fails with proc.ErrLockBusy while another
// owner holds the state lock, and never writes.
func TestStateFileUpdateLockBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	held, err := proc.TryLock(path + ".lock")
	if err != nil {
		t.Fatalf("pre-hold lock: %v", err)
	}
	defer held.Release()

	sf := StateFile{Path: path}
	err = sf.Update(context.Background(), func(map[string]json.RawMessage) error {
		t.Fatal("mutate ran while the lock was held")
		return nil
	})
	if !errors.Is(err, proc.ErrLockBusy) {
		t.Fatalf("Update err = %v, want proc.ErrLockBusy", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Error("state file written despite a busy lock")
	}
}

// TestStateFileUpdateUnlocked: UpdateUnlocked writes without taking the flock, so
// a caller already inside the critical section (the flock is non-reentrant) can
// still mutate.
func TestStateFileUpdateUnlocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	held, err := proc.TryLock(path + ".lock")
	if err != nil {
		t.Fatalf("hold lock: %v", err)
	}
	defer held.Release()

	sf := StateFile{Path: path}
	err = sf.UpdateUnlocked(func(state map[string]json.RawMessage) error {
		state["v"] = json.RawMessage(`"1.2.3"`)
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateUnlocked while holding the lock: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"v":"1.2.3"}` {
		t.Errorf("state = %s, want {\"v\":\"1.2.3\"}", data)
	}
}

// TestStateFileReadErrorPropagates: a failing read (here the path is a
// directory) aborts Update before mutate ever runs, so a transient read error
// never rewrites the state file from empty.
func TestStateFileReadErrorPropagates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	sf := StateFile{Path: path}
	err := sf.Update(context.Background(), func(map[string]json.RawMessage) error {
		t.Error("mutate ran on a failed read")
		return nil
	})
	if err == nil {
		t.Fatal("Update succeeded on an unreadable state file, want error")
	}
	fi, statErr := os.Stat(path)
	if statErr != nil || !fi.IsDir() {
		t.Errorf("state path clobbered after a read error: %v", statErr)
	}
}

// TestStateFileMutateErrorAborts: a mutate error propagates and nothing is written.
func TestStateFileMutateErrorAborts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	boom := errors.New("bad mutation")
	sf := StateFile{Path: path}

	err := sf.Update(context.Background(), func(map[string]json.RawMessage) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Update err = %v, want the mutate error", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Error("state file written despite a mutate error")
	}
}
