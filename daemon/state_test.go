package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const testStateFingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type testState struct {
	Value int `json:"value"`
}

func testStateFile(path string) ExactStateFile[testState] {
	return ExactStateFile[testState]{
		Path: path,
		Codec: ExactStateCodec[testState]{
			Identity: "daemonkit.test.state.v1", Fingerprint: testStateFingerprint,
			New: func() (testState, error) { return testState{}, nil },
			Encode: func(state testState) (json.RawMessage, error) {
				return json.Marshal(state)
			},
			Decode: func(raw json.RawMessage) (testState, error) {
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(raw, &fields); err != nil {
					return testState{}, err
				}
				if len(fields) != 1 || fields["value"] == nil {
					return testState{}, errors.New("test payload field set is not exact")
				}
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.DisallowUnknownFields()
				var state testState
				if err := decoder.Decode(&state); err != nil {
					return testState{}, err
				}
				if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
					return testState{}, errors.New("test payload has trailing JSON")
				}
				return state, nil
			},
		},
	}
}

func TestExactStateFileCreatesExactEnvelope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "state.json")
	file := testStateFile(path)
	if err := file.Update(t.Context(), func(state *testState) error {
		state.Value = 42
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"identity":"daemonkit.test.state.v1","schema":1,"fingerprint":"` + testStateFingerprint + `","payload":{"value":42}}`
	if string(data) != want {
		t.Fatalf("state = %s, want %s", data, want)
	}
	state, err := file.Read()
	if err != nil || state.Value != 42 {
		t.Fatalf("Read = %+v, %v; want value 42", state, err)
	}
}

func TestExactStateFileRejectsForeignOrDamagedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	file := testStateFile(path)
	tests := map[string]string{
		"empty":         "",
		"null":          "null",
		"legacy map":    `{"value":1}`,
		"foreign":       `{"identity":"foreign","schema":1,"fingerprint":"` + testStateFingerprint + `","payload":{"value":1}}`,
		"unknown":       `{"identity":"daemonkit.test.state.v1","schema":1,"fingerprint":"` + testStateFingerprint + `","payload":{"value":1},"future":true}`,
		"payload null":  `{"identity":"daemonkit.test.state.v1","schema":1,"fingerprint":"` + testStateFingerprint + `","payload":null}`,
		"payload drift": `{"identity":"daemonkit.test.state.v1","schema":1,"fingerprint":"` + testStateFingerprint + `","payload":{"value":1,"future":true}}`,
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := file.Read(); err == nil {
				t.Fatal("Read accepted foreign or damaged state")
			}
		})
	}
}

func TestExactStateFileRejectsInvalidCodecPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	file := testStateFile(path)
	file.Codec.Encode = func(testState) (json.RawMessage, error) {
		return json.RawMessage("not-json"), nil
	}
	err := file.Update(t.Context(), func(*testState) error { return nil })
	if err == nil {
		t.Fatal("Update accepted invalid codec payload")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("state file written despite invalid payload: %v", statErr)
	}
}

func TestExactStateFileUpdateLockBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	held, err := (proc.FileLockSpec{
		Path: path + ".lock", Mode: proc.FileLockExclusive, Deadline: time.Second,
	}).TryAcquire()
	if err != nil {
		t.Fatalf("pre-hold lock: %v", err)
	}
	defer held.Close()
	err = testStateFile(path).Update(t.Context(), func(*testState) error {
		t.Fatal("mutate ran while lock was held")
		return nil
	})
	if !errors.Is(err, proc.ErrLockBusy) {
		t.Fatalf("Update err = %v, want proc.ErrLockBusy", err)
	}
}

func TestExactStateFileUpdateUnlocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	held, err := (proc.FileLockSpec{
		Path: path + ".lock", Mode: proc.FileLockExclusive, Deadline: time.Second,
	}).TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	file := testStateFile(path)
	if err := file.UpdateUnlocked(func(state *testState) error {
		state.Value = 7
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	state, err := file.Read()
	if err != nil || state.Value != 7 {
		t.Fatalf("Read = %+v, %v; want value 7", state, err)
	}
}

func TestExactStateFileReadErrorPropagates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	err := testStateFile(path).Update(t.Context(), func(*testState) error {
		t.Error("mutate ran on failed read")
		return nil
	})
	if err == nil {
		t.Fatal("Update succeeded on unreadable state")
	}
	if info, statErr := os.Stat(path); statErr != nil || !info.IsDir() {
		t.Fatalf("state path clobbered: %v", statErr)
	}
}

func TestExactStateFileMutateErrorAborts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	boom := errors.New("bad mutation")
	err := testStateFile(path).Update(t.Context(), func(*testState) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Update err = %v, want %v", err, boom)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("state file written despite mutate error: %v", statErr)
	}
}

func TestExactStateFileRejectsInvalidConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	file := testStateFile(path)
	file.Codec.Fingerprint = "ABC"
	if _, err := file.Read(); err == nil {
		t.Fatal("Read accepted invalid schema fingerprint")
	}
}

func TestWriteFileDurableSyncsCreatedDirectoryParents(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "drain", "g1", "owner.json")
	var synced []string
	if err := writeFileDurable(path, []byte("owner"), 0o600, func(path string) error {
		synced = append(synced, path)
		return nil
	}); err != nil {
		t.Fatalf("writeFileDurable: %v", err)
	}
	want := []string{filepath.Dir(root), root, filepath.Join(root, "drain"), filepath.Join(root, "drain", "g1")}
	if fmt.Sprint(synced) != fmt.Sprint(want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
}

func TestMkdirAllDurableRetriesFailedParentSync(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "drain", "g1")
	syncErr := errors.New("sync failed")
	failed := false
	err := mkdirAllDurable(dir, 0o700, func(path string) error {
		if path == root && !failed {
			failed = true
			return syncErr
		}
		return nil
	})
	if !errors.Is(err, syncErr) {
		t.Fatalf("first mkdirAllDurable err = %v, want sync failure", err)
	}
	var synced []string
	if err := mkdirAllDurable(dir, 0o700, func(path string) error {
		synced = append(synced, path)
		return nil
	}); err != nil {
		t.Fatalf("mkdirAllDurable retry: %v", err)
	}
	if len(synced) == 0 || synced[0] != root {
		t.Fatalf("retry synced directories = %v, want %q first", synced, root)
	}
}
