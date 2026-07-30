package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

const era Schema = 7

type records struct {
	Live []Core `json:"live"`
	Note string `json:"note"`
}

func (r records) Cores() []Core { return r.Live }

func handFrame(schema uint32, envelope, payload string) []byte {
	raw := make([]byte, 28, 28+len(envelope)+len(payload))
	copy(raw, "dkstate\n")
	binary.BigEndian.PutUint32(raw[8:], schema)
	binary.BigEndian.PutUint64(raw[12:], uint64(len(envelope)))
	binary.BigEndian.PutUint64(raw[20:], uint64(len(payload)))
	return append(append(raw, envelope...), payload...)
}

func seed(t *testing.T, raw []byte) (string, *File[records]) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "records.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, New[records](path, era)
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "records.json")
	file := New[records](path, era)
	want := records{Live: []Core{{PID: 41, Start: 9, Boot: 7, Generation: 2}}, Note: "hi"}
	if err := file.Store(want); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	got, err := file.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(got.Value, want) {
		t.Errorf("Value = %#v, want %#v", got.Value, want)
	}
	if !reflect.DeepEqual(got.Cores, want.Live) {
		t.Errorf("Cores = %#v, want %#v", got.Cores, want.Live)
	}
	if got.Archived != "" {
		t.Errorf("Archived = %q, want empty", got.Archived)
	}
}

func TestLoadMissingFileIsFresh(t *testing.T) {
	file := New[records](filepath.Join(t.TempDir(), "records.json"), era)
	got, err := file.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(got, Loaded[records]{}) {
		t.Errorf("Load() = %#v, want zero", got)
	}
}

func TestUnboundFileRefusesEveryOperation(t *testing.T) {
	tests := []struct {
		name string
		file *File[records]
	}{
		{"zero value", &File[records]{}},
		{"nil handle", nil},
		{"empty path", New[records]("", era)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			got, err := tt.file.Load()
			if !errors.Is(err, ErrUnbound) {
				t.Errorf("Load() = %#v, %v; want ErrUnbound", got, err)
			}
			if err := tt.file.Store(records{}); !errors.Is(err, ErrUnbound) {
				t.Errorf("Store() error = %v, want ErrUnbound", err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Errorf("working directory = %v, want nothing written", names(entries))
			}
		})
	}
}

func TestFrameLayoutIsFrozen(t *testing.T) {
	const (
		wantEnvelope = `{"cores":[{"pid":1,"start":2,"boot":3,"generation":4}]}`
		wantPayload  = `{"live":[{"pid":1,"start":2,"boot":3,"generation":4}],"note":"hi"}`
	)
	path := filepath.Join(t.TempDir(), "records.json")
	file := New[records](path, 0x01020304)
	if err := file.Store(records{Live: []Core{{PID: 1, Start: 2, Boot: 3, Generation: 4}}, Note: "hi"}); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw[0:8]); got != "dkstate\n" {
		t.Errorf("magic = %q, want %q", got, "dkstate\n")
	}
	if got := binary.BigEndian.Uint32(raw[8:12]); got != 0x01020304 {
		t.Errorf("schema = %#x, want %#x", got, 0x01020304)
	}
	if got := binary.BigEndian.Uint64(raw[12:20]); got != uint64(len(wantEnvelope)) {
		t.Errorf("envelope length = %d, want %d", got, len(wantEnvelope))
	}
	if got := binary.BigEndian.Uint64(raw[20:28]); got != uint64(len(wantPayload)) {
		t.Errorf("payload length = %d, want %d", got, len(wantPayload))
	}
	if got := string(raw[28 : 28+len(wantEnvelope)]); got != wantEnvelope {
		t.Errorf("envelope = %s, want %s", got, wantEnvelope)
	}
	if got := string(raw[28+len(wantEnvelope):]); got != wantPayload {
		t.Errorf("payload = %s, want %s", got, wantPayload)
	}
}

func TestSessionCoreIsFrozenInTheEnvelope(t *testing.T) {
	const wantEnvelope = `{"cores":[{"pid":1,"start":2,"boot":3,"generation":4,"session":1}]}`
	path := filepath.Join(t.TempDir(), "records.json")
	file := New[records](path, era)
	if err := file.Store(records{Live: []Core{{PID: 1, Start: 2, Boot: 3, Generation: 4, Session: 1}}}); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw[28 : 28+len(wantEnvelope)]); got != wantEnvelope {
		t.Errorf("envelope = %s, want %s", got, wantEnvelope)
	}

	_, future := seed(t, handFrame(uint32(era)+1, wantEnvelope, "not this era's payload"))
	got, err := future.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []Core{{PID: 1, Start: 2, Boot: 3, Generation: 4, Session: 1}}
	if !reflect.DeepEqual(got.Cores, want) {
		t.Errorf("Cores = %#v, want %#v", got.Cores, want)
	}
}

func TestLoadArchivesAsideAndYieldsCores(t *testing.T) {
	const (
		envelope = `{"cores":[{"pid":11,"start":22,"boot":33,"generation":44}],"era":"unknown to this build"}`
		payload  = `{"live":[{"pid":11,"start":22,"boot":33,"generation":44}],"note":"x"}`
	)
	recorded := []Core{{PID: 11, Start: 22, Boot: 33, Generation: 44}}
	whole := handFrame(uint32(era), envelope, payload)

	tests := []struct {
		name  string
		raw   []byte
		cores []Core
	}{
		{"schema from the future", handFrame(uint32(era)+1, envelope, payload), recorded},
		{"schema from the past", handFrame(uint32(era)-1, envelope, payload), recorded},
		{"payload is garbage", handFrame(uint32(era), envelope, "not json at all"), recorded},
		{"payload truncated", whole[:28+len(envelope)+4], recorded},
		{"envelope truncated", whole[:28+7], nil},
		{"envelope is garbage", handFrame(uint32(era), `{"cores":[`, payload), nil},
		{"header truncated", whole[:12], nil},
		{"not a state file", []byte("nothing to see here"), nil},
		{"empty file", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, file := seed(t, tt.raw)
			got, err := file.Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if dir := filepath.Dir(got.Archived); dir != filepath.Dir(path) {
				t.Errorf("Archived directory = %q, want %q", dir, filepath.Dir(path))
			}
			base := filepath.Base(got.Archived)
			if !strings.HasPrefix(base, "records.json.") || !strings.HasSuffix(base, ".bak") {
				t.Errorf("Archived = %q, want records.json.<unique>.bak", got.Archived)
			}
			if !reflect.DeepEqual(got.Cores, tt.cores) {
				t.Errorf("Cores = %#v, want %#v", got.Cores, tt.cores)
			}
			if !reflect.DeepEqual(got.Value, records{}) {
				t.Errorf("Value = %#v, want zero", got.Value)
			}
			aside, err := os.ReadFile(got.Archived)
			if err != nil {
				t.Fatalf("read archived file: %v", err)
			}
			if !bytes.Equal(aside, tt.raw) {
				t.Errorf("archived bytes = %q, want %q", aside, tt.raw)
			}
			if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("stat original = %v, want not-exist", err)
			}
			next, err := file.Load()
			if err != nil {
				t.Fatalf("second Load() error = %v", err)
			}
			if !reflect.DeepEqual(next, Loaded[records]{}) {
				t.Errorf("second Load() = %#v, want zero", next)
			}
		})
	}
}

func TestArchivesNeverOverwriteOneAnother(t *testing.T) {
	raw := []byte("nothing to see here")
	path := filepath.Join(t.TempDir(), "records.json")
	file := New[records](path, era)
	seen := make(map[string]bool, 2)
	for i := range 2 {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := file.Load()
		if err != nil {
			t.Fatalf("Load() %d error = %v", i, err)
		}
		if seen[got.Archived] {
			t.Fatalf("Archived = %q twice", got.Archived)
		}
		seen[got.Archived] = true
		aside, err := os.ReadFile(got.Archived)
		if err != nil || !bytes.Equal(aside, raw) {
			t.Fatalf("archived bytes = %q, %v", aside, err)
		}
	}
}

func TestLoadUnreadableFileIsError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a 0000 file regardless of mode")
	}
	path, file := seed(t, handFrame(uint32(era), `{"cores":[]}`, `{"note":"x"}`))
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Load(); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("Load() error = %v, want permission", err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "records.json" {
		t.Errorf("directory = %v, want the unreadable file alone", names(entries))
	}
}

func TestStoreLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	file := New[records](filepath.Join(dir, "records.json"), era)
	for i := range 3 {
		if err := file.Store(records{Note: strconv.Itoa(i)}); err != nil {
			t.Fatalf("Store(%d) error = %v", i, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "records.json" {
		t.Errorf("directory = %v, want the record file alone", names(entries))
	}
}

func TestStoreIntoAnUnusableDirectoryIsError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	file := New[records](filepath.Join(blocker, "records.json"), era)
	if err := file.Store(records{Note: "x"}); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("Store() error = %v, want ENOTDIR", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "blocker" {
		t.Errorf("directory = %v, want the blocker alone", names(entries))
	}
}

func TestStoreIsWholeOrNothingUnderConcurrentLoad(t *testing.T) {
	const rounds = 100
	file := New[records](filepath.Join(t.TempDir(), "records.json"), era)
	if err := file.Store(records{Live: []Core{{PID: 0}}, Note: "0"}); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	var stored error
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= rounds; i++ {
			if stored = file.Store(records{Live: []Core{{PID: i}}, Note: strconv.Itoa(i)}); stored != nil {
				return
			}
		}
	}()
	reads := 0
	for {
		select {
		case <-done:
			if stored != nil {
				t.Fatalf("Store() error = %v", stored)
			}
			if reads == 0 {
				t.Fatal("observed no read during the writes")
			}
			return
		default:
		}
		got, err := file.Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if got.Archived != "" {
			t.Fatalf("Load() archived %q mid-write", got.Archived)
		}
		generation, err := strconv.Atoi(got.Value.Note)
		if err != nil || generation < 0 || generation > rounds {
			t.Fatalf("Value.Note = %q, want a generation in [0,%d]", got.Value.Note, rounds)
		}
		if want := []Core{{PID: generation}}; !reflect.DeepEqual(got.Cores, want) {
			t.Fatalf("Cores = %#v, want %#v", got.Cores, want)
		}
		reads++
	}
}

func names(entries []os.DirEntry) []string {
	found := make([]string, 0, len(entries))
	for _, entry := range entries {
		found = append(found, entry.Name())
	}
	return found
}
