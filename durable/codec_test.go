package durable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type cursor struct {
	Identity string `json:"identity"`
	Schema   int    `json:"schema"`
	Offset   int    `json:"offset"`
	Peer     *peer  `json:"peer,omitempty"`
}

type peer struct {
	Label string `json:"label"`
}

var errForeignCursor = errors.New("foreign cursor")

func (c cursor) Validate() error {
	if c.Identity != "daemonkit.test.cursor.v1" || c.Schema != 1 {
		return errForeignCursor
	}
	return nil
}

func exactCursor() cursor {
	return cursor{Identity: "daemonkit.test.cursor.v1", Schema: 1, Offset: 7}
}

func TestMarshalValidatesAndTerminates(t *testing.T) {
	data, err := Marshal(exactCursor())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"identity":"daemonkit.test.cursor.v1","schema":1,"offset":7}` + "\n"
	if string(data) != want {
		t.Fatalf("Marshal = %q, want %q", data, want)
	}
	if _, err := Marshal(cursor{}); !errors.Is(err, errForeignCursor) {
		t.Fatalf("Marshal of an invalid value = %v, want %v", err, errForeignCursor)
	}
}

func TestUnmarshalIsStrict(t *testing.T) {
	exact := `{"identity":"daemonkit.test.cursor.v1","schema":1,"offset":7}`
	tests := []struct {
		name string
		data string
	}{
		{"unknown field", `{"identity":"daemonkit.test.cursor.v1","schema":1,"offset":7,"future":true}`},
		{"trailing value", exact + `{"identity":"daemonkit.test.cursor.v1","schema":1}`},
		{"duplicate key", `{"identity":"daemonkit.test.cursor.v1","schema":1,"offset":7,"offset":9}`},
		{"duplicate key nested in an object", `{"identity":"daemonkit.test.cursor.v1","schema":1,` +
			`"peer":{"label":"a","label":"b"}}`},
		{"duplicate key nested in an array", `[{"label":"a","label":"b"}]`},
		{"empty", ``},
		{"null", `null`},
		{"truncated", `{"identity":"daemonkit`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Unmarshal[cursor]([]byte(test.data)); err == nil {
				t.Fatalf("Unmarshal accepted %s", test.data)
			}
		})
	}

	got, err := Unmarshal[cursor]([]byte(exact))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != exactCursor() {
		t.Fatalf("Unmarshal = %+v, want %+v", got, exactCursor())
	}
	if _, err := Unmarshal[cursor]([]byte(`{"identity":"foreign","schema":1,"offset":7}`)); !errors.Is(err, errForeignCursor) {
		t.Fatalf("Unmarshal of a foreign identity = %v, want %v", err, errForeignCursor)
	}
}

// TestReadFileSeparatesAbsenceFromCorruption pins the one branch every ladder
// makes at the call site: absence is a state, a torn file never is.
func TestReadFileSeparatesAbsenceFromCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")

	if _, err := ReadFile[cursor](path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadFile of an absent path = %v, want os.ErrNotExist", err)
	}

	if err := os.WriteFile(path, []byte(`{"identity":"daemonkit.test.cur`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadFile[cursor](path)
	if err == nil {
		t.Fatal("ReadFile accepted a torn file")
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadFile reported a torn file as absence: %v", err)
	}

	data, err := Marshal(exactCursor())
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFile[cursor](path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got != exactCursor() {
		t.Fatalf("ReadFile = %+v, want %+v", got, exactCursor())
	}
}
