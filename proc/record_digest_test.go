package proc

import "testing"

func TestRecordDigestBindsExactProcessIdentity(t *testing.T) {
	identity := Identity{PID: 42, StartTime: "start", Boot: "boot", Comm: "ignored", Executable: "/bin/runtime"}
	first, err := NewRecordDigest(identity)
	if err != nil {
		t.Fatal(err)
	}
	identity.Comm = "also-ignored"
	second, err := NewRecordDigest(identity)
	if err != nil {
		t.Fatal(err)
	}
	if first == (RecordDigest{}) || first != second {
		t.Fatalf("record digest = %x, %x", first, second)
	}
	identity.StartTime = "successor"
	third, err := NewRecordDigest(identity)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("record digest accepted a reused PID identity")
	}
}

func TestRecordDigestRejectsIncompleteIdentity(t *testing.T) {
	if _, err := NewRecordDigest(Identity{PID: 42, StartTime: "start", Boot: "boot"}); err == nil {
		t.Fatal("record digest accepted an identity without an executable")
	}
}
