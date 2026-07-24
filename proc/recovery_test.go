package proc

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestRecoveryIDIsStrictNamespacedV1(t *testing.T) {
	for _, value := range []string{
		"daemonkit.task.v1", "consumer.alpha.v1", "consumer2.beta.v1",
	} {
		id, err := ParseRecoveryID(value)
		if err != nil {
			t.Fatalf("ParseRecoveryID(%q): %v", value, err)
		}
		if string(id) != value {
			t.Fatalf("id = %q, want %q", id, value)
		}
		payload, err := json.Marshal(id)
		if err != nil {
			t.Fatal(err)
		}
		var decoded RecoveryID
		if err := json.Unmarshal(payload, &decoded); err != nil || decoded != id {
			t.Fatalf("json round trip = %q, %v", decoded, err)
		}
	}
	for _, value := range []string{
		"", "task.v1", "Daemonkit.task.v1", "daemonkit..task.v1", ".daemonkit.task.v1",
		"daemonkit.task.v2", "daemonkit.task_.v1", "daemonkit.-task.v1", "daemonkit.task.v1.",
		strings.Repeat("a", maxRecoveryIDBytes+1),
	} {
		if _, err := ParseRecoveryID(value); err == nil {
			t.Fatalf("ParseRecoveryID(%q) succeeded", value)
		}
		var decoded RecoveryID
		if err := json.Unmarshal([]byte(`"`+value+`"`), &decoded); err == nil {
			t.Fatalf("json.Unmarshal(%q) succeeded", value)
		}
	}
}

func TestRecoveryReceiptCanonicalizesAndCopiesSettledGenerations(t *testing.T) {
	current := testOwnerGeneration("current")
	first := testOwnerGeneration("first")
	second := testOwnerGeneration("second")
	receipt, err := newRecoveryReceipt(RecoveryTaskID, current, []OwnerGeneration{second, first, second})
	if err != nil {
		t.Fatal(err)
	}
	settled := receipt.Settled()
	want := []OwnerGeneration{first, second}
	slices.SortFunc(want, generationCompare)
	if !slices.Equal(settled, want) {
		t.Fatalf("settled = %v, want %v", settled, want)
	}
	settled[0] = OwnerGeneration{}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("caller mutated receipt: %v", err)
	}

	duplicate, err := newRecoveryReceipt(RecoveryTaskID, current, []OwnerGeneration{first})
	if err != nil {
		t.Fatal(err)
	}
	combined, err := CombineRecoveryReceipts(RecoveryTaskID, current, receipt, duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(combined.Settled(), want) {
		t.Fatalf("combined settled = %v, want %v", combined.Settled(), want)
	}
}

func TestRecoveryReceiptRejectsInvalidAuthorityAndForgery(t *testing.T) {
	current := testOwnerGeneration("current")
	other := testOwnerGeneration("other")
	valid, err := newRecoveryReceipt(RecoveryTaskID, current, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CombineRecoveryReceipts(RecoveryTrustID, current, valid); err == nil {
		t.Fatal("combined mismatched recovery id")
	}
	if _, err := CombineRecoveryReceipts(RecoveryTaskID, other, valid); err == nil {
		t.Fatal("combined mismatched current generation")
	}
	if _, err := CombineRecoveryReceipts(RecoveryTaskID, current); err == nil {
		t.Fatal("combined zero receipts")
	}
	forged := valid
	forged.digest[0] ^= 1
	if err := forged.Validate(); err == nil {
		t.Fatal("forged digest validated")
	}
	for _, settled := range [][]OwnerGeneration{{{}}, {current}} {
		if _, err := newRecoveryReceipt(RecoveryTaskID, current, settled); err == nil {
			t.Fatalf("invalid settled generations accepted: %v", settled)
		}
	}
}
