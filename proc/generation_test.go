package proc

import (
	"encoding/json"
	"testing"
)

func TestProcessGenerationIsStableAndNonempty(t *testing.T) {
	first, err := ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	second, err := ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if first == (OwnerGeneration{}) || first != second {
		t.Fatalf("process generations = %q, %q", first, second)
	}
}

func TestOwnerGenerationTextAndJSONAreExact(t *testing.T) {
	const encoded = "0123456789abcdef0123456789abcdef"
	generation, err := ParseOwnerGeneration(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if generation.String() != encoded {
		t.Fatalf("generation = %q", generation)
	}
	payload, err := json.Marshal(generation)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `"`+encoded+`"` {
		t.Fatalf("json = %s", payload)
	}
	var decoded OwnerGeneration
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != generation {
		t.Fatalf("decoded = %q", decoded)
	}
}

func TestOwnerGenerationRejectsNoncanonicalValues(t *testing.T) {
	for _, value := range []string{
		"", "1", "0123456789abcdef0123456789abcde",
		"0123456789ABCDEF0123456789ABCDEF",
		"0123456789abcdef0123456789abcdeg",
		"00000000000000000000000000000000",
		"0123456789abcdef0123456789abcdef0",
	} {
		if _, err := ParseOwnerGeneration(value); err == nil {
			t.Fatalf("ParseOwnerGeneration(%q) succeeded", value)
		}
		var generation OwnerGeneration
		if err := json.Unmarshal([]byte(`"`+value+`"`), &generation); err == nil {
			t.Fatalf("json.Unmarshal(%q) succeeded", value)
		}
	}
	if _, err := json.Marshal(OwnerGeneration{}); err == nil {
		t.Fatal("zero generation marshaled")
	}
}
