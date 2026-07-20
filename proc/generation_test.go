package proc

import "testing"

func TestProcessGenerationIsStableAndNonempty(t *testing.T) {
	first, err := ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	second, err := ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || first != second {
		t.Fatalf("process generations = %q, %q", first, second)
	}
}
