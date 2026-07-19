package wire

import (
	"errors"
	"math"
	"testing"
)

func TestStreamSequenceRejectsBeforeWrappingToZero(t *testing.T) {
	sequence := streamSequence{next: math.MaxUint32 - 1}
	first, err := sequence.take()
	if err != nil || first != math.MaxUint32-1 {
		t.Fatalf("first take = %d, %v", first, err)
	}
	last, err := sequence.take()
	if err != nil || last != math.MaxUint32 {
		t.Fatalf("last take = %d, %v", last, err)
	}
	if _, err := sequence.take(); !errors.Is(err, ErrStreamOrder) {
		t.Fatalf("exhausted take error = %v, want ErrStreamOrder", err)
	}
}
