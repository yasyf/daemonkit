package stopcontrol

import (
	"testing"
	"time"
)

func TestV1TimingOrder(t *testing.T) {
	if IdentityBound <= 0 || TrackBound <= 0 || AuthorityBound <= 0 || ChildSettlementBound <= 0 ||
		ParentSettlementMargin <= 0 || DeferredUntrackBound <= 0 || PollInterval <= 0 {
		t.Fatal("stop-control timing must be positive")
	}
	if ParentOperationBound != ChildSettlementBound+ParentSettlementMargin {
		t.Fatalf("parent operation bound = %v, want %v", ParentOperationBound, ChildSettlementBound+ParentSettlementMargin)
	}
	if TotalBound != 50*time.Second {
		t.Fatalf("total stop bound = %v, want 50s", TotalBound)
	}
}
