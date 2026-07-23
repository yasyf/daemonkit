package stopcontrol

import "testing"

func TestV1TimingOrder(t *testing.T) {
	if IdentityBound <= 0 || AuthorityBound <= 0 || ChildSettlementBound <= 0 || PollInterval <= 0 {
		t.Fatal("stop-control timing must be positive")
	}
	if ParentOperationBound <= ChildSettlementBound {
		t.Fatalf("parent operation bound %v must exceed child settlement bound %v", ParentOperationBound, ChildSettlementBound)
	}
}
