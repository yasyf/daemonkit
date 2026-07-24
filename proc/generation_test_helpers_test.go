package proc

import "crypto/sha256"

const testConsumerRecoveryID RecoveryID = "test.consumer.v1"

func testOwnerGeneration(label string) OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}
