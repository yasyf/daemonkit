//go:build darwin

package deployment

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestRuntimeStopControlStoreConsumesControllerAuthority(t *testing.T) {
	executable, location := runtimeAppFixture(t)
	prior := runtimeExecutable
	runtimeExecutable = func() (string, error) { return executable, nil }
	t.Cleanup(func() { runtimeExecutable = prior })
	runtimeStore, err := RuntimeStopControlStore()
	if err != nil {
		t.Fatal(err)
	}
	controllerStore := &proc.FileStore{Path: deploymentPathsForLocation(location).serviceProcess}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	reaper := &proc.Reaper{Store: controllerStore, Generation: proc.OwnerGeneration{1}}
	const role = "com.example.stop"
	const operationID = "stop-operation"
	target := proc.OwnerGeneration{2}
	stopSession := proc.StopSessionID{1}
	preparationNonce := proc.StopPreparationNonce{2}
	if _, err := reaper.TrackStopControl(
		context.Background(), identity, role, operationID, stopSession, preparationNonce, 1, target, time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	_, consumed, err := runtimeStore.ConsumeStopControl(
		t.Context(), identity, role, operationID, stopSession, preparationNonce, 1, target, time.Now(),
	)
	if err != nil || !consumed {
		t.Fatalf("ConsumeStopControl = %v, %v", consumed, err)
	}
}
