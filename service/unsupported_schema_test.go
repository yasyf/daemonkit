package service

import (
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

func TestControllerConfigThreadsSchemaPolicyToProcessStore(t *testing.T) {
	config := controllerConfig(t)
	config.UnsupportedSchema = proc.ArchiveUnsupportedSchema
	store := config.processStore()
	if store.Path != config.ProcessPath {
		t.Fatalf("process store path = %q, want %q", store.Path, config.ProcessPath)
	}
	if store.UnsupportedSchema != proc.ArchiveUnsupportedSchema {
		t.Fatalf("process store policy = %v, want ArchiveUnsupportedSchema", store.UnsupportedSchema)
	}
}

func TestControllerConfigDefaultsToFailClosedSchema(t *testing.T) {
	var failClosed proc.UnsupportedSchemaPolicy
	if store := controllerConfig(t).processStore(); store.UnsupportedSchema != failClosed {
		t.Fatalf("default process store policy = %v, want zero (fail-closed)", store.UnsupportedSchema)
	}
}
