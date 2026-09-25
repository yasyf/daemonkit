//go:build darwin

package launchd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

type maintenanceRunner struct {
	label               string
	disabled            bool
	verbs               []string
	replaceAfterDisable bool
}

func (r *maintenanceRunner) run(_ context.Context, _ string, args ...string) (string, int, error) {
	r.verbs = append(r.verbs, args[0])
	switch args[0] {
	case "print":
		pid := os.Getpid()
		if r.replaceAfterDisable && r.disabled {
			pid++
		}
		return fmt.Sprintf("%s = {\n\tpid = %d\n\tproperties = keepalive | runatload\n}\n", serviceTarget(r.label), pid), 0, nil
	case "print-disabled":
		state := "enabled"
		if r.disabled {
			state = "disabled"
		}
		return fmt.Sprintf("disabled services = {\n\t\"%s\" => %s\n}\n", r.label, state), 0, nil
	case "disable":
		r.disabled = true
		return "", 0, nil
	case "enable":
		r.disabled = false
		return "", 0, nil
	default:
		return "unexpected verb", 1, errors.New("unexpected launchctl mutation")
	}
}

func TestMaintenanceOnlyChangesExactLabelEnablement(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			dir := launchAgentsDir(t)
			agent := applyAgent(t, "com.example.maintenance")
			installPlist(t, dir, agent)
			runner := &maintenanceRunner{label: agent.Label, disabled: disabled}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			lease, err := PauseRestarts(ctx, runner.run, agent.Label, os.Getpid())
			if err != nil {
				t.Fatal(err)
			}
			if !runner.disabled {
				t.Fatal("service not disabled")
			}
			if err := lease.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if runner.disabled != disabled {
				t.Fatal("original state not restored")
			}
			for _, forbidden := range []string{"bootout", "bootstrap", "kickstart", "kill"} {
				if slices.Contains(runner.verbs, forbidden) {
					t.Fatalf("unexpected %s", forbidden)
				}
			}
		})
	}
}

func TestMaintenanceRefusesChangedLoadedIdentityWithoutSignaling(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.maintenance-race")
	installPlist(t, dir, agent)
	runner := &maintenanceRunner{label: agent.Label, replaceAfterDisable: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := PauseRestarts(ctx, runner.run, agent.Label, os.Getpid())
	if !errors.Is(err, ErrMaintenanceUnproven) || lease == nil {
		t.Fatalf("Pause=%v %v", lease, err)
	}
	if err := lease.Restore(ctx); !errors.Is(err, ErrMaintenanceUnproven) {
		t.Fatalf("Restore=%v", err)
	}
	if slices.Contains(runner.verbs, "enable") || slices.Contains(runner.verbs, "bootout") {
		t.Fatalf("changed job mutated: %v", runner.verbs)
	}
}

func TestMaintenanceDisabledStateUsesLaunchctlWords(t *testing.T) {
	for _, tc := range []struct {
		value    string
		disabled bool
		refused  bool
	}{
		{"enabled", false, false},
		{"disabled", true, false},
		{"true", false, true},
		{"", false, true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			client := applier{run: func(context.Context, string, ...string) (string, int, error) {
				return "disabled services = {\n\t\"com.example.maintenance\" => " + tc.value + "\n}\n", 0, nil
			}}
			disabled, err := client.disabled(t.Context(), "com.example.maintenance")
			if disabled != tc.disabled || errors.Is(err, ErrMaintenanceUnproven) != tc.refused {
				t.Fatalf("disabled=%v err=%v", disabled, err)
			}
		})
	}
}
