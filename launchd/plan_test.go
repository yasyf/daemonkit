package launchd

import (
	"path/filepath"
	"testing"
	"time"
)

func residentAgent(t *testing.T, label string) Agent {
	t.Helper()
	return Agent{
		Label:         label,
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), label+".log"),
		Env:           map[string]string{"PATH": "/usr/bin"},
		RestartPolicy: RestartAlways,
	}
}

func TestNewPlanRestorePlanRoundTripIsStable(t *testing.T) {
	agents := []Agent{residentAgent(t, "com.example.a"), residentAgent(t, "com.example.b")}
	plan, err := NewPlan(agents)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Digest().String()) != 64 {
		t.Fatalf("digest %q is not 32 hex-encoded bytes", plan.Digest())
	}

	restored, err := RestorePlan(plan.Agents(), plan.Digest())
	if err != nil {
		t.Fatalf("RestorePlan of a NewPlan round-trip failed: %v", err)
	}
	if restored.Digest() != plan.Digest() {
		t.Fatalf("restored digest %s != original %s", restored.Digest(), plan.Digest())
	}
}

func TestNewPlanDigestIsDeterministicAndOrderIndependent(t *testing.T) {
	a := residentAgent(t, "com.example.a")
	b := residentAgent(t, "com.example.b")

	first, err := NewPlan([]Agent{a, b})
	if err != nil {
		t.Fatal(err)
	}
	again, err := NewPlan([]Agent{a, b})
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := NewPlan([]Agent{b, a})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != again.Digest() {
		t.Fatalf("NewPlan digest is not deterministic: %s vs %s", first.Digest(), again.Digest())
	}
	if first.Digest() != reordered.Digest() {
		t.Fatalf("NewPlan digest depends on agent order: %s vs %s", first.Digest(), reordered.Digest())
	}
}

// TestRestorePlanReconstructsWithoutProgramResidency proves the durable value
// layer stays pure: RestorePlan validates and digests a plan whose program is
// not resident in the current namespace, unlike NewPlan.
func TestRestorePlanReconstructsWithoutProgramResidency(t *testing.T) {
	agents := []Agent{{
		Label:         "com.example.gone",
		Program:       "/opt/absent/bin/worker",
		LogPath:       "/opt/absent/var/worker.log",
		Env:           map[string]string{"PATH": "/usr/bin"},
		RestartPolicy: RestartAlways,
	}}
	if _, err := NewPlan(agents); err == nil {
		t.Fatal("NewPlan accepted a non-resident program")
	}

	canonical, err := canonicalPlanAgents(agents)
	if err != nil {
		t.Fatal(err)
	}
	want, err := planFromAgents(canonical)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestorePlan(agents, want.Digest())
	if err != nil {
		t.Fatalf("RestorePlan rejected a durable non-resident plan: %v", err)
	}
	if restored.Digest() != want.Digest() {
		t.Fatalf("restored digest %s != expected %s", restored.Digest(), want.Digest())
	}
}

// TestPlanDigestSeparatesExitTimeOut holds ExitTimeOut inside the digested
// identity: two agents that drain on different deadlines are different agents,
// unlike the ownership marker, which is a render-time constant every plist
// carries and so distinguishes nothing.
func TestPlanDigestSeparatesExitTimeOut(t *testing.T) {
	agent := residentAgent(t, "com.example.a")
	patient := agent
	patient.ExitTimeOut = 90 * time.Second

	plain, err := NewPlan([]Agent{agent})
	if err != nil {
		t.Fatal(err)
	}
	timed, err := NewPlan([]Agent{patient})
	if err != nil {
		t.Fatal(err)
	}
	if plain.Digest() == timed.Digest() {
		t.Fatalf("digest %s ignores ExitTimeOut", plain.Digest())
	}
	restored, err := RestorePlan(timed.Agents(), timed.Digest())
	if err != nil {
		t.Fatalf("RestorePlan of an agent with an exit timeout: %v", err)
	}
	if got := restored.Agents()[0].ExitTimeOut; got != patient.ExitTimeOut {
		t.Fatalf("restored ExitTimeOut = %s, want %s", got, patient.ExitTimeOut)
	}
}

// TestPlanDigestIsFrozenWithoutAnExitTimeOut pins the durable wire format: an
// agent that sets no exit timeout digests to exactly what it digested before
// the field existed, so a plan a consumer stored still restores.
func TestPlanDigestIsFrozenWithoutAnExitTimeOut(t *testing.T) {
	const frozen = "d4afa0cd2002257967cd4dce3e0c6b84d5a109c9eea76b42a159ecf2f83e70b5"
	plan, err := NewPlan([]Agent{{
		Label:         "com.example.frozen",
		Program:       "/usr/bin/true",
		LogPath:       "/tmp/daemonkit-frozen.log",
		Env:           map[string]string{"PATH": "/usr/bin"},
		RestartPolicy: RestartAlways,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Digest().String(); got != frozen {
		t.Fatalf("plan digest = %s, want the frozen %s", got, frozen)
	}
}

func TestRestorePlanRejectsMismatchedDigest(t *testing.T) {
	agents := []Agent{residentAgent(t, "com.example.a")}
	if _, err := RestorePlan(agents, PlanDigest{}); err == nil {
		t.Fatal("RestorePlan accepted a digest that does not match its agents")
	}
}
