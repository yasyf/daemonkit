package trust

import "testing"

func TestTrustPolicyStopAuthorityHasExactlyOneRole(t *testing.T) {
	config := trustPolicyConfig()
	if _, err := NewTrustPolicy(config); err != nil {
		t.Fatalf("one stop role: %v", err)
	}
	config.Roles["stop-2"] = trustPolicyRequirement("com.yasyf.daemonkit.stop-2")
	config.StopRoles = []PeerRole{"stop", "stop-2"}
	if _, err := NewTrustPolicy(config); err == nil {
		t.Fatal("two stop roles succeeded")
	}
}

func TestTrustPolicyLifecycleAuthorityHasAtMostTwoDistinctRoles(t *testing.T) {
	config := trustPolicyConfig()
	policy, err := NewTrustPolicy(config)
	if err != nil || !policy.AllowsReceipt("lifecycle") || !policy.AllowsReadiness("lifecycle") {
		t.Fatalf("shared lifecycle role = %v", err)
	}
	config.Roles["lifecycle-2"] = trustPolicyRequirement("com.yasyf.daemonkit.lifecycle-2")
	config.ReadinessRoles = []PeerRole{"lifecycle-2"}
	if _, err := NewTrustPolicy(config); err != nil {
		t.Fatalf("two lifecycle roles: %v", err)
	}
	config.Roles["lifecycle-3"] = trustPolicyRequirement("com.yasyf.daemonkit.lifecycle-3")
	config.ReadinessRoles = []PeerRole{"lifecycle-2", "lifecycle-3"}
	if _, err := NewTrustPolicy(config); err == nil {
		t.Fatal("three lifecycle roles succeeded")
	}
}

func TestTrustPolicyHandoffAuthorityHasOneSlotPerRole(t *testing.T) {
	config := trustPolicyConfig()
	config.Roles["broker-2"] = trustPolicyRequirement("com.yasyf.daemonkit.broker-2")
	config.HandoffRoles = []PeerRole{"broker", "broker-2"}
	policy, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range config.HandoffRoles {
		if !policy.AllowsHandoff(role) {
			t.Fatalf("handoff role %q lost authority", role)
		}
	}
	config.HandoffRoles = nil
	policy, err = NewTrustPolicy(config)
	if err != nil || policy.AllowsHandoff("broker") {
		t.Fatalf("empty handoff roles = %v", err)
	}
}

func TestTrustPolicyDeclaredOrdinaryRoleStaysOrdinary(t *testing.T) {
	config := trustPolicyConfig()
	config.Roles["ordinary"] = trustPolicyRequirement("com.yasyf.daemonkit.ordinary")
	policy, err := NewTrustPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	if policy.AllowsStop("ordinary") || policy.AllowsReceipt("ordinary") ||
		policy.AllowsReadiness("ordinary") || policy.AllowsHandoff("ordinary") {
		t.Fatal("declared ordinary role received protected authority")
	}
}
