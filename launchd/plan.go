package launchd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// PlanDigest is the immutable digest of one canonical launchd plan.
type PlanDigest [sha256.Size]byte

// String returns the lowercase hexadecimal digest.
func (d PlanDigest) String() string { return hex.EncodeToString(d[:]) }

// Plan is an immutable canonical set of LaunchAgents.
type Plan struct {
	agents map[string]Agent
	digest PlanDigest
	valid  bool
}

// NewPlan validates and canonicalizes one complete LaunchAgent set, requiring
// each agent's program to be resident in the current namespace.
func NewPlan(agents []Agent) (Plan, error) {
	canonical, err := desiredAgents(agents)
	if err != nil {
		return Plan{}, err
	}
	return planFromAgents(canonical)
}

// RestorePlan reconstructs one previously validated durable plan without
// requiring that every old executable is resident in the current namespace.
func RestorePlan(agents []Agent, expected PlanDigest) (Plan, error) {
	canonical, err := canonicalPlanAgents(agents)
	if err != nil {
		return Plan{}, err
	}
	plan, err := planFromAgents(canonical)
	if err != nil {
		return Plan{}, err
	}
	if plan.digest != expected {
		return Plan{}, errors.New("launchd: restored plan digest does not match its agents")
	}
	return plan, nil
}

func canonicalPlanAgents(agents []Agent) (map[string]Agent, error) {
	canonical := make(map[string]Agent, len(agents))
	for _, agent := range agents {
		if _, err := agent.Plist(); err != nil {
			return nil, fmt.Errorf("launchd: validate plan agent %q: %w", agent.Label, err)
		}
		if _, duplicate := canonical[agent.Label]; duplicate {
			return nil, fmt.Errorf("launchd: duplicate plan agent label %q", agent.Label)
		}
		acceptIgnoredSessionType(&agent)
		agent.Args = append([]string(nil), agent.Args...)
		agent.Env = cloneStrings(agent.Env)
		agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
			agent.AssociatedBundleIdentifiers,
		)
		canonical[agent.Label] = agent
	}
	return canonical, nil
}

func planFromAgents(agents map[string]Agent) (Plan, error) {
	type encodedAgent struct {
		Label string          `json:"label"`
		Agent json.RawMessage `json:"agent"`
	}
	// The identity string feeds the digest and is aliased by durable fleet
	// state; it must stay byte-identical across the service → launchd re-home.
	wire := struct {
		Identity string         `json:"identity"`
		Schema   int            `json:"schema"`
		Agents   []encodedAgent `json:"agents"`
	}{Identity: "daemonkit.service.plan.v1", Schema: 1}
	for _, label := range slices.Sorted(maps.Keys(agents)) {
		agent := agents[label]
		if label != agent.Label {
			return Plan{}, fmt.Errorf("launchd: plan key %q does not match agent label %q", label, agent.Label)
		}
		payload, err := encodeAgent(agent)
		if err != nil {
			return Plan{}, err
		}
		wire.Agents = append(wire.Agents, encodedAgent{Label: label, Agent: payload})
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return Plan{}, fmt.Errorf("launchd: encode plan: %w", err)
	}
	return Plan{agents: copyAgents(agents), digest: sha256.Sum256(payload), valid: true}, nil
}

// Agents returns a defensive copy in canonical label order.
func (p Plan) Agents() []Agent {
	agents := make([]Agent, 0, len(p.agents))
	for _, label := range slices.Sorted(maps.Keys(p.agents)) {
		agent := copyAgents(map[string]Agent{label: p.agents[label]})[label]
		agents = append(agents, agent)
	}
	return agents
}

// Digest returns the canonical plan digest.
func (p Plan) Digest() PlanDigest { return p.digest }

func encodeAgent(agent Agent) ([]byte, error) {
	if _, err := agent.Plist(); err != nil {
		return nil, fmt.Errorf("launchd: validate stored agent %q: %w", agent.Label, err)
	}
	agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
		agent.AssociatedBundleIdentifiers,
	)
	payload, err := json.Marshal(agent)
	if err != nil {
		return nil, fmt.Errorf("launchd: encode stored agent %q: %w", agent.Label, err)
	}
	return payload, nil
}

func copyAgents(agents map[string]Agent) map[string]Agent {
	copied := make(map[string]Agent, len(agents))
	for label, agent := range agents {
		agent.Args = append([]string(nil), agent.Args...)
		agent.Env = cloneStrings(agent.Env)
		agent.AssociatedBundleIdentifiers = append(
			[]string(nil), agent.AssociatedBundleIdentifiers...,
		)
		copied[label] = agent
	}
	return copied
}

func cloneStrings(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
