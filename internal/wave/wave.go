/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package wave holds the pure decision logic for sites and waves.
//
// The reconciler reads every member's state from the backend (GitHub) once per
// reconcile, flattens it into an Input, and calls Decide. Decide has no I/O and
// no clock: the same Input always yields the same Decision, so every cluster
// that sees the same GitHub state reaches the same answer about who leads and
// what each site may deploy.
//
// Vocabulary
//
//   - Environment: dev, staging, prod. Env-to-env ordering is the existing
//     Relationship (After); this package only needs the upstream env name.
//   - Site: one cluster's instance of an environment (eu-west-1).
//   - Wave: ordering of sites WITHIN one environment, 1-based, contiguous.
//   - Member: a site registered in the backend for (environment, wave). A member
//     is registered once it has at least one deployment and its newest deployment
//     is not marked deregistered.
//   - Selection record: a deployment written by the leader of (env, wave) with
//     Selection=true. The newest selection record (highest Order) among the
//     members of (env, wave) — including deregistered members — is the wave's
//     current selection: its Revision is what followers deploy, its Site is the
//     recorded leader.
//
// Leader rule (single writer, sticky while in flight)
//
//  1. Let S be the current selection of (env, wave), if any.
//  2. If S exists, its recorded leader is still registered, and S is NOT yet
//     complete (some registered member of the wave has not baked S.Revision
//     successfully), the recorded leader stays leader. A lower-sorting site that
//     registers mid-flight does not take over.
//  3. Otherwise the leader is the lexicographically lowest registered site of the
//     wave (self is always counted, registered or not).
//  4. If no selection record exists at all in (env, wave) (fresh fleet, or a
//     fleet migrating from site-less), the computed leader's newest deployment
//     acts as the implicit selection, so followers have something to follow.
//
// Allowed versions
//
//   - Wave-1 leader: revisions that fully completed the upstream environment
//     (baked Succeeded by every registered member of the upstream's LAST wave; a
//     site-less upstream passes UpstreamBaked through). No upstream: unrestricted.
//   - Wave-N leader (N>1): revisions baked Succeeded by EVERY registered member of
//     wave N-1 of the same environment.
//   - Follower (any wave): exactly the current selection of its wave. Followers do
//     not wait for the leader's bake.
//   - Missing or unreadable bake status counts as not done.
//   - Revisions are mapped to tags through Input.Candidates; a revision without a
//     tag is dropped (WaitingForCandidate when that empties a follower's set).
//
// Lockstep: if the wave-1 leader's own spec has Lockstep and the current wave-1
// selection is not yet baked Succeeded by every registered member of every wave
// of the environment, the leader's allowed set is pinned to the current
// selection (LockstepWaiting).
//
// Topology: the waves present in an environment (registered members plus self)
// must be exactly 1..max. A gap blocks everything (empty allowed set,
// InvalidTopology). The same check applies to the upstream environment.
package wave

// Bake states as the controller writes them; they match the rollout
// controller's BakeStatus strings. Empty means unknown.
const (
	BakeSucceeded  = "Succeeded"
	BakeFailed     = "Failed"
	BakeInProgress = "InProgress"
	BakeDeploying  = "Deploying"
	BakeCancelled  = "Cancelled"
)

// Role of a site inside its (environment, wave).
type Role string

const (
	RoleLeader   Role = "Leader"
	RoleFollower Role = "Follower"
)

// Condition reasons. Every blocked state carries exactly one blocking reason.
const (
	ReasonReady               = "Ready"
	ReasonWaitingFor          = "WaitingFor"
	ReasonWaitingForCandidate = "WaitingForCandidate"
	ReasonPinned              = "Pinned"
	ReasonInvalidTopology     = "InvalidTopology"
	ReasonLockstepWaiting     = "LockstepWaiting"
	ReasonLockstepMismatch    = "LockstepMismatch"
)

// Deployment is one backend deployment of one member, already reduced to the
// fields the decision needs.
type Deployment struct {
	// Revision is the git revision deployed.
	Revision string
	// Selection is true when this deployment was written by the leader of its
	// (environment, wave) — a selection record.
	Selection bool
	// Bake is the latest bake state read back from the backend ("" = unknown).
	Bake string
	// Order is a strictly increasing backend ordering (GitHub deployment ID).
	// Larger is newer. Orders are comparable across members of one repository.
	Order int64
}

// Member is one site of one environment as seen in the backend.
type Member struct {
	Environment string
	Site        string
	// Wave as recorded on the member's newest deployment.
	Wave int32
	// Lockstep as recorded on the member's newest deployment.
	Lockstep bool
	// Deregistered is true when the member's newest deployment carries the
	// deregistration marker. A deregistered member does not count for
	// membership, leadership, or completion, but its selection records still do.
	Deregistered bool
	// Deployments, any order; Decide sorts by Order. For a given revision the
	// member's bake is the Bake of its newest (highest Order) deployment of
	// that revision.
	Deployments []Deployment
}

// Self is the Environment being reconciled.
type Self struct {
	Environment string
	Site        string
	Wave        int32
	Lockstep    bool
	// Upstream is the Relationship (After) environment, "" when none.
	Upstream string
	// PinnedVersion is the local Rollout's spec.wantedVersion ("" when not pinned).
	PinnedVersion string
}

// Input is everything Decide needs. It is built once per reconcile.
type Input struct {
	Self Self
	// Members of Self.Environment and of Self.Upstream, as read from the
	// backend. Self may or may not be present (it is absent before its first
	// deployment); Decide always counts Self as a registered member of
	// (Self.Environment, Self.Wave) and overrides any recorded Wave/Lockstep for
	// Self with the spec values.
	Members []Member
	// UpstreamSiteless is true when the upstream environment has no registered
	// sited members; UpstreamBaked then carries the revisions it baked
	// Succeeded (today's single-stream logic).
	UpstreamSiteless bool
	UpstreamBaked    []string
	// Candidates maps revision -> tag from the local Rollout's releaseCandidates.
	Candidates map[string]string
}

// Blocker explains why a site cannot progress. Message names the blocker, e.g.
// "prod/us-east-1 bake on 1a2b3c4" or "prod has waves 1,3 (missing 2)".
type Blocker struct {
	Reason  string
	Message string
}

// Decision is the outcome for Self.
type Decision struct {
	Role Role
	// Leader is the site leading (Self.Environment, Self.Wave).
	Leader string
	// SelectedRevision is the current selection of Self's wave ("" if none).
	SelectedRevision string
	// Unrestricted is true when the gate must not restrict versions at all
	// (wave-1 leader without upstream and not held by lockstep). AllowedTags is
	// nil in that case.
	Unrestricted bool
	// AllowedTags is the RolloutGate allowedVersions when !Unrestricted. Never
	// nil when !Unrestricted (empty slice blocks everything). Order: newest
	// first by the Order of the deployment that qualified the revision; ties
	// broken by tag name.
	AllowedTags []string
	// Blocker is the reason Self is not progressing, nil when Ready. When
	// AllowedTags is empty and !Unrestricted, Blocker is always set.
	Blocker *Blocker
	// Mismatch is set (Reason LockstepMismatch) when a registered member of the
	// environment disagrees with the wave-1 leader on Lockstep. Informational:
	// it never changes AllowedTags.
	Mismatch *Blocker
}

// Decide computes Self's role and gate. Pure; see the package doc for the rules.
func Decide(in Input) Decision {
	panic("wave.Decide: not implemented")
}

// UpstreamComplete returns the revisions that fully completed the given sited
// environment: baked Succeeded by every registered member of its last wave. It
// returns a Blocker (InvalidTopology) when the environment's waves have a gap.
// Used directly by site-less Environments whose upstream is sited.
func UpstreamComplete(env string, members []Member) ([]string, *Blocker) {
	panic("wave.UpstreamComplete: not implemented")
}
