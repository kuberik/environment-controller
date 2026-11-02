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
//     wave. Self always counts for completion and topology, but a Self that is
//     not registered in the backend (no deployments yet, or deregistered) is
//     only eligible to lead when no other registered member exists in its wave:
//     the other members cannot see it yet, so it must not out-rank them.
//  4. If no selection record exists at all in (env, wave) (fresh fleet, or a
//     fleet migrating from site-less), the computed leader's newest deployment
//     acts as the implicit selection, so followers have something to follow. In
//     waves N>1 the implicit selection only counts when wave N-1 completed it.
//
// Selection records count only for the wave recorded on the deployment
// (Deployment.Wave), never for the member's current wave.
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
//   - A leader whose newest allowed release is the one it already runs
//     (Input.Current) while the previous wave (or upstream) still has a newer
//     selection in flight is not Ready: Blocker names the member it waits for.
//
// Lockstep: if the wave-1 leader's own spec has Lockstep and the current wave-1
// selection is not yet baked Succeeded by every registered member of every wave
// of the environment, the leader's allowed set is pinned to the current
// selection (LockstepWaiting). Consequences an operator must know: a failed
// bake in any wave holds the whole environment on the failed revision, and if
// the selected revision drops out of the leader's releaseCandidates the leader
// stays blocked until it returns. Both are released by turning lockstep off or
// pinning the leader's Rollout.
//
// Topology: the waves present in an environment (registered members plus self)
// must be exactly 1..max. A gap blocks everything (empty allowed set,
// InvalidTopology). The same check applies to the upstream environment.
package wave

import "sort"

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
	// Wave the member was in when it created this deployment (payload "wave").
	// A selection record only counts for the wave it was written in, so a site
	// that moves from wave 1 to wave 2 does not carry its wave-1 pick along.
	// 0 means unknown and is read as the member's recorded Wave.
	Wave int32
}

// Member is one site of one environment as seen in the backend. Input must
// carry at most one Member per (Environment, Site); duplicates are illegal
// input (the first one wins).
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
	// Candidates maps revision -> tag from the local Rollout's releaseCandidates
	// plus the release the site runs now (so an up-to-date site maps too).
	Candidates map[string]string
	// Current is the revision the local Rollout runs now ("" unknown). Only
	// used to tell "up to date, waiting for the previous wave" from Ready.
	Current string
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
	s := in.Self
	sm, selfRegistered := selfMember(in)
	own := newFleet(s.Environment, in.Members, sm, selfRegistered)
	st := own.state(s.Wave)

	d := Decision{
		Role:             RoleFollower,
		Leader:           st.leader,
		SelectedRevision: st.selRev,
		Mismatch:         own.mismatch(),
	}
	if st.leader == s.Site {
		d.Role = RoleLeader
	}

	// Topology problems block everything.
	if b := own.topology(); b != nil {
		return blocked(d, b)
	}
	// DECISION: an upstream wave gap blocks every role, followers included,
	// because the contract says it takes precedence over everything.
	var up fleet
	if s.Upstream != "" && s.Upstream == s.Environment {
		return blocked(d, &Blocker{Reason: ReasonInvalidTopology,
			Message: s.Environment + " is its own upstream"})
	}
	hasUpstream := s.Upstream != ""
	sitedUpstream := hasUpstream && !in.UpstreamSiteless
	if sitedUpstream {
		up = newFleet(s.Upstream, in.Members, nil, false)
		if b := up.topology(); b != nil {
			return blocked(d, b)
		}
	}

	if d.Role == RoleFollower {
		return decideFollower(d, in, st)
	}

	// Wave-1 leader with lockstep: pinned to the current selection until the
	// whole environment baked it.
	if s.Wave == 1 && s.Lockstep && st.selRev != "" {
		if b := own.lockstepBlocker(st.selRev); b != nil {
			d.Unrestricted = false
			d.AllowedTags = tagList(map[string]int64{st.selRev: 0}, in.Candidates)
			d.Blocker = b
			return d
		}
	}

	var revs map[string]int64
	var prev waveState
	var prevEnv string
	switch {
	case s.Wave > 1:
		prev, prevEnv = own.state(s.Wave-1), s.Environment
		revs = completedRevs(prev.reg)
	case !hasUpstream:
		d.Unrestricted = true
		return d
	case in.UpstreamSiteless:
		revs = map[string]int64{}
		for _, r := range in.UpstreamBaked {
			revs[r] = 0
		}
	default:
		prevEnv = s.Upstream
		prev = up.state(up.lastWave())
		revs = completedRevs(prev.reg)
	}

	d.AllowedTags = tagList(revs, in.Candidates)
	if len(d.AllowedTags) == 0 {
		if in.UpstreamSiteless && s.Wave == 1 {
			// DECISION: nothing baked upstream yet; there is no member to name.
			d.Blocker = &Blocker{Reason: ReasonWaitingFor, Message: s.Upstream + " has no baked revision"}
		} else {
			d.Blocker = prev.blocker(prevEnv)
		}
	} else if _, done := revs[prev.selRev]; prev.selRev != "" && !done &&
		in.Current != "" && d.AllowedTags[0] == in.Candidates[in.Current] {
		// The newest allowed release is the one this site already runs, and
		// the previous wave's newer selection is still in flight: nothing to
		// deploy yet, so name who the site is waiting for instead of Ready.
		d.Blocker = prev.blocker(prevEnv)
	}
	return d
}

func blocked(d Decision, b *Blocker) Decision {
	d.Unrestricted = false
	d.AllowedTags = []string{}
	d.Blocker = b
	return d
}

func decideFollower(d Decision, in Input, st waveState) Decision {
	env := in.Self.Environment
	d.AllowedTags = []string{}
	if st.selRev == "" {
		d.Blocker = &Blocker{Reason: ReasonWaitingFor, Message: env + "/" + st.leader + " selection"}
		return d
	}
	tag, ok := in.Candidates[st.selRev]
	if !ok || tag == "" {
		d.Blocker = &Blocker{Reason: ReasonWaitingForCandidate, Message: short(st.selRev)}
		return d
	}
	d.AllowedTags = []string{tag}
	if p := in.Self.PinnedVersion; p != "" && p != tag {
		d.Blocker = &Blocker{Reason: ReasonPinned, Message: "pinned to " + p + ", wave selected " + tag}
	}
	return d
}

// UpstreamComplete returns the revisions that fully completed the given sited
// environment: baked Succeeded by every registered member of its last wave. It
// returns a Blocker (InvalidTopology) when the environment's waves have a gap.
// Used directly by site-less Environments whose upstream is sited.
// Revisions are ordered newest first (ties by revision).
func UpstreamComplete(env string, members []Member) ([]string, *Blocker) {
	f := newFleet(env, members, nil, false)
	if b := f.topology(); b != nil {
		return nil, b
	}
	revs := completedRevs(f.state(f.lastWave()).reg)
	out := make([]string, 0, len(revs))
	for r := range revs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if revs[out[i]] != revs[out[j]] {
			return revs[out[i]] > revs[out[j]]
		}
		return out[i] < out[j]
	})
	return out, nil
}
