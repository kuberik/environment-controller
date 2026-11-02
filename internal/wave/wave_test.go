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

package wave

import (
	"math/rand"
	"reflect"
	"testing"
)

// d builds a deployment; sel marks a selection record.
func d(rev string, order int64, bake string, sel bool) Deployment {
	return Deployment{Revision: rev, Order: order, Bake: bake, Selection: sel}
}

func m(env, site string, wave int32, deps ...Deployment) Member {
	return Member{Environment: env, Site: site, Wave: wave, Deployments: deps}
}

func dereg(x Member) Member { x.Deregistered = true; return x }
func lock(x Member) Member  { x.Lockstep = true; return x }

var cands = map[string]string{"aaaaaaaaaa": "v1", "bbbbbbbbbb": "v2", "cccccccccc": "v3"}

const (
	A = "aaaaaaaaaa"
	B = "bbbbbbbbbb"
	C = "cccccccccc"
)

func self(site string, wave int32) Self {
	return Self{Environment: "prod", Site: site, Wave: wave}
}

func TestDecide(t *testing.T) {
	ok := BakeSucceeded
	tests := []struct {
		name        string
		in          Input
		role        Role
		leader      string
		selected    string
		unrestr     bool
		allowed     []string
		reason      string // "" = ready
		msg         string
		mismatch    bool
		checkMsg    bool
		checkAllowd bool
	}{
		{
			name: "no upstream, wave 1 leader unrestricted",
			in:   Input{Self: self("b", 1), Members: []Member{m("prod", "a", 1)}},
			role: RoleFollower, leader: "a", reason: ReasonWaitingFor, msg: "prod/a selection", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "lowest site leads, unrestricted without upstream",
			in:   Input{Self: self("a", 1), Members: []Member{m("prod", "a", 1), m("prod", "b", 1)}},
			role: RoleLeader, leader: "a", unrestr: true,
		},
		{
			name: "empty environment: self alone leads",
			in:   Input{Self: self("x", 1)},
			role: RoleLeader, leader: "x", unrestr: true,
		},
		{
			name: "sticky leader mid-flight when lower site registers",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "b", 1, d(A, 10, ok, true)), m("prod", "a", 1)}},
			role: RoleLeader, leader: "b", selected: A, unrestr: true,
		},
		{
			name: "leader recomputed after completion",
			in: Input{Self: self("a", 1), Candidates: cands, Members: []Member{
				m("prod", "b", 1, d(A, 10, ok, true)), m("prod", "a", 1, d(A, 11, ok, false))}},
			role: RoleLeader, leader: "a", selected: A, unrestr: true,
		},
		{
			name: "follower follows immediately without leader bake",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, BakeInProgress, true))}},
			role: RoleFollower, leader: "a", selected: A, allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			name: "follower selection record without tag",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d("deadbeefcafe", 10, "", true))}},
			role: RoleFollower, leader: "a", selected: "deadbeefcafe", reason: ReasonWaitingForCandidate, msg: "deadbee", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "follower: leader bake Failed drops the version",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, BakeFailed, true))}},
			role: RoleFollower, leader: "a", selected: A, reason: ReasonWaitingFor, msg: "prod/a bake failed on aaaaaaa", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "follower: leader bake Cancelled drops the version",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, BakeCancelled, true))}},
			role: RoleFollower, leader: "a", selected: A, reason: ReasonWaitingFor, msg: "prod/a bake cancelled on aaaaaaa", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "follower: leader bake Deploying keeps the version",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, BakeDeploying, true))}},
			role: RoleFollower, leader: "a", selected: A, allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			name: "follower: leader bake Succeeded keeps the version",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, BakeSucceeded, true))}},
			role: RoleFollower, leader: "a", selected: A, allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			name: "pinned follower",
			in: Input{Self: Self{Environment: "prod", Site: "b", Wave: 1, PinnedVersion: "v0"}, Candidates: cands,
				Members: []Member{m("prod", "a", 1, d(A, 10, "", true))}},
			role: RoleFollower, leader: "a", selected: A, allowed: []string{"v1"}, checkAllowd: true,
			reason: ReasonPinned, msg: "pinned to v0, wave selected v1", checkMsg: true,
		},
		{
			name: "pinned to selected tag is ready",
			in: Input{Self: Self{Environment: "prod", Site: "b", Wave: 1, PinnedVersion: "v1"}, Candidates: cands,
				Members: []Member{m("prod", "a", 1, d(A, 10, "", true))}},
			role: RoleFollower, leader: "a", selected: A, allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			// Live demo: wave 2 kept saying Ready while wave 1 was still rolling
			// a newer selection, because the release it already ran was allowed.
			name: "wave-2 leader names the wave-1 member still baking the newer selection",
			in: Input{Self: self("c", 2), Candidates: cands, Current: A, Members: []Member{
				m("prod", "a", 1, d(A, 10, ok, true), d(B, 20, ok, true)),
				m("prod", "b", 1, d(A, 11, ok, false), d(B, 21, BakeInProgress, false)),
				m("prod", "c", 2, d(A, 12, ok, true))}},
			role: RoleLeader, leader: "c", selected: A, allowed: []string{"v1"}, checkAllowd: true,
			reason: ReasonWaitingFor, msg: "prod/b bake on bbbbbbb", checkMsg: true,
		},
		{
			name: "wave-2 leader already on the completed selection is ready",
			in: Input{Self: self("c", 2), Candidates: cands, Current: B, Members: []Member{
				m("prod", "a", 1, d(B, 20, ok, true)),
				m("prod", "b", 1, d(B, 21, ok, false)),
				m("prod", "c", 2, d(B, 22, ok, true))}},
			role: RoleLeader, leader: "c", selected: B, allowed: []string{"v2"}, checkAllowd: true,
		},
		{
			name: "wave-2 leader gated on all wave-1 members",
			in: Input{Self: self("c", 2), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, ok, true), d(B, 20, ok, true)),
				m("prod", "b", 1, d(A, 11, ok, false), d(B, 21, BakeInProgress, false))}},
			role: RoleLeader, leader: "c", allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			name: "wave-2 leader blocked names first unbaked member",
			in: Input{Self: self("c", 2), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, ok, true)),
				m("prod", "b", 1, d(A, 11, BakeInProgress, false)),
				m("prod", "z", 1, d(A, 12, "", false))}},
			role: RoleLeader, leader: "c", reason: ReasonWaitingFor, msg: "prod/b bake on aaaaaaa", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "unknown status counts as not done; member without the revision too",
			in: Input{Self: self("c", 2), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, ok, true)),
				m("prod", "b", 1)}},
			role: RoleLeader, leader: "c", reason: ReasonWaitingFor, msg: "prod/b bake on aaaaaaa", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "deregistered member excluded from completion",
			in: Input{Self: self("c", 2), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, ok, true)),
				dereg(m("prod", "b", 1, d(A, 11, BakeFailed, false)))}},
			role: RoleLeader, leader: "c", allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			name: "deregistered leader mid-flight: followers keep selection",
			in: Input{Self: self("c", 1), Candidates: cands, Members: []Member{
				dereg(m("prod", "a", 1, d(B, 30, BakeInProgress, true))),
				m("prod", "c", 1)}},
			role: RoleLeader, leader: "c", selected: B, unrestr: true,
		},
		{
			name: "deregistered leader: other follower keeps following",
			in: Input{Self: self("d", 1), Candidates: cands, Members: []Member{
				dereg(m("prod", "a", 1, d(B, 30, BakeInProgress, true))),
				m("prod", "c", 1), m("prod", "d", 1)}},
			role: RoleFollower, leader: "c", selected: B, allowed: []string{"v2"}, checkAllowd: true,
		},
		{
			name: "pinned follower never bakes and blocks next wave",
			in: Input{Self: self("c", 2), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(B, 20, ok, true)),
				m("prod", "b", 1, d(A, 11, ok, false))}},
			role: RoleLeader, leader: "c", reason: ReasonWaitingFor, msg: "prod/b bake on bbbbbbb", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "gap in self env",
			in:   Input{Self: self("a", 1), Members: []Member{m("prod", "c", 3)}},
			role: RoleLeader, leader: "a", reason: ReasonInvalidTopology, msg: "prod has waves 1,3 (missing 2)", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "gap in self env blocks followers too",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 1, ok, true)), m("prod", "c", 3)}},
			role: RoleFollower, leader: "a", selected: A, reason: ReasonInvalidTopology, msg: "prod has waves 1,3 (missing 2)", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "gap in upstream",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Upstream: "staging"}, Members: []Member{
				m("staging", "s1", 1), m("staging", "s3", 3)}},
			role: RoleLeader, leader: "a", reason: ReasonInvalidTopology, msg: "staging has waves 1,3 (missing 2)", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "no upstream members at all",
			in:   Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Upstream: "staging"}},
			role: RoleLeader, leader: "a", reason: ReasonWaitingFor, msg: "staging has no registered sites", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "sited upstream: last wave only, newest first",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Upstream: "staging"}, Candidates: cands,
				Members: []Member{
					m("staging", "s1", 1, d(A, 1, ok, true), d(B, 5, BakeInProgress, true)),
					m("staging", "s2", 2, d(A, 3, ok, true), d(B, 6, ok, true), d(C, 9, ok, true)),
					m("staging", "s3", 2, d(A, 4, ok, false), d(B, 7, ok, false), d(C, 10, BakeFailed, false))}},
			role: RoleLeader, leader: "a", allowed: []string{"v2", "v1"}, checkAllowd: true,
		},
		{
			name: "sited upstream blocker names unbaked member of last wave",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Upstream: "staging"}, Candidates: cands,
				Members: []Member{
					m("staging", "s1", 1, d(A, 1, ok, true)),
					m("staging", "s2", 2, d(B, 6, ok, true)),
					m("staging", "s3", 2, d(B, 7, BakeInProgress, false))}},
			role: RoleLeader, leader: "a", reason: ReasonWaitingFor, msg: "staging/s3 bake on bbbbbbb", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "tag dedup and drop revisions without tag",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Upstream: "staging"},
				Candidates: map[string]string{A: "v1", B: "v1"},
				Members: []Member{
					m("staging", "s1", 1, d(A, 1, ok, true), d(B, 2, ok, true), d(C, 3, ok, true))}},
			role: RoleLeader, leader: "a", allowed: []string{"v1"}, checkAllowd: true,
		},
		{
			name: "lockstep waiting pins to current selection",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Lockstep: true}, Candidates: cands,
				Members: []Member{
					lock(m("prod", "a", 1, d(B, 20, ok, true))),
					lock(m("prod", "c", 2, d(B, 21, BakeInProgress, false)))}},
			role: RoleLeader, leader: "a", selected: B, allowed: []string{"v2"}, checkAllowd: true,
			reason: ReasonLockstepWaiting, msg: "prod/c bake on bbbbbbb", checkMsg: true,
		},
		{
			name: "lockstep waiting without tag: empty allowed",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Lockstep: true},
				Members: []Member{
					lock(m("prod", "a", 1, d(B, 20, ok, true))),
					lock(m("prod", "c", 2))}},
			role: RoleLeader, leader: "a", selected: B, allowed: []string{}, checkAllowd: true,
			reason: ReasonLockstepWaiting, msg: "prod/c bake on bbbbbbb", checkMsg: true,
		},
		{
			name: "lockstep released when whole env baked",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Lockstep: true}, Candidates: cands,
				Members: []Member{
					lock(m("prod", "a", 1, d(B, 20, ok, true))),
					lock(m("prod", "c", 2, d(B, 21, ok, false)))}},
			role: RoleLeader, leader: "a", selected: B, unrestr: true,
		},
		{
			name: "lockstep ignored for non-leader and wave 2",
			in: Input{Self: Self{Environment: "prod", Site: "c", Wave: 2, Lockstep: true}, Candidates: cands,
				Members: []Member{
					lock(m("prod", "a", 1, d(B, 20, ok, true))),
					lock(m("prod", "c", 2))}},
			role: RoleLeader, leader: "c", allowed: []string{"v2"}, checkAllowd: true,
		},
		{
			name: "lockstep mismatch (self is leader)",
			in: Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Lockstep: true},
				Members: []Member{m("prod", "a", 1), m("prod", "b", 1), m("prod", "c", 2), lock(m("prod", "d", 2))}},
			role: RoleLeader, leader: "a", unrestr: true, mismatch: true,
		},
		{
			name: "lockstep mismatch (leader is other member, self disagrees)",
			in: Input{Self: self("c", 2),
				Members: []Member{lock(m("prod", "a", 1))}},
			role: RoleLeader, leader: "c", mismatch: true, allowed: []string{}, checkAllowd: true,
			reason: ReasonWaitingFor, msg: "prod/a bake", checkMsg: true,
		},
		{
			name: "wave-2 blocker when wave 1 has no selection and no deployments",
			in:   Input{Self: self("c", 2), Members: []Member{m("prod", "a", 1)}},
			role: RoleLeader, leader: "c", reason: ReasonWaitingFor, msg: "prod/a bake", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "implicit selection on migration: follower follows leader's newest",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("prod", "a", 1, d(A, 10, ok, false), d(B, 12, BakeInProgress, false)),
				m("prod", "b", 1, d(A, 11, ok, false))}},
			role: RoleFollower, leader: "a", selected: B, allowed: []string{"v2"}, checkAllowd: true,
		},
		{
			name: "no selection and leader has no deployment: follower waits",
			in:   Input{Self: self("b", 1), Candidates: cands, Members: []Member{m("prod", "a", 1)}},
			role: RoleFollower, leader: "a", reason: ReasonWaitingFor, msg: "prod/a selection", checkMsg: true,
			allowed: []string{}, checkAllowd: true,
		},
		{
			name: "recorded wave of self ignored in favour of spec",
			in:   Input{Self: self("a", 1), Members: []Member{m("prod", "a", 5)}},
			role: RoleLeader, leader: "a", unrestr: true,
		},
		{
			name: "members of unrelated environments ignored",
			in: Input{Self: self("b", 1), Candidates: cands, Members: []Member{
				m("dev", "a", 1, d(A, 99, ok, true)), m("other", "a", 1, d(B, 1, ok, true))}},
			role: RoleLeader, leader: "b", unrestr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Decide(tc.in)
			if got.Role != tc.role {
				t.Errorf("role = %s, want %s", got.Role, tc.role)
			}
			if got.Leader != tc.leader {
				t.Errorf("leader = %q, want %q", got.Leader, tc.leader)
			}
			if got.SelectedRevision != tc.selected {
				t.Errorf("selected = %q, want %q", got.SelectedRevision, tc.selected)
			}
			if got.Unrestricted != tc.unrestr {
				t.Errorf("unrestricted = %v, want %v", got.Unrestricted, tc.unrestr)
			}
			if got.Unrestricted && got.AllowedTags != nil {
				t.Errorf("unrestricted but AllowedTags = %v", got.AllowedTags)
			}
			if !got.Unrestricted && got.AllowedTags == nil {
				t.Errorf("AllowedTags is nil while restricted")
			}
			if tc.checkAllowd && !reflect.DeepEqual(got.AllowedTags, tc.allowed) {
				t.Errorf("allowed = %#v, want %#v", got.AllowedTags, tc.allowed)
			}
			if tc.reason == "" {
				if got.Blocker != nil {
					t.Errorf("blocker = %+v, want nil", *got.Blocker)
				}
			} else if got.Blocker == nil || got.Blocker.Reason != tc.reason {
				t.Errorf("blocker = %+v, want reason %s", got.Blocker, tc.reason)
			} else if tc.checkMsg && got.Blocker.Message != tc.msg {
				t.Errorf("blocker message = %q, want %q", got.Blocker.Message, tc.msg)
			}
			if !got.Unrestricted && len(got.AllowedTags) == 0 && got.Blocker == nil {
				t.Errorf("empty allowed set without blocker")
			}
			if (got.Mismatch != nil) != tc.mismatch {
				t.Errorf("mismatch = %+v, want %v", got.Mismatch, tc.mismatch)
			}
			if got.Mismatch != nil && got.Mismatch.Reason != ReasonLockstepMismatch {
				t.Errorf("mismatch reason = %s", got.Mismatch.Reason)
			}
		})
	}
}

func TestMismatchMessage(t *testing.T) {
	got := Decide(Input{Self: Self{Environment: "prod", Site: "a", Wave: 1, Lockstep: true},
		Members: []Member{m("prod", "a", 1), m("prod", "b", 1), lock(m("prod", "c", 2)), m("prod", "d", 2)}})
	want := "wave-1 leader prod/a has lockstep=true; disagreeing sites: prod/b, prod/d"
	if got.Mismatch == nil || got.Mismatch.Message != want {
		t.Fatalf("mismatch = %+v, want %q", got.Mismatch, want)
	}
}

func TestNewestDeploymentOfRevisionDecidesBake(t *testing.T) {
	// A re-deploy of A that failed after an earlier success is not done.
	got := Decide(Input{Self: self("c", 2), Candidates: cands, Members: []Member{
		m("prod", "a", 1, d(A, 10, BakeSucceeded, true), d(A, 12, BakeFailed, true))}})
	if got.Blocker == nil || got.Blocker.Reason != ReasonWaitingFor {
		t.Fatalf("got %+v", got)
	}
}

func TestDeterminism(t *testing.T) {
	ok := BakeSucceeded
	members := []Member{
		m("prod", "a", 1, d(A, 10, ok, true), d(B, 20, ok, true)),
		m("prod", "b", 1, d(A, 11, ok, false), d(B, 21, ok, false), d(C, 30, ok, true)),
		m("prod", "c", 2, d(A, 12, ok, true)),
		m("staging", "s1", 1, d(A, 1, ok, true), d(B, 2, ok, true)),
		m("staging", "s2", 2, d(A, 3, ok, true), d(B, 4, ok, true)),
	}
	for _, s := range []Self{
		{Environment: "prod", Site: "c", Wave: 2, Upstream: "staging"},
		{Environment: "prod", Site: "a", Wave: 1, Upstream: "staging"},
		{Environment: "prod", Site: "b", Wave: 1, Upstream: "staging"},
	} {
		want := Decide(Input{Self: s, Members: members, Candidates: cands})
		r := rand.New(rand.NewSource(1))
		for i := 0; i < 50; i++ {
			sh := append([]Member(nil), members...)
			r.Shuffle(len(sh), func(i, j int) { sh[i], sh[j] = sh[j], sh[i] })
			for k := range sh {
				deps := append([]Deployment(nil), sh[k].Deployments...)
				r.Shuffle(len(deps), func(i, j int) { deps[i], deps[j] = deps[j], deps[i] })
				sh[k].Deployments = deps
			}
			if got := Decide(Input{Self: s, Members: sh, Candidates: cands}); !reflect.DeepEqual(got, want) {
				t.Fatalf("self %s: %+v != %+v", s.Site, got, want)
			}
		}
	}
}
