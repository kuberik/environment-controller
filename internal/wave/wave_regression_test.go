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
	"reflect"
	"testing"
)

// Regression tests for the adversarial review of the first implementation.

func dw(rev string, order int64, bake string, sel bool, wave int32) Deployment {
	x := d(rev, order, bake, sel)
	x.Wave = wave
	return x
}

// A site that moved from wave 1 to wave 2 must not carry its wave-1
// selection into wave 2.
func TestWaveReassignmentDoesNotCarrySelection(t *testing.T) {
	ok, ip := BakeSucceeded, BakeInProgress
	members := []Member{
		m("prod", "a", 2, dw(A, 10, ok, true, 1), dw(B, 30, ip, true, 1)),
		m("prod", "b", 1, dw(A, 11, ok, false, 1)),
		m("prod", "c", 2, dw(A, 12, ok, true, 2)),
	}
	got := Decide(Input{Self: self("c", 2), Members: members, Candidates: cands})
	if got.SelectedRevision != A || !reflect.DeepEqual(got.AllowedTags, []string{"v1"}) {
		t.Fatalf("wave-2 picked up wave-1 selection: %+v", got)
	}
	// From the moving site's own side (still recorded as wave 1).
	members[0].Wave = 1
	got = Decide(Input{Self: self("a", 2), Members: members, Candidates: cands})
	if got.SelectedRevision == B {
		t.Fatalf("moving site carried its wave-1 pick into wave 2: %+v", got)
	}
	for _, rev := range got.AllowedTags {
		if rev == "v2" {
			t.Fatalf("wave 2 allowed v2 before wave 1 baked it: %+v", got)
		}
	}
	// The wave-1 selection by a still governs wave 1: b follows B.
	got = Decide(Input{Self: self("b", 1), Members: members, Candidates: cands})
	if got.SelectedRevision != B {
		t.Fatalf("wave-1 selection lost: %+v", got)
	}
}

// A Self the backend does not see yet must not out-rank registered members:
// with the same Members, at most one site may compute itself leader.
func TestUnseenSelfDoesNotLead(t *testing.T) {
	ok, ip := BakeSucceeded, BakeInProgress
	cases := []struct {
		name    string
		members []Member
		sites   []string
	}{
		{"fresh newcomer", []Member{m("prod", "b", 1, d(A, 10, ok, false))}, []string{"a", "b"}},
		{"deregistered leader mid-flight", []Member{
			dereg(m("prod", "a", 1, d(B, 30, ip, true))),
			m("prod", "c", 1, d(A, 5, ok, false)),
		}, []string{"b", "c"}},
		{"re-registration", []Member{
			dereg(m("prod", "a", 1, d(A, 10, ok, false))),
			m("prod", "b", 1, d(A, 11, ok, true)),
		}, []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leaders := 0
			for _, s := range tc.sites {
				if Decide(Input{Self: self(s, 1), Members: tc.members, Candidates: cands}).Role == RoleLeader {
					leaders++
				}
			}
			if leaders != 1 {
				t.Fatalf("%d leaders, want 1", leaders)
			}
		})
	}
	// Deregistered leader mid-flight: followers keep its selection.
	got := Decide(Input{Self: self("b", 1), Members: cases[1].members, Candidates: cands})
	if got.Role != RoleFollower || got.Leader != "c" || got.SelectedRevision != B {
		t.Fatalf("newcomer: %+v", got)
	}
}

// An implicit selection in wave N>1 counts only once wave N-1 completed it.
func TestImplicitSelectionGatedInLaterWaves(t *testing.T) {
	ok := BakeSucceeded
	members := []Member{
		m("prod", "a", 1, d(A, 10, ok, true)),
		m("prod", "c", 2, d(B, 12, ok, false)),
		m("prod", "d", 2),
	}
	got := Decide(Input{Self: self("d", 2), Members: members, Candidates: cands})
	if got.Role != RoleFollower || len(got.AllowedTags) != 0 || got.Blocker == nil {
		t.Fatalf("ungated implicit selection leaked: %+v", got)
	}
	// Once wave 1 completed B, the implicit selection counts.
	members[0].Deployments = append(members[0].Deployments, d(B, 20, ok, true))
	got = Decide(Input{Self: self("d", 2), Members: members, Candidates: cands})
	if !reflect.DeepEqual(got.AllowedTags, []string{"v2"}) {
		t.Fatalf("implicit selection after completion: %+v", got)
	}
}

// Duplicate deployment Orders resolve conservatively regardless of order.
func TestDuplicateOrderIsConservative(t *testing.T) {
	ok, ip := BakeSucceeded, BakeInProgress
	for _, deps := range [][]Deployment{
		{d(A, 11, ok, false), d(A, 11, ip, false)},
		{d(A, 11, ip, false), d(A, 11, ok, false)},
	} {
		members := []Member{
			m("prod", "a", 1, d(A, 10, ok, true)),
			m("prod", "b", 1, deps...),
			m("prod", "c", 2),
		}
		got := Decide(Input{Self: self("c", 2), Members: members, Candidates: cands})
		if len(got.AllowedTags) != 0 {
			t.Fatalf("duplicate Succeeded copy won: %+v", got)
		}
	}
}

func TestSelfUpstreamIsInvalid(t *testing.T) {
	s := self("a", 1)
	s.Upstream = "prod"
	got := Decide(Input{Self: s, Candidates: cands})
	if got.Unrestricted || got.Blocker == nil || got.Blocker.Reason != ReasonInvalidTopology {
		t.Fatalf("self-upstream must block: %+v", got)
	}
}
