package wave

import (
	"reflect"
	"testing"
)

// The structured Blocker fields are the contract; messages are display text.
func TestBlockerStructuredFields(t *testing.T) {
	ok := BakeSucceeded
	pinSelf := self("b", 1)
	pinSelf.PinnedVersion = "v9"
	stg := Self{Environment: "prod", Upstream: "staging", Site: "p", Wave: 1}
	lockSelf := Self{Environment: "prod", Site: "a", Wave: 1, Lockstep: true}
	aliasSelf := Self{Environment: "staging", Alias: "staging", Site: "eu", Wave: 1}
	aStg, aPre := m("staging", "eu", 1, d(A, 1, ok, false)), m("staging", "us", 1, d(A, 2, ok, false))
	aStg.Alias, aPre.Alias = "staging", "preprod"
	upGap := []Member{m("staging", "s1", 1, d(A, 1, ok, true)), m("staging", "s3", 3, d(A, 2, ok, true))}

	tests := []struct {
		name string
		in   Input
		want Blocker // Message ignored
	}{
		{"no baked revision, sites exist", Input{Self: self("c", 2), Candidates: cands, Members: []Member{m("prod", "a", 1), m("prod", "c", 2)}},
			Blocker{Reason: ReasonWaitingFor, Environment: "prod", WaitingOn: WaitBake}},
		{"bake", Input{Self: self("c", 2), Candidates: cands, Members: []Member{
			m("prod", "a", 1, d(A, 10, ok, true)), m("prod", "b", 1, d(A, 11, BakeInProgress, false)), m("prod", "c", 2)}},
			Blocker{Reason: ReasonWaitingFor, Environment: "prod", Site: "b", WaitingOn: WaitBake, Revision: A}},
		{"upstream no baked revision", Input{Self: stg, Candidates: cands, Members: nil},
			Blocker{Reason: ReasonWaitingFor, Environment: "staging", WaitingOn: WaitSites}},
		{"upstream sites without baked revision", Input{Self: stg, Candidates: cands, Members: []Member{m("staging", "s1", 1)}},
			Blocker{Reason: ReasonWaitingFor, Environment: "staging", WaitingOn: WaitBake}},
		{"upstream bake", Input{Self: stg, Candidates: cands, Members: []Member{m("staging", "s1", 1, d(A, 1, BakeInProgress, true))}},
			Blocker{Reason: ReasonWaitingFor, Environment: "staging", Site: "s1", WaitingOn: WaitBake, Revision: A}},
		{"follower waits selection", Input{Self: self("b", 1), Candidates: cands, Members: []Member{m("prod", "a", 1), m("prod", "b", 1)}},
			Blocker{Reason: ReasonWaitingFor, Environment: "prod", Site: "a", WaitingOn: WaitSelection}},
		{"leader bake failed", Input{Self: self("b", 1), Candidates: cands, Members: []Member{
			m("prod", "a", 1, d(A, 10, BakeFailed, true)), m("prod", "b", 1)}},
			Blocker{Reason: ReasonWaitingFor, Environment: "prod", Site: "a", WaitingOn: WaitBake, Outcome: BakeFailed, Revision: A}},
		{"leader bake cancelled", Input{Self: self("b", 1), Candidates: cands, Members: []Member{
			m("prod", "a", 1, d(A, 10, BakeCancelled, true)), m("prod", "b", 1)}},
			Blocker{Reason: ReasonWaitingFor, Environment: "prod", Site: "a", WaitingOn: WaitBake, Outcome: BakeCancelled, Revision: A}},
		{"candidate (follower)", Input{Self: self("b", 1), Candidates: map[string]string{}, Members: []Member{
			m("prod", "a", 1, d(A, 10, ok, true)), m("prod", "b", 1)}},
			Blocker{Reason: ReasonWaitingForCandidate, Environment: "prod", WaitingOn: WaitCandidate, Revision: A}},
		{"pinned", Input{Self: pinSelf, Candidates: cands, Members: []Member{
			m("prod", "a", 1, d(A, 10, ok, true)), m("prod", "b", 1)}},
			Blocker{Reason: ReasonPinned, Environment: "prod", PinnedVersion: "v9", SelectedVersion: "v1", Revision: A}},
		{"gap", Input{Self: self("a", 1), Candidates: cands, Members: []Member{m("prod", "a", 1), m("prod", "c", 3)}},
			Blocker{Reason: ReasonInvalidTopology, Environment: "prod", Waves: []int32{1, 3}, MissingWaves: []int32{2}}},
		{"upstream gap", Input{Self: stg, Candidates: cands, Members: upGap},
			Blocker{Reason: ReasonInvalidTopology, Environment: "staging", Waves: []int32{1, 3}, MissingWaves: []int32{2}}},
		{"alias", Input{Self: aliasSelf, Candidates: cands, Members: []Member{aStg, aPre}},
			Blocker{Reason: ReasonInvalidTopology, Environment: "staging", Names: []string{"staging", "preprod"}}},
		{"self upstream", Input{Self: Self{Environment: "prod", Upstream: "prod", Site: "a", Wave: 1}, Candidates: cands},
			Blocker{Reason: ReasonInvalidTopology, Environment: "prod"}},
		{"lockstep", Input{Self: lockSelf, Candidates: cands, Members: []Member{
			lock(m("prod", "a", 1, d(A, 10, ok, true))), lock(m("prod", "b", 2))}},
			Blocker{Reason: ReasonLockstepWaiting, Environment: "prod", Site: "b", WaitingOn: WaitBake, Revision: A}},
		{"unknown tier", Input{Self: Self{Environment: "canary", Site: "a", Wave: 1}},
			Blocker{Reason: ReasonUnknownTier, Names: []string{"canary"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Decide(tc.in).Blocker
			if got == nil {
				t.Fatalf("no blocker")
			}
			g := *got
			if g.Message == "" {
				t.Errorf("empty message")
			}
			g.Message = ""
			if !reflect.DeepEqual(g, tc.want) {
				t.Errorf("blocker = %+v\nwant      %+v", g, tc.want)
			}
		})
	}
}
