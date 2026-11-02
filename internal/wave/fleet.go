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
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// fleet is the members of one environment grouped by wave. Deregistered
// members are kept (their selection records still count).
type fleet struct {
	env    string
	byWave map[int32][]Member
}

// selfMember builds Self as a registered member of its spec wave, keeping the
// deployments recorded for it in the backend.
func selfMember(in Input) *Member {
	s := in.Self
	m := Member{Environment: s.Environment, Site: s.Site, Wave: s.Wave, Lockstep: s.Lockstep}
	for _, o := range in.Members {
		if o.Environment == s.Environment && o.Site == s.Site {
			m.Deployments = o.Deployments
			break
		}
	}
	return &m
}

func newFleet(env string, members []Member, self *Member) fleet {
	f := fleet{env: env, byWave: map[int32][]Member{}}
	seen := map[string]bool{}
	if self != nil {
		seen[self.Site] = true
		f.byWave[self.Wave] = append(f.byWave[self.Wave], *self)
	}
	for _, m := range members {
		if m.Environment != env || seen[m.Site] {
			continue
		}
		seen[m.Site] = true
		f.byWave[m.Wave] = append(f.byWave[m.Wave], m)
	}
	return f
}

func registered(ms []Member) []Member {
	var out []Member
	for _, m := range ms {
		if !m.Deregistered {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Site < out[j].Site })
	return out
}

// registeredWaves returns the sorted waves with at least one registered member.
func (f fleet) registeredWaves() []int32 {
	var ws []int32
	for w, ms := range f.byWave {
		if len(registered(ms)) > 0 {
			ws = append(ws, w)
		}
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i] < ws[j] })
	return ws
}

func (f fleet) lastWave() int32 {
	ws := f.registeredWaves()
	if len(ws) == 0 {
		return 0
	}
	return ws[len(ws)-1]
}

// topology returns InvalidTopology when the registered waves are not 1..max.
func (f fleet) topology() *Blocker {
	ws := f.registeredWaves()
	if len(ws) == 0 {
		return nil
	}
	var missing []string
	have := map[int32]bool{}
	var names []string
	for _, w := range ws {
		have[w] = true
		names = append(names, strconv.Itoa(int(w)))
	}
	if ws[0] < 1 {
		return &Blocker{Reason: ReasonInvalidTopology,
			Message: fmt.Sprintf("%s has waves %s (waves start at 1)", f.env, strings.Join(names, ","))}
	}
	for w := int32(1); w < ws[len(ws)-1]; w++ {
		if !have[w] {
			missing = append(missing, strconv.Itoa(int(w)))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return &Blocker{Reason: ReasonInvalidTopology,
		Message: fmt.Sprintf("%s has waves %s (missing %s)", f.env, strings.Join(names, ","), strings.Join(missing, ","))}
}

// bakeOf is the bake of a member's newest deployment of rev, and that
// deployment's Order. ok is false when the member never deployed rev.
func bakeOf(m Member, rev string) (bake string, order int64, ok bool) {
	for _, d := range m.Deployments {
		if d.Revision == rev && (!ok || d.Order > order) {
			bake, order, ok = d.Bake, d.Order, true
		}
	}
	return
}

func baked(m Member, rev string) bool {
	b, _, ok := bakeOf(m, rev)
	return ok && b == BakeSucceeded
}

func allBaked(ms []Member, rev string) bool {
	if len(ms) == 0 {
		return false
	}
	for _, m := range ms {
		if !baked(m, rev) {
			return false
		}
	}
	return true
}

// completedRevs maps every revision baked Succeeded by all of ms to the
// highest Order among the members' deployments of it.
func completedRevs(ms []Member) map[string]int64 {
	out := map[string]int64{}
	if len(ms) == 0 {
		return out
	}
	for _, d := range ms[0].Deployments {
		if _, done := out[d.Revision]; done || !allBaked(ms, d.Revision) {
			continue
		}
		var max int64
		for _, m := range ms {
			_, o, _ := bakeOf(m, d.Revision)
			if o > max {
				max = o
			}
		}
		out[d.Revision] = max
	}
	return out
}

// tagList maps revisions to tags (dropping those without one), newest first,
// ties by tag, no duplicates. Never nil.
func tagList(revs map[string]int64, cand map[string]string) []string {
	byTag := map[string]int64{}
	for r, o := range revs {
		t, ok := cand[r]
		if !ok || t == "" {
			continue
		}
		if cur, seen := byTag[t]; !seen || o > cur {
			byTag[t] = o
		}
	}
	out := make([]string, 0, len(byTag))
	for t := range byTag {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if byTag[out[i]] != byTag[out[j]] {
			return byTag[out[i]] > byTag[out[j]]
		}
		return out[i] < out[j]
	})
	return out
}

func short(rev string) string {
	if len(rev) > 7 {
		return rev[:7]
	}
	return rev
}

// waveState is the computed state of one (environment, wave).
type waveState struct {
	reg      []Member // registered, sorted by site
	leader   string
	selRev   string // current selection (explicit, else implicit)
	explicit bool
}

func (f fleet) state(w int32) waveState {
	all := f.byWave[w]
	st := waveState{reg: registered(all)}
	if len(st.reg) == 0 {
		return st
	}
	var selSite string
	var selOrder int64
	for _, m := range all {
		for _, d := range m.Deployments {
			if d.Selection && (!st.explicit || d.Order > selOrder) {
				st.explicit, selOrder, st.selRev, selSite = true, d.Order, d.Revision, m.Site
			}
		}
	}
	st.leader = st.reg[0].Site
	if st.explicit {
		for _, m := range st.reg {
			if m.Site == selSite && !allBaked(st.reg, st.selRev) {
				st.leader = selSite
			}
		}
		return st
	}
	// Implicit selection: the computed leader's newest deployment.
	var newest Deployment
	found := false
	for _, d := range st.reg[0].Deployments {
		if !found || d.Order > newest.Order {
			newest, found = d, true
		}
	}
	if found {
		st.selRev = newest.Revision
	}
	return st
}

// blocker names the first registered member (by site) that has not baked the
// wave's current selection.
func (st waveState) blocker(env string) *Blocker {
	for _, m := range st.reg {
		if st.selRev == "" {
			return &Blocker{Reason: ReasonWaitingFor, Message: env + "/" + m.Site + " bake"}
		}
		if !baked(m, st.selRev) {
			return &Blocker{Reason: ReasonWaitingFor, Message: env + "/" + m.Site + " bake on " + short(st.selRev)}
		}
	}
	if st.selRev != "" {
		// Everyone baked it; the revision just has no tag.
		return &Blocker{Reason: ReasonWaitingForCandidate, Message: short(st.selRev)}
	}
	return &Blocker{Reason: ReasonWaitingFor, Message: env + " has no registered sites"}
}

// lockstepBlocker returns LockstepWaiting while any registered member of any
// wave has not baked rev; members are ordered by (wave, site).
func (f fleet) lockstepBlocker(rev string) *Blocker {
	for _, w := range f.registeredWaves() {
		for _, m := range registered(f.byWave[w]) {
			if !baked(m, rev) {
				return &Blocker{Reason: ReasonLockstepWaiting, Message: f.env + "/" + m.Site + " bake on " + short(rev)}
			}
		}
	}
	return nil
}

// mismatch reports registered members whose Lockstep differs from the wave-1
// leader's.
func (f fleet) mismatch() *Blocker {
	st := f.state(1)
	if len(st.reg) == 0 {
		return nil
	}
	var want bool
	for _, m := range st.reg {
		if m.Site == st.leader {
			want = m.Lockstep
		}
	}
	var bad []string
	for _, w := range f.registeredWaves() {
		for _, m := range registered(f.byWave[w]) {
			if m.Lockstep != want {
				bad = append(bad, f.env+"/"+m.Site)
			}
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return &Blocker{Reason: ReasonLockstepMismatch,
		Message: fmt.Sprintf("wave-1 leader %s/%s has lockstep=%t; disagreeing sites: %s",
			f.env, st.leader, want, strings.Join(bad, ", "))}
}
