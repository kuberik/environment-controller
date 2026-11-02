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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	"github.com/kuberik/environment-controller/internal/wave"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

const (
	// DeregisterFinalizer is set on sited Environments so that deleting one posts
	// the deregistration marker before the object disappears.
	DeregisterFinalizer = "environments.kuberik.com/deregister"
	// SkipDeregisterAnnotation, when "true", removes the finalizer without any
	// GitHub call (dead cluster, revoked token).
	SkipDeregisterAnnotation = "environments.kuberik.com/skip-deregister"

	// ConditionWaveReady and ConditionLockstepConsistent are only set on sited Environments.
	ConditionWaveReady          = "WaveReady"
	ConditionLockstepConsistent = "LockstepConsistent"

	deregisteredPrefix = "Deregistered"
	stateInactive      = "inactive"

	// Member-state reader bounds, see readMembers.
	maxDeploymentPages   = 10
	deploymentsPerPage   = 100
	deploymentsPerSite   = 20
	deregistrationMarker = "Deregistered: site %s removed"
)

// effectiveSite is spec.site, else the controller default. "" means site-less.
func (r *GitHubEnvironmentReconciler) effectiveSite(env *kuberikv1alpha1.Environment) string {
	if env.Spec.Site != "" {
		return env.Spec.Site
	}
	return r.DefaultSite
}

// effectiveWave is spec.wave, else 1 for sited Environments; 0 for site-less ones.
func (r *GitHubEnvironmentReconciler) effectiveWave(env *kuberikv1alpha1.Environment) int32 {
	if r.effectiveSite(env) == "" {
		return 0
	}
	if env.Spec.Wave != nil {
		return *env.Spec.Wave
	}
	return 1
}

// isWaveLeader reports whether the last published status.wave says this site is
// the leader of its wave. Stale status (another site) never counts.
func isWaveLeader(env *kuberikv1alpha1.Environment, site string) bool {
	w := env.Status.Wave
	return w != nil && w.Site == site && w.Role == kuberikv1alpha1.WaveRoleLeader
}

func isDeregisteredStatus(s *github.DeploymentStatus) bool {
	return s != nil && strings.HasPrefix(s.GetDescription(), deregisteredPrefix)
}

// isDeregistered reports whether a deployment's statuses (newest first) say its
// site is deregistered. GitHub's automatic stateInactive statuses (no description),
// which a later success in the same GitHub environment appends, do not bury the
// marker; any other newer status means the site registered again.
func isDeregistered(statuses []*github.DeploymentStatus) bool {
	for _, s := range statuses {
		if isDeregisteredStatus(s) {
			return true
		}
		if s.GetState() == stateInactive && s.GetDescription() == "" {
			continue
		}
		return false
	}
	return false
}

// ---- per-reconcile status cache -------------------------------------------

type reconcileCacheKey struct{}

// reconcileCache holds deployment statuses fetched during one reconcile so a
// deployment's statuses are fetched at most once (history building and the wave
// member reader share it). Posting a status invalidates the entry. A nil cache
// is valid and caches nothing.
type reconcileCache struct {
	statuses map[int64][]*github.DeploymentStatus
}

func withReconcileCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, reconcileCacheKey{}, &reconcileCache{statuses: map[int64][]*github.DeploymentStatus{}})
}

func reconcileCacheFrom(ctx context.Context) *reconcileCache {
	c, _ := ctx.Value(reconcileCacheKey{}).(*reconcileCache)
	return c
}

func (c *reconcileCache) get(id int64) ([]*github.DeploymentStatus, bool) {
	if c == nil {
		return nil, false
	}
	s, ok := c.statuses[id]
	return s, ok
}

func (c *reconcileCache) put(id int64, s []*github.DeploymentStatus) {
	if c != nil {
		c.statuses[id] = s
	}
}

func (c *reconcileCache) invalidate(id int64) {
	if c != nil {
		delete(c.statuses, id)
	}
}

// ---- status history display ------------------------------------------------

// ownSitesByEnv maps environment name -> the site of the Environment resource of
// that environment in this namespace (same spec.name). The current Environment is
// always present, even when site-less.
func (r *GitHubEnvironmentReconciler) ownSitesByEnv(current *kuberikv1alpha1.Environment, list *kuberikv1alpha1.EnvironmentList) map[string]string {
	own := map[string]string{}
	for i := range list.Items {
		e := &list.Items[i]
		if e.Spec.Name != current.Spec.Name || e.Spec.Environment == "" {
			continue
		}
		own[canonicalEnv(e.Spec.Environment)] = r.effectiveSite(e)
	}
	own[canonicalEnv(current.Spec.Environment)] = r.effectiveSite(current)
	return own
}

// displayDeployments narrows an environment's deployments to ONE site, because
// Rollout history IDs are per-cluster sequences and merging sites would collide
// them. Sites whose newest deployment's newest status is the deregistration
// marker are dead and ignored (one cached status fetch per site). Environments
// without any registered site show their site-less deployments untouched. An
// environment with a local Environment resource shows that resource's site; any
// other sited environment shows its lowest-wave registered site (ties: lowest
// name).
func (r *GitHubEnvironmentReconciler) displayDeployments(ctx context.Context, gh *github.Client, owner, repo, envName string, deps []*github.Deployment, ownSites map[string]string) []*github.Deployment {
	type siteInfo struct {
		wave int32
		id   int64
	}
	newest := map[string]siteInfo{}
	for _, d := range deps {
		p := r.extractDeploymentPayload(d)
		if p == nil || p.Site == "" {
			continue
		}
		if cur, ok := newest[p.Site]; !ok || d.GetID() > cur.id {
			newest[p.Site] = siteInfo{wave: p.Wave, id: d.GetID()}
		}
	}
	if len(newest) == 0 {
		return deps
	}
	sites := map[string]siteInfo{}
	for name, info := range newest {
		statuses, err := r.fetchAllDeploymentStatuses(ctx, gh, owner, repo, info.id)
		if err == nil && isDeregistered(statuses) {
			continue
		}
		sites[name] = info
	}
	site, ok := ownSites[envName]
	if !ok {
		names := make([]string, 0, len(sites))
		for n := range sites {
			names = append(names, n)
		}
		sort.Slice(names, func(i, j int) bool {
			a, b := sites[names[i]], sites[names[j]]
			if a.wave != b.wave {
				return a.wave < b.wave
			}
			return names[i] < names[j]
		})
		if len(names) > 0 {
			site = names[0]
		} // else: no registered site, show the site-less deployments
	}
	out := make([]*github.Deployment, 0, len(deps))
	for _, d := range deps {
		p := r.extractDeploymentPayload(d)
		ps := ""
		if p != nil {
			ps = p.Site
		}
		if ps == site {
			out = append(out, d)
		}
	}
	return out
}

// ---- member state reader ---------------------------------------------------

// listEnvDeployments lists deployments of one GitHub environment, newest first,
// paginated and capped at maxDeploymentPages x deploymentsPerPage. With
// stopIfSiteless it stops after the first page when that page has no sited
// deployment (site-less fleets pay one call).
func (r *GitHubEnvironmentReconciler) listEnvDeployments(ctx context.Context, gh *github.Client, owner, repo, task, formattedEnv string, stopIfSiteless bool) ([]*github.Deployment, error) {
	var all []*github.Deployment
	opts := &github.DeploymentsListOptions{
		Task:        task,
		Environment: formattedEnv,
		ListOptions: github.ListOptions{PerPage: deploymentsPerPage},
	}
	for page := 0; page < maxDeploymentPages; page++ {
		deps, resp, err := gh.Repositories.ListDeployments(ctx, owner, repo, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to list deployments for %s: %w", formattedEnv, err)
		}
		all = append(all, deps...)
		if resp.NextPage == 0 {
			break
		}
		if stopIfSiteless && page == 0 {
			sited := false
			for _, d := range all {
				if p := r.extractDeploymentPayload(d); p != nil && p.Site != "" {
					sited = true
					break
				}
			}
			if !sited {
				break
			}
		}
		opts.Page = resp.NextPage
	}
	return all, nil
}

// bakeOf reduces a deployment's statuses to the wave package's bake string.
func bakeOf(statuses []*github.DeploymentStatus) string {
	entry := kuberikrolloutv1alpha1.DeploymentHistoryEntry{}
	applyGitHubStatusToEntry(&entry, statuses)
	if entry.BakeStatus == nil {
		return ""
	}
	return *entry.BakeStatus
}

// listTierDeployments lists the deployments of every alias of a tier (the GitHub
// environment keeps the name written in spec, so "prod" and "production" are
// separate GitHub environments of one tier), newest first.
func (r *GitHubEnvironmentReconciler) listTierDeployments(ctx context.Context, gh *github.Client, owner, repo, task, deploymentName, tier string, stopIfSiteless bool) ([]*github.Deployment, error) {
	var all []*github.Deployment
	for _, alias := range wave.Aliases(tier) {
		deps, err := r.listEnvDeployments(ctx, gh, owner, repo, task, formatDeploymentEnvironment(deploymentName, alias), stopIfSiteless)
		if err != nil {
			return nil, err
		}
		all = append(all, deps...)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].GetID() > all[j].GetID() })
	return all, nil
}

// membersOf reduces the deployments of one tier to its sited members. Site-less
// deployments are ignored for wave logic.
//
// Call budget: one status fetch per deployment in the newest-deploymentsPerSite
// window of every site, shared with history building through the per-reconcile
// cache. Nothing is fetched per member beyond that window.
func (r *GitHubEnvironmentReconciler) membersOf(ctx context.Context, gh *github.Client, owner, repo, tier string, deps []*github.Deployment) ([]wave.Member, error) {
	var members []wave.Member
	bySite := map[string][]*github.Deployment{}
	for _, d := range deps {
		p := r.extractDeploymentPayload(d)
		if p == nil || p.Site == "" || p.Version == nil || p.Version.Revision == nil || *p.Version.Revision == "" {
			continue
		}
		bySite[p.Site] = append(bySite[p.Site], d)
	}
	siteNames := make([]string, 0, len(bySite))
	for s := range bySite {
		siteNames = append(siteNames, s)
	}
	sort.Strings(siteNames)
	for _, s := range siteNames {
		sd := bySite[s]
		sort.Slice(sd, func(i, j int) bool { return sd[i].GetID() > sd[j].GetID() })
		if len(sd) > deploymentsPerSite {
			sd = sd[:deploymentsPerSite]
		}
		m := wave.Member{Environment: tier, Site: s}
		for i, d := range sd {
			p := r.extractDeploymentPayload(d)
			statuses, err := r.fetchAllDeploymentStatuses(ctx, gh, owner, repo, d.GetID())
			if err != nil {
				return nil, fmt.Errorf("failed to read statuses of deployment %d: %w", d.GetID(), err)
			}
			if i == 0 { // newest deployment defines wave, lockstep and registration
				m.Wave = max(p.Wave, 1)
				m.Lockstep = p.Lockstep
				m.Deregistered = isDeregistered(statuses)
			}
			m.Deployments = append(m.Deployments, wave.Deployment{
				Revision:  *p.Version.Revision,
				Wave:      max(p.Wave, 1),
				Selection: p.Selection,
				Bake:      bakeOf(statuses),
				Order:     d.GetID(),
			})
		}
		members = append(members, m)
	}
	return members, nil
}

// readMembers reads the sited members of one tier from GitHub (all aliases).
func (r *GitHubEnvironmentReconciler) readMembers(ctx context.Context, gh *github.Client, owner, repo, deploymentName, tier string) ([]wave.Member, error) {
	deps, err := r.listTierDeployments(ctx, gh, owner, repo, formatDeploymentTask(deploymentName), deploymentName, tier, false)
	if err != nil {
		return nil, err
	}
	return r.membersOf(ctx, gh, owner, repo, tier, deps)
}

// findUpstream returns the nearest earlier tier of self that is present in
// GitHub for this deployment name: it has registered sited members (returned as
// members) or, failing that, site-less deployments (members empty). Empty tiers
// are skipped. "" means no earlier tier is present.
//
// Call budget: per earlier tier examined, one list call per alias (first page
// only when the tier is site-less), plus the status fetches of membersOf for
// sited tiers.
func (r *GitHubEnvironmentReconciler) findUpstream(ctx context.Context, gh *github.Client, owner, repo, deploymentName, self string) (string, []wave.Member, error) {
	task := formatDeploymentTask(deploymentName)
	for _, tier := range wave.EarlierTiers(self) {
		deps, err := r.listTierDeployments(ctx, gh, owner, repo, task, deploymentName, tier, true)
		if err != nil {
			return "", nil, err
		}
		members, err := r.membersOf(ctx, gh, owner, repo, tier, deps)
		if err != nil {
			return "", nil, err
		}
		if sitedRegistered(members, tier) {
			return tier, members, nil
		}
		for _, d := range deps {
			if p := r.extractDeploymentPayload(d); p != nil && p.Site == "" {
				return tier, nil, nil
			}
		}
	}
	return "", nil, nil
}

func newestDeployment(m wave.Member) (wave.Deployment, bool) {
	var best wave.Deployment
	found := false
	for _, d := range m.Deployments {
		if !found || d.Order > best.Order {
			best, found = d, true
		}
	}
	return best, found
}

// candidatesOf maps revisions to tags: the Rollout's release candidates plus
// the release it runs now. The rollout controller drops the running version
// from releaseCandidates, so without it a site that is up to date with its
// wave's selection would report WaitingForCandidate forever instead of Ready.
// Only the running release is added (not every available release) so the
// gate's allowedVersions stays short.
func candidatesOf(rollout *kuberikrolloutv1alpha1.Rollout) map[string]string {
	c := map[string]string{}
	if h := rollout.Status.History; len(h) > 0 && h[0].Version.Revision != nil &&
		*h[0].Version.Revision != "" && h[0].Version.Tag != "" {
		c[*h[0].Version.Revision] = h[0].Version.Tag
	}
	for _, cand := range rollout.Status.ReleaseCandidates {
		if cand.Revision != nil && *cand.Revision != "" && cand.Tag != "" {
			c[*cand.Revision] = cand.Tag
		}
	}
	return c
}

// currentRevision is the revision of the Rollout's newest history entry.
func currentRevision(rollout *kuberikrolloutv1alpha1.Rollout) string {
	if len(rollout.Status.History) == 0 || rollout.Status.History[0].Version.Revision == nil {
		return ""
	}
	return *rollout.Status.History[0].Version.Revision
}

func sitedRegistered(members []wave.Member, env string) bool {
	for _, m := range members {
		if m.Environment == env && m.Site != "" && !m.Deregistered {
			return true
		}
	}
	return false
}

// upstreamBakedRevisions is today's single-stream logic input: revisions the
// (site-less) related environment baked Succeeded, from status.environmentInfos.
func upstreamBakedRevisions(env *kuberikv1alpha1.Environment, upstream string) []string {
	var out []string
	for _, info := range env.Status.EnvironmentInfos {
		if info.Environment != upstream {
			continue
		}
		for _, e := range info.History {
			if e.Version.Revision != nil && *e.Version.Revision != "" && e.BakeStatus != nil && *e.BakeStatus == kuberikrolloutv1alpha1.BakeStatusSucceeded {
				out = append(out, *e.Version.Revision)
			}
		}
	}
	return out
}

// ---- wave wiring -----------------------------------------------------------

// reconcileWave decides Self's role and gate from GitHub state, publishes
// status.wave and the conditions, and only then updates the RolloutGate.
func (r *GitHubEnvironmentReconciler) reconcileWave(ctx context.Context, gh *github.Client, env *kuberikv1alpha1.Environment, rollout *kuberikrolloutv1alpha1.Rollout) error {
	owner, repo, err := parseProject(env.Spec.Backend.Project)
	if err != nil {
		return err
	}
	deploymentName := ensureKuberikPrefix(env.Spec.Name)
	self := canonicalEnv(env.Spec.Environment)
	upstream, upMembers, err := r.findUpstream(ctx, gh, owner, repo, deploymentName, self)
	if err != nil {
		return err
	}
	members, err := r.readMembers(ctx, gh, owner, repo, deploymentName, self)
	if err != nil {
		return err
	}
	members = append(members, upMembers...)

	site := r.effectiveSite(env)
	pinned := ""
	if rollout.Spec.WantedVersion != nil {
		pinned = *rollout.Spec.WantedVersion
	}
	in := wave.Input{
		Self: wave.Self{
			Environment:   self,
			Site:          site,
			Wave:          r.effectiveWave(env),
			Lockstep:      env.Spec.Lockstep,
			Upstream:      upstream,
			PinnedVersion: pinned,
		},
		Members:    members,
		Candidates: candidatesOf(rollout),
		Current:    currentRevision(rollout),
	}
	if upstream != "" && !sitedRegistered(members, upstream) {
		in.UpstreamSiteless = true
		in.UpstreamBaked = upstreamBakedRevisions(env, upstream)
	}
	dec := wave.Decide(in)

	if err := r.applyWaveStatus(ctx, env, in, dec); err != nil {
		return err
	}

	var allowed *[]string
	if !dec.Unrestricted {
		tags := dec.AllowedTags
		if tags == nil {
			tags = []string{}
		}
		allowed = &tags
	}
	return r.setGateAllowedVersions(ctx, env, allowed)
}

// applyWaveStatus writes status.wave and the WaveReady / LockstepConsistent
// conditions when they changed.
func (r *GitHubEnvironmentReconciler) applyWaveStatus(ctx context.Context, env *kuberikv1alpha1.Environment, in wave.Input, dec wave.Decision) error {
	ws := &kuberikv1alpha1.WaveStatus{
		Site:             in.Self.Site,
		Environment:      env.Spec.Environment,
		Name:             ensureKuberikPrefix(env.Spec.Name),
		Wave:             in.Self.Wave,
		Role:             kuberikv1alpha1.WaveRole(dec.Role),
		Leader:           dec.Leader,
		SelectedRevision: dec.SelectedRevision,
	}
	for _, m := range in.Members {
		ms := kuberikv1alpha1.WaveMemberStatus{Environment: m.Environment, Site: m.Site, Wave: m.Wave, Deregistered: m.Deregistered}
		if d, ok := newestDeployment(m); ok {
			ms.Revision, ms.BakeStatus = d.Revision, d.Bake
		}
		ws.Members = append(ws.Members, ms)
	}
	sort.SliceStable(ws.Members, func(i, j int) bool {
		a, b := ws.Members[i], ws.Members[j]
		if a.Environment != b.Environment {
			return a.Environment < b.Environment
		}
		if a.Wave != b.Wave {
			return a.Wave < b.Wave
		}
		return a.Site < b.Site
	})

	before := env.Status.DeepCopy()
	env.Status.Wave = ws

	ready := metav1.Condition{Type: ConditionWaveReady, Status: metav1.ConditionTrue, Reason: wave.ReasonReady, Message: "site may deploy the allowed versions", ObservedGeneration: env.Generation}
	if dec.Blocker != nil {
		ready.Status, ready.Reason, ready.Message = metav1.ConditionFalse, dec.Blocker.Reason, dec.Blocker.Message
	}
	meta.SetStatusCondition(&env.Status.Conditions, ready)

	lock := metav1.Condition{Type: ConditionLockstepConsistent, Status: metav1.ConditionTrue, Reason: "Consistent", Message: "all members agree on lockstep", ObservedGeneration: env.Generation}
	if dec.Mismatch != nil {
		lock.Status, lock.Reason, lock.Message = metav1.ConditionFalse, wave.ReasonLockstepMismatch, dec.Mismatch.Message
	}
	meta.SetStatusCondition(&env.Status.Conditions, lock)

	if equality.Semantic.DeepEqual(before.Wave, env.Status.Wave) && equality.Semantic.DeepEqual(before.Conditions, env.Status.Conditions) {
		return nil
	}
	return r.Status().Update(ctx, env)
}

// setGateAllowedVersions sets RolloutGate.spec.allowedVersions (nil = unrestricted).
func (r *GitHubEnvironmentReconciler) setGateAllowedVersions(ctx context.Context, env *kuberikv1alpha1.Environment, allowed *[]string) error {
	if env.Status.RolloutGateRef == nil {
		return nil
	}
	gate := &kuberikrolloutv1alpha1.RolloutGate{}
	if err := r.Get(ctx, types.NamespacedName{Name: env.Status.RolloutGateRef.Name, Namespace: env.Namespace}, gate); err != nil {
		return fmt.Errorf("failed to get RolloutGate: %w", err)
	}
	cur := gate.Spec.AllowedVersions
	if (cur == nil) == (allowed == nil) && (cur == nil || r.slicesEqual(*cur, *allowed)) {
		return nil
	}
	gate.Spec.AllowedVersions = allowed
	if err := r.Update(ctx, gate); err != nil {
		return fmt.Errorf("failed to update RolloutGate allowedVersions: %w", err)
	}
	return nil
}

// updateAllowedVersions is the gate update for site-less Environments: the
// nearest earlier tier that is present decides. A sited upstream uses
// wave.UpstreamComplete; a site-less one passes the revisions it baked
// Succeeded. With no earlier tier the gate is unrestricted.
func (r *GitHubEnvironmentReconciler) updateAllowedVersions(ctx context.Context, gh *github.Client, env *kuberikv1alpha1.Environment, rollout *kuberikrolloutv1alpha1.Rollout) error {
	if env.Status.RolloutGateRef == nil {
		return nil
	}
	owner, repo, err := parseProject(env.Spec.Backend.Project)
	if err != nil {
		return err
	}
	upstream, members, err := r.findUpstream(ctx, gh, owner, repo, ensureKuberikPrefix(env.Spec.Name), canonicalEnv(env.Spec.Environment))
	if err != nil {
		return err
	}
	if upstream == "" {
		return r.setGateAllowedVersions(ctx, env, nil)
	}
	if !sitedRegistered(members, upstream) {
		tags := siteLessGateTags(env, upstream, rollout)
		return r.setGateAllowedVersions(ctx, env, &tags)
	}
	revs, blocker := wave.UpstreamComplete(upstream, members)
	tags := []string{}
	if blocker == nil {
		cands := candidatesOf(rollout)
		seen := map[string]bool{}
		for _, rev := range revs {
			if t, ok := cands[rev]; ok && !seen[t] {
				seen[t] = true
				tags = append(tags, t)
			}
		}
	} else {
		log.FromContext(ctx).Info("Upstream waves not usable", "upstream", upstream, "reason", blocker.Reason, "message", blocker.Message)
	}
	return r.setGateAllowedVersions(ctx, env, &tags)
}

// ---- finalizer / deregistration -------------------------------------------

func (r *GitHubEnvironmentReconciler) event(env *kuberikv1alpha1.Environment, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(env, eventType, reason, msg)
	}
}

// regIdentity is what a sited Environment wrote to GitHub: the deployment name
// (with the kuberik prefix), the environment and the site. Deregistration must
// use the identity that was written, not whatever the spec says now.
type regIdentity struct {
	name, environment, site string
}

func (r *GitHubEnvironmentReconciler) currentIdentity(env *kuberikv1alpha1.Environment) regIdentity {
	return regIdentity{name: ensureKuberikPrefix(env.Spec.Name), environment: env.Spec.Environment, site: r.effectiveSite(env)}
}

// recordedIdentity is the identity persisted in status.wave. Fields missing from
// older status fall back to the current spec. ok is false when nothing was
// recorded.
func (r *GitHubEnvironmentReconciler) recordedIdentity(env *kuberikv1alpha1.Environment) (regIdentity, bool) {
	w := env.Status.Wave
	if w == nil || w.Site == "" {
		return regIdentity{}, false
	}
	id := r.currentIdentity(env)
	id.site = w.Site
	if w.Environment != "" {
		id.environment = w.Environment
	}
	if w.Name != "" {
		id.name = w.Name
	}
	return id, true
}

// deregisterSite posts the deregistration marker on the newest deployment of
// (name, environment, site). Idempotent: nothing is posted when the marker is
// already newest or the site never deployed. The marker is always posted with
// auto_inactive=false, whatever the Environment's site is now.
func (r *GitHubEnvironmentReconciler) deregisterSite(ctx context.Context, gh *github.Client, env *kuberikv1alpha1.Environment, id regIdentity) error {
	owner, repo, err := parseProject(env.Spec.Backend.Project)
	if err != nil {
		return err
	}
	task := formatDeploymentTask(id.name)
	formattedEnv := formatDeploymentEnvironment(id.name, id.environment)

	opts := &github.DeploymentsListOptions{Task: task, Environment: formattedEnv, ListOptions: github.ListOptions{PerPage: deploymentsPerPage}}
	var newest *github.Deployment
	for page := 0; page < maxDeploymentPages && newest == nil; page++ {
		deps, resp, err := gh.Repositories.ListDeployments(ctx, owner, repo, opts)
		if err != nil {
			return fmt.Errorf("failed to list deployments: %w", err)
		}
		for _, d := range deps { // newest first
			if p := r.extractDeploymentPayload(d); p != nil && p.Site == id.site {
				newest = d
				break
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	if newest == nil {
		return nil
	}
	statuses, err := r.fetchAllDeploymentStatuses(ctx, gh, owner, repo, newest.GetID())
	if err != nil {
		return fmt.Errorf("failed to read statuses of deployment %d: %w", newest.GetID(), err)
	}
	if isDeregistered(statuses) {
		return nil
	}
	reconcileCacheFrom(ctx).invalidate(newest.GetID())
	_, _, err = gh.Repositories.CreateDeploymentStatus(ctx, owner, repo, newest.GetID(), &github.DeploymentStatusRequest{
		State:        github.String(stateInactive),
		Description:  github.String(fmt.Sprintf(deregistrationMarker, id.site)),
		Environment:  &formattedEnv,
		AutoInactive: github.Bool(false),
	})
	if err != nil {
		return fmt.Errorf("failed to create deployment status: %w", err)
	}
	return nil
}

func (r *GitHubEnvironmentReconciler) removeFinalizer(ctx context.Context, env *kuberikv1alpha1.Environment) error {
	if !controllerutil.ContainsFinalizer(env, DeregisterFinalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(env, DeregisterFinalizer)
	if err := r.Update(ctx, env); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// reconcileDelete posts the deregistration marker (unless bypassed) and releases
// the finalizer.
func (r *GitHubEnvironmentReconciler) reconcileDelete(ctx context.Context, env *kuberikv1alpha1.Environment) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(env, DeregisterFinalizer) {
		return ctrl.Result{}, nil
	}
	if env.Annotations[SkipDeregisterAnnotation] == "true" {
		lg.Info("Skipping deregistration (annotation)")
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}
	id := r.currentIdentity(env)
	if rec, ok := r.recordedIdentity(env); ok {
		id = rec // what was actually written to GitHub
	}
	site := id.site
	if site == "" || env.Spec.Backend.Type != backendTypeGitHub {
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}
	gh, err := r.getGitHubClient(ctx, env)
	if err != nil {
		if apierrors.IsNotFound(err) && r.namespaceTerminating(ctx, env.Namespace) {
			msg := "namespace is terminating and the GitHub token Secret is gone; giving up deregistration of site " + site
			lg.Info(msg)
			r.event(env, corev1.EventTypeWarning, "DeregisterSkipped", msg)
			return ctrl.Result{}, r.removeFinalizer(ctx, env)
		}
		return ctrl.Result{}, err
	}
	if err := r.deregisterSite(ctx, gh, env, id); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.removeFinalizer(ctx, env)
}

func (r *GitHubEnvironmentReconciler) namespaceTerminating(ctx context.Context, name string) bool {
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, ns); err != nil {
		return apierrors.IsNotFound(err)
	}
	return ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating
}

// reconcileSiteChange handles the finalizer and registration identity before any
// GitHub write:
//   - sited: when the recorded (name, environment, site) differs from the
//     current one, deregister the OLD identity first, then persist the new one in
//     status.wave. The identity is persisted only when it is new.
//   - site-less but still carrying the finalizer or a recorded site (site
//     removed): deregister the recorded identity, release the finalizer and drop
//     the wave status and conditions.
func (r *GitHubEnvironmentReconciler) reconcileSiteChange(ctx context.Context, gh *github.Client, env *kuberikv1alpha1.Environment) error {
	cur := r.currentIdentity(env)
	old, hadOld := r.recordedIdentity(env)
	if hadOld && old != cur {
		if err := r.deregisterSite(ctx, gh, env, old); err != nil {
			return err
		}
	}
	if cur.site != "" {
		w := env.Status.Wave
		if w != nil && w.Site == cur.site && w.Environment == cur.environment && w.Name == cur.name {
			return nil
		}
		if hadOld && old == cur {
			// recorded by an older version without the identity fields: keep the rest
			w.Environment, w.Name = cur.environment, cur.name
		} else {
			env.Status.Wave = &kuberikv1alpha1.WaveStatus{Site: cur.site, Environment: cur.environment, Name: cur.name, Wave: r.effectiveWave(env)}
		}
		return r.Status().Update(ctx, env)
	}
	if env.Status.Wave != nil || controllerutil.ContainsFinalizer(env, DeregisterFinalizer) {
		env.Status.Wave = nil
		meta.RemoveStatusCondition(&env.Status.Conditions, ConditionWaveReady)
		meta.RemoveStatusCondition(&env.Status.Conditions, ConditionLockstepConsistent)
		if err := r.Status().Update(ctx, env); err != nil {
			return err
		}
	}
	return r.removeFinalizer(ctx, env)
}
