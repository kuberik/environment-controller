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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	"github.com/kuberik/environment-controller/internal/wave"
	releasev1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

const (
	// DeregisterFinalizer is set on every Environment so that deleting one posts
	// the deregistration marker before the object disappears.
	DeregisterFinalizer = "environments.kuberik.com/deregister"
	// SkipDeregisterAnnotation, when "true", removes the finalizer without any
	// GitHub call (dead cluster, revoked token).
	SkipDeregisterAnnotation = "environments.kuberik.com/skip-deregister"
	annotationTrue           = "true"
	// SiteTakeOverAnnotation, when "true" on an Environment in DuplicateSite
	// conflict, deregisters the conflicting owners and takes the site.
	SiteTakeOverAnnotation = "environments.kuberik.com/take-over-site"

	// DefaultSiteName is the site of an Environment that sets no spec.site on a
	// controller without a default site. Deployments in GitHub whose payload
	// has no site (written before sites existed) are read as this site, wave 1.
	DefaultSiteName = "default"

	ConditionWaveReady          = "WaveReady"
	ConditionLockstepConsistent = "LockstepConsistent"
	// ConditionDeprecated is True while the deprecated spec.relationship is set.
	ConditionDeprecated = "Deprecated"

	// ReasonDuplicateSite is the WaveReady reason of an Environment whose
	// (name, tier, site) is owned by another Environment.
	ReasonDuplicateSite = "DuplicateSite"
	// ReasonRelationshipIgnored is the reason of the Deprecated condition.
	ReasonRelationshipIgnored  = "RelationshipIgnored"
	messageRelationshipIgnored = "spec.relationship is ignored; environment order is fixed (dev, test, staging, prod)"

	deregisteredPrefix = "Deregistered"
	stateInactive      = "inactive"

	// Member-state reader bounds, see readMembers.
	maxDeploymentPages   = 10
	deploymentsPerPage   = 100
	deploymentsPerSite   = 20
	deregistrationMarker = "Deregistered: site %s removed"
	takeOverMarker       = "Deregistered: site %s taken over by %s/%s/%s"
)

// effectiveSite is spec.site, else the controller default site, else
// DefaultSiteName. Never empty.
func (r *GitHubEnvironmentReconciler) effectiveSite(env *kuberikv1alpha1.Environment) string {
	if env.Spec.Site != "" {
		return env.Spec.Site
	}
	if r.DefaultSite != "" {
		return r.DefaultSite
	}
	return DefaultSiteName
}

// effectiveWave is spec.wave, else 1.
func (r *GitHubEnvironmentReconciler) effectiveWave(env *kuberikv1alpha1.Environment) int32 {
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
	// lists holds deployment lists by (task, GitHub environment).
	lists map[string][]*github.Deployment
}

func listCacheKey(task, formattedEnv string) string { return task + "\x00" + formattedEnv }

func (c *reconcileCache) getList(key string) ([]*github.Deployment, bool) {
	if c == nil {
		return nil, false
	}
	l, ok := c.lists[key]
	return l, ok
}

func (c *reconcileCache) putList(key string, l []*github.Deployment) {
	if c != nil {
		c.lists[key] = l
	}
}

func (c *reconcileCache) invalidateList(key string) {
	if c != nil {
		delete(c.lists, key)
	}
}

func withReconcileCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, reconcileCacheKey{}, &reconcileCache{statuses: map[int64][]*github.DeploymentStatus{}, lists: map[string][]*github.Deployment{}})
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
// always present.
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
// marker are dead and ignored (one cached status fetch per site). An
// environment with a local Environment resource shows that resource's site; any
// other environment shows its lowest-wave registered site (ties: lowest name),
// or nothing when every site is deregistered.
func (r *GitHubEnvironmentReconciler) displayDeployments(ctx context.Context, gh *github.Client, owner, repo, envName string, deps []*github.Deployment, ownSites map[string]string) []*github.Deployment {
	type siteInfo struct {
		wave int32
		id   int64
	}
	newest := map[string]siteInfo{}
	for _, d := range deps {
		p := r.extractDeploymentPayload(d)
		if p == nil {
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
		if len(names) == 0 {
			return []*github.Deployment{}
		}
		site = names[0]
	}
	out := make([]*github.Deployment, 0, len(deps))
	for _, d := range deps {
		if p := r.extractDeploymentPayload(d); p != nil && p.Site == site {
			out = append(out, d)
		}
	}
	return out
}

// ---- member state reader ---------------------------------------------------

// listEnvDeployments lists deployments of one GitHub environment, newest first,
// paginated and capped at maxDeploymentPages x deploymentsPerPage.
func (r *GitHubEnvironmentReconciler) listEnvDeployments(ctx context.Context, gh *github.Client, owner, repo, task, formattedEnv string) ([]*github.Deployment, error) {
	cache := reconcileCacheFrom(ctx)
	key := listCacheKey(task, formattedEnv)
	if l, ok := cache.getList(key); ok {
		return l, nil
	}
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
		opts.Page = resp.NextPage
	}
	cache.putList(key, all)
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
func (r *GitHubEnvironmentReconciler) listTierDeployments(ctx context.Context, gh *github.Client, owner, repo, task, deploymentName, tier string) ([]*github.Deployment, error) {
	var all []*github.Deployment
	for _, alias := range wave.Aliases(tier) {
		deps, err := r.listEnvDeployments(ctx, gh, owner, repo, task, formatDeploymentEnvironment(deploymentName, alias))
		if err != nil {
			return nil, err
		}
		all = append(all, deps...)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].GetID() > all[j].GetID() })
	return all, nil
}

// membersOf reduces the deployments of one tier to its members, one per site.
//
// Call budget: one status fetch per deployment in the newest-deploymentsPerSite
// window of every site, shared with history building through the per-reconcile
// cache. Nothing is fetched per member beyond that window.
func (r *GitHubEnvironmentReconciler) membersOf(ctx context.Context, gh *github.Client, owner, repo, tier string, deps []*github.Deployment) ([]wave.Member, error) {
	var members []wave.Member
	bySite := map[string][]*github.Deployment{}
	for _, d := range deps {
		p := r.extractDeploymentPayload(d)
		if p == nil || p.Version == nil || p.Version.Revision == nil || *p.Version.Revision == "" {
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
		alias := sd[0].GetEnvironment()
		alias = alias[strings.LastIndex(alias, "/")+1:]
		m := wave.Member{Environment: tier, Alias: alias, Site: s}
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

// readMembers reads the members of one tier from GitHub (all aliases).
func (r *GitHubEnvironmentReconciler) readMembers(ctx context.Context, gh *github.Client, owner, repo, deploymentName, tier string) ([]wave.Member, error) {
	deps, err := r.listTierDeployments(ctx, gh, owner, repo, formatDeploymentTask(deploymentName), deploymentName, tier)
	if err != nil {
		return nil, err
	}
	return r.membersOf(ctx, gh, owner, repo, tier, deps)
}

// findUpstream returns the nearest earlier tier of self that is present in
// GitHub for this deployment name, i.e. has registered members (returned).
// Tiers without any registered member are skipped. "" means no earlier tier is
// present.
//
// Call budget: per earlier tier examined, the list calls of every alias plus the
// status fetches of membersOf.
func (r *GitHubEnvironmentReconciler) findUpstream(ctx context.Context, gh *github.Client, owner, repo, deploymentName, self string) (string, []wave.Member, error) {
	task := formatDeploymentTask(deploymentName)
	for _, tier := range wave.EarlierTiers(self) {
		deps, err := r.listTierDeployments(ctx, gh, owner, repo, task, deploymentName, tier)
		if err != nil {
			return "", nil, err
		}
		members, err := r.membersOf(ctx, gh, owner, repo, tier, deps)
		if err != nil {
			return "", nil, err
		}
		if anyRegistered(members, tier) {
			return tier, members, nil
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

// ReasonCandidatesUnresolved is the WaveReady reason when the release catalogue
// cannot be read, so revisions cannot be mapped to tags.
const ReasonCandidatesUnresolved = "CandidatesUnresolved"

// errCatalogueUnavailable means the ReleaseSlice kind is not served.
var errCatalogueUnavailable = errors.New("ReleaseSlice CRD is not served on this cluster")

// candidatesOf maps revisions to tags: status.candidates (gated and blocked
// alike) plus the release the Rollout runs now. status.candidates carries tags
// only, so revisions are resolved from the ReleaseSlice catalogue of
// status.releaseSource. The old releaseCandidates, availableReleases and
// gatedReleaseCandidates lists are not read.
//
// Every candidate is mapped, not only the gated ones: the map is a translation
// table from a GitHub revision to a tag, and the gate (allowedVersions) is what
// decides eligibility. The old releaseCandidates list was likewise the
// pre-gate list. The running release is added from history[0] because the
// rollout controller excludes it from candidates, and a site that is up to
// date with its wave's selection would otherwise report WaitingForCandidate
// forever instead of Ready.
func (r *GitHubEnvironmentReconciler) candidatesOf(ctx context.Context, rollout *kuberikrolloutv1alpha1.Rollout) (map[string]string, error) {
	c := map[string]string{}
	if h := rollout.Status.History; len(h) > 0 && h[0].Version.Revision != nil &&
		*h[0].Version.Revision != "" && h[0].Version.Tag != "" {
		c[*h[0].Version.Revision] = h[0].Version.Tag
	}
	if len(rollout.Status.Candidates) == 0 || rollout.Status.ReleaseSource == nil {
		return c, nil
	}
	var slices releasev1alpha1.ReleaseSliceList
	err := r.List(ctx, &slices, client.InNamespace(rollout.Namespace),
		client.MatchingLabels{releasev1alpha1.ReleaseSourceLabel: rollout.Status.ReleaseSource.Name})
	if err != nil {
		if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			return nil, fmt.Errorf("%w: %v", errCatalogueUnavailable, err)
		}
		return nil, err
	}
	revByTag := map[string]string{}
	for i := range slices.Items {
		for _, rel := range slices.Items[i].Releases {
			if rel.Revision != nil && *rel.Revision != "" {
				revByTag[rel.Tag] = *rel.Revision
			}
		}
	}
	for _, cand := range rollout.Status.Candidates {
		if rev, ok := revByTag[cand.Tag]; ok {
			c[rev] = cand.Tag
		}
	}
	return c, nil
}

// currentRevision is the revision of the Rollout's newest history entry.
func currentRevision(rollout *kuberikrolloutv1alpha1.Rollout) string {
	if len(rollout.Status.History) == 0 || rollout.Status.History[0].Version.Revision == nil {
		return ""
	}
	return *rollout.Status.History[0].Version.Revision
}

func anyRegistered(members []wave.Member, env string) bool {
	for _, m := range members {
		if m.Environment == env && !m.Deregistered {
			return true
		}
	}
	return false
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
	candidates, err := r.candidatesOf(ctx, rollout)
	if err != nil {
		if errors.Is(err, errCatalogueUnavailable) {
			// Not retryable by requeue alone; say so and leave the gate as is.
			return r.setWaveUnresolved(ctx, env, err)
		}
		return err
	}
	in := wave.Input{
		Self: wave.Self{
			Environment:   self,
			Alias:         env.Spec.Environment,
			Site:          site,
			Wave:          r.effectiveWave(env),
			Lockstep:      env.Spec.Lockstep,
			Upstream:      upstream,
			PinnedVersion: pinned,
		},
		Members:    members,
		Candidates: candidates,
		Current:    currentRevision(rollout),
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

// setWaveUnresolved marks WaveReady False when candidates cannot be resolved.
// The RolloutGate is left untouched.
func (r *GitHubEnvironmentReconciler) setWaveUnresolved(ctx context.Context, env *kuberikv1alpha1.Environment, cause error) error {
	before := env.Status.DeepCopy()
	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type: ConditionWaveReady, Status: metav1.ConditionFalse, Reason: ReasonCandidatesUnresolved,
		Message: cause.Error(), ObservedGeneration: env.Generation,
	})
	if equality.Semantic.DeepEqual(before.Conditions, env.Status.Conditions) {
		return nil
	}
	return r.Status().Update(ctx, env)
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

	ws.Blocker = waveBlockerStatus(dec.Blocker)
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

// ---- finalizer / deregistration -------------------------------------------

func (r *GitHubEnvironmentReconciler) event(env *kuberikv1alpha1.Environment, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(env, eventType, reason, msg)
	}
}

// regIdentity is what an Environment wrote to GitHub: the deployment name
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

	// An Environment that does not own the site posted nothing for it and must
	// not mark the owner's registration as gone.
	self, err := r.selfOwner(ctx, env)
	if err != nil {
		return err
	}
	conflict, err := r.findSiteConflict(ctx, gh, owner, repo, id.name, id.environment, id.site, self)
	if err != nil {
		return err
	}
	if conflict != nil {
		return nil
	}

	deps, err := r.listEnvDeployments(ctx, gh, owner, repo, task, formattedEnv)
	if err != nil {
		return err
	}
	// Mark self's newest deployment; failing that the newest ownerless one.
	// Never another owner's.
	var newest, ownerless *github.Deployment
	for _, d := range deps { // newest first
		p := r.extractDeploymentPayload(d)
		if p == nil || p.Site != id.site {
			continue
		}
		if p.Owner != nil && *p.Owner == self {
			newest = d
			break
		}
		if ownerless == nil && (p.Owner == nil || p.Owner.isZero()) {
			ownerless = d
		}
	}
	if newest == nil {
		newest = ownerless
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

func (r *GitHubEnvironmentReconciler) addFinalizer(ctx context.Context, env *kuberikv1alpha1.Environment) error {
	if controllerutil.ContainsFinalizer(env, DeregisterFinalizer) {
		return nil
	}
	// A merge patch sends only metadata.finalizers: an object that fails
	// full-object validation can still get its finalizer.
	base := env.DeepCopy()
	controllerutil.AddFinalizer(env, DeregisterFinalizer)
	return r.Patch(ctx, env, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

func (r *GitHubEnvironmentReconciler) removeFinalizer(ctx context.Context, env *kuberikv1alpha1.Environment) error {
	if !controllerutil.ContainsFinalizer(env, DeregisterFinalizer) {
		return nil
	}
	base := env.DeepCopy()
	controllerutil.RemoveFinalizer(env, DeregisterFinalizer)
	if err := r.Patch(ctx, env, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
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
	if env.Annotations[SkipDeregisterAnnotation] == annotationTrue {
		lg.Info("Skipping deregistration (annotation)")
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}
	id := r.currentIdentity(env)
	if rec, ok := r.recordedIdentity(env); ok {
		id = rec // what was actually written to GitHub
	}
	site := id.site
	if env.Spec.Backend.Type != backendTypeGitHub || isDuplicateSite(env) {
		// A DuplicateSite Environment posted nothing to GitHub: nothing to undo.
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}
	gh, err := r.getGitHubClient(ctx, env)
	if err != nil {
		if apierrors.IsNotFound(err) {
			terminating, nsErr := r.namespaceTerminating(ctx, env.Namespace)
			if nsErr != nil {
				return ctrl.Result{}, nsErr
			}
			if !terminating {
				return ctrl.Result{}, err
			}
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

// namespaceReadTimeout bounds the Namespace read so a stuck API call can never
// freeze the single reconcile worker.
var namespaceReadTimeout = 10 * time.Second

// namespaceTerminating reports whether the namespace is gone or terminating.
// Namespaces are read through APIReader (the manager cache has no Namespace
// informer and RBAC only grants get: a cached Get would start an informer whose
// list is forbidden and block forever); r.Client is used only when APIReader is
// nil (tests). On a read error it returns false and the error.
func (r *GitHubEnvironmentReconciler) namespaceTerminating(ctx context.Context, name string) (bool, error) {
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	ctx, cancel := context.WithTimeout(ctx, namespaceReadTimeout)
	defer cancel()
	ns := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to read namespace %s: %w", name, err)
	}
	return ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating, nil
}

// reconcileSiteChange handles the registration identity before any GitHub
// write: when the recorded (name, environment, site) differs from the current
// one, deregister the OLD identity first, then persist the new one in
// status.wave. The identity is persisted only when it is new.
func (r *GitHubEnvironmentReconciler) reconcileSiteChange(ctx context.Context, gh *github.Client, env *kuberikv1alpha1.Environment) error {
	cur := r.currentIdentity(env)
	old, hadOld := r.recordedIdentity(env)
	if hadOld && old != cur {
		if err := r.deregisterSite(ctx, gh, env, old); err != nil {
			return err
		}
	}
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

// waveBlockerStatus maps the pure package's Blocker to the API type; nil in, nil out.
func waveBlockerStatus(b *wave.Blocker) *kuberikv1alpha1.WaveBlocker {
	if b == nil {
		return nil
	}
	return &kuberikv1alpha1.WaveBlocker{
		Reason: b.Reason, Environment: b.Environment, Site: b.Site, WaitingOn: b.WaitingOn,
		Outcome: b.Outcome, Revision: b.Revision, PinnedVersion: b.PinnedVersion,
		SelectedVersion: b.SelectedVersion, Waves: b.Waves, MissingWaves: b.MissingWaves, Names: b.Names,
	}
}
