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

	"github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
)

// One deployment name + tier + site has exactly one owning Environment across
// all clusters. Ownership is recorded in the payload of every deployment the
// owner creates and evaluated from GitHub alone:
//
//   - deployments are grouped by owner identity;
//   - an owner is active unless the newest status of its newest deployment is a
//     deregistration marker;
//   - self is in conflict when another owner is active, unless self is active
//     too and registered first (lowest earliest deployment ID, which settles a
//     race between two new Environments);
//   - deployments without an owner (written before ownership existed) belong to
//     nobody: they never conflict and are adopted by whoever reconciles them.

// deploymentOwner identifies an Environment object across clusters. The zero
// value means "no owner recorded".
type deploymentOwner struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func (o deploymentOwner) isZero() bool { return o == deploymentOwner{} }

func (o deploymentOwner) String() string {
	return fmt.Sprintf("cluster %s namespace %s name %s", o.Cluster, o.Namespace, o.Name)
}

// clusterID is the UID of namespace kube-system, looked up once and cached.
// Tests may pre-set GitHubEnvironmentReconciler.ClusterID.
func (r *GitHubEnvironmentReconciler) clusterID(ctx context.Context) (string, error) {
	r.clusterMu.Lock()
	defer r.clusterMu.Unlock()
	if r.ClusterID != "" {
		return r.ClusterID, nil
	}
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: metav1.NamespaceSystem}, ns); err != nil {
		return "", fmt.Errorf("failed to read the cluster identity (namespace %s): %w", metav1.NamespaceSystem, err)
	}
	r.ClusterID = string(ns.UID)
	return r.ClusterID, nil
}

// selfOwner is the ownership identity of env.
func (r *GitHubEnvironmentReconciler) selfOwner(ctx context.Context, env *kuberikv1alpha1.Environment) (deploymentOwner, error) {
	cluster, err := r.clusterID(ctx)
	if err != nil {
		return deploymentOwner{}, err
	}
	return deploymentOwner{Cluster: cluster, Namespace: env.Namespace, Name: env.Name}, nil
}

// ownerMatches reports whether a deployment with the given recorded owner may be
// matched (and re-used) by self: it is self's or nobody's.
func ownerMatches(recorded, self deploymentOwner) bool {
	return recorded.isZero() || recorded == self
}

// findSiteConflict returns the active owner that keeps self from owning
// (deploymentName, tier, site), or nil when self may register.
//
// Call budget: the list calls of every alias of the tier plus one status fetch
// per owner (the newest deployment of each), shared with the rest of the
// reconcile through the status cache.
func (r *GitHubEnvironmentReconciler) findSiteConflict(ctx context.Context, gh *github.Client, owner, repo, deploymentName, tier, site string, self deploymentOwner) (*deploymentOwner, error) {
	deps, err := r.listTierDeployments(ctx, gh, owner, repo, formatDeploymentTask(deploymentName), deploymentName, canonicalEnv(tier))
	if err != nil {
		return nil, err
	}
	type group struct{ earliest, newest int64 }
	groups := map[deploymentOwner]*group{}
	for _, d := range deps {
		p := r.extractDeploymentPayload(d)
		if p == nil || p.Site != site || p.Owner == nil || p.Owner.isZero() {
			continue
		}
		g := groups[*p.Owner]
		if g == nil {
			g = &group{earliest: d.GetID(), newest: d.GetID()}
			groups[*p.Owner] = g
		}
		g.earliest = min(g.earliest, d.GetID())
		g.newest = max(g.newest, d.GetID())
	}
	var other *deploymentOwner
	var otherEarliest int64
	var selfEarliest int64
	selfActive := false
	for o, g := range groups {
		statuses, err := r.fetchAllDeploymentStatuses(ctx, gh, owner, repo, g.newest)
		if err != nil {
			return nil, fmt.Errorf("failed to read statuses of deployment %d: %w", g.newest, err)
		}
		if isDeregistered(statuses) {
			continue
		}
		if o == self {
			selfActive, selfEarliest = true, g.earliest
			continue
		}
		if other == nil || g.earliest < otherEarliest || (g.earliest == otherEarliest && o.String() < other.String()) {
			oc := o
			other, otherEarliest = &oc, g.earliest
		}
	}
	if other == nil || (selfActive && selfEarliest < otherEarliest) {
		return nil, nil
	}
	return other, nil
}

func isDuplicateSite(env *kuberikv1alpha1.Environment) bool {
	c := meta.FindStatusCondition(env.Status.Conditions, ConditionWaveReady)
	return c != nil && c.Reason == ReasonDuplicateSite
}

// reconcileDuplicateSite is the reconcile of an Environment that does not own
// its site: nothing is posted to GitHub, the gate is created but closed, and
// WaveReady says who owns the site.
func (r *GitHubEnvironmentReconciler) reconcileDuplicateSite(ctx context.Context, env *kuberikv1alpha1.Environment, conflict deploymentOwner) error {
	site := r.effectiveSite(env)
	msg := fmt.Sprintf("site %s of %s for %s is owned by %s", site, canonicalEnv(env.Spec.Environment), env.Spec.Name, conflict)
	log.FromContext(ctx).Info("Site is owned by another Environment", "site", site, "owner", conflict.String())

	if err := r.createOrUpdateRolloutGate(ctx, env); err != nil {
		return err
	}
	closed := []string{}
	if err := r.setGateAllowedVersions(ctx, env, &closed); err != nil {
		return err
	}

	wasDuplicate := isDuplicateSite(env)
	before := env.Status.DeepCopy()
	env.Status.Wave = &kuberikv1alpha1.WaveStatus{
		Site:        site,
		Environment: env.Spec.Environment,
		Name:        ensureKuberikPrefix(env.Spec.Name),
		Wave:        r.effectiveWave(env),
	}
	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type: ConditionWaveReady, Status: metav1.ConditionFalse, Reason: ReasonDuplicateSite,
		Message: msg, ObservedGeneration: env.Generation,
	})
	meta.RemoveStatusCondition(&env.Status.Conditions, ConditionLockstepConsistent)
	if !wasDuplicate {
		r.event(env, corev1.EventTypeWarning, ReasonDuplicateSite, msg)
	}
	if equality.Semantic.DeepEqual(before.Wave, env.Status.Wave) && equality.Semantic.DeepEqual(before.Conditions, env.Status.Conditions) {
		return nil
	}
	return r.Status().Update(ctx, env)
}

// reconcileDeprecations reports the deprecated spec.relationship. The Deprecated
// condition is True while it is set and removed otherwise; the log line is
// written once per object generation.
func (r *GitHubEnvironmentReconciler) reconcileDeprecations(ctx context.Context, env *kuberikv1alpha1.Environment) error {
	cur := meta.FindStatusCondition(env.Status.Conditions, ConditionDeprecated)
	if env.Spec.Relationship == nil {
		if cur == nil {
			return nil
		}
		meta.RemoveStatusCondition(&env.Status.Conditions, ConditionDeprecated)
		return r.Status().Update(ctx, env)
	}
	if cur != nil && cur.Status == metav1.ConditionTrue && cur.ObservedGeneration == env.Generation {
		return nil
	}
	log.FromContext(ctx).Info(messageRelationshipIgnored, "generation", env.Generation)
	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type: ConditionDeprecated, Status: metav1.ConditionTrue, Reason: ReasonRelationshipIgnored,
		Message: messageRelationshipIgnored, ObservedGeneration: env.Generation,
	})
	return r.Status().Update(ctx, env)
}
