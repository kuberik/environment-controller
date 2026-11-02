package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	k8sptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	"github.com/kuberik/environment-controller/internal/wave"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// The deployment payload of a site-less Environment must stay byte-identical to
// what was written before sites existed.
func TestSitelessPayloadIsByteIdentical(t *testing.T) {
	rev := "abc"
	version := kuberikrolloutv1alpha1.VersionInfo{Tag: "v1", Revision: &rev}
	rel := &kuberikv1alpha1.EnvironmentRelationship{Environment: "staging", Type: kuberikv1alpha1.RelationshipTypeAfter}
	legacy := struct {
		ID           string                                   `json:"id"`
		Relationship *kuberikv1alpha1.EnvironmentRelationship `json:"relationship,omitempty"`
		Version      *kuberikrolloutv1alpha1.VersionInfo      `json:"version,omitempty"`
	}{"7", rel, &version}
	want, _ := json.Marshal(legacy)
	got, _ := json.Marshal(deploymentPayload{ID: "7", Relationship: rel, Version: &version})
	if string(want) != string(got) {
		t.Fatalf("site-less payload changed:\nwant %s\ngot  %s", want, got)
	}
	for _, k := range []string{"site", "wave", "lockstep", "selection"} {
		if strings.Contains(string(got), `"`+k+`"`) {
			t.Fatalf("site-less payload contains %q: %s", k, got)
		}
	}
}

var waveNS int32

const (
	wRevA = "aaaaaaa1111111111111111111111111111111aa"
	wRevB = "bbbbbbb2222222222222222222222222222222bb"
	wRevC = "ccccccc3333333333333333333333333333333cc"
	wApp  = "waves-app"
)

var _ = Describe("Sites and waves", func() {
	var (
		ctx     = context.Background()
		fake    *fakeGitHub
		ns      string
		rec     *GitHubEnvironmentReconciler
		prodGH  = "kuberik/" + wApp + "/prod"
		stagGH  = "kuberik/" + wApp + "/staging"
		taskGH  = "deploy:kuberik/" + wApp
		histSeq int64
	)

	payload := func(site string, waveN int32, selection bool, rev, tag string) map[string]any {
		histSeq++
		p := map[string]any{"id": fmt.Sprint(histSeq), "version": map[string]any{"tag": tag, "revision": rev}}
		if site != "" {
			p["site"], p["wave"] = site, waveN
		}
		if selection {
			p["selection"] = true
		}
		return p
	}
	// seed adds a deployment of another cluster with a final bake state.
	seed := func(ghEnv, site string, waveN int32, selection bool, rev, tag, bake string) int64 {
		id := fake.addDeployment(taskGH, ghEnv, rev, payload(site, waveN, selection, rev, tag))
		state, prefix := mapBakeToGitHubState(&bake)
		fake.addStatus(id, state, prefix)
		return id
	}

	mkRollout := func(history ...kuberikrolloutv1alpha1.DeploymentHistoryEntry) *kuberikrolloutv1alpha1.Rollout {
		ro := &kuberikrolloutv1alpha1.Rollout{
			ObjectMeta: metav1.ObjectMeta{Name: "rollout", Namespace: ns},
			Spec:       kuberikrolloutv1alpha1.RolloutSpec{ReleasesImagePolicy: corev1.LocalObjectReference{Name: "p"}},
		}
		Expect(k8sClient.Create(ctx, ro)).To(Succeed())
		ro.Status = kuberikrolloutv1alpha1.RolloutStatus{
			History: history,
			ReleaseCandidates: []kuberikrolloutv1alpha1.VersionInfo{
				{Tag: "v1", Revision: k8sptr.To(wRevA)},
				{Tag: "v2", Revision: k8sptr.To(wRevB)},
				{Tag: "v3", Revision: k8sptr.To(wRevC)},
			},
		}
		Expect(k8sClient.Status().Update(ctx, ro)).To(Succeed())
		return ro
	}
	histEntry := func(id int64, rev, tag, bake string) kuberikrolloutv1alpha1.DeploymentHistoryEntry {
		return kuberikrolloutv1alpha1.DeploymentHistoryEntry{
			ID: k8sptr.To(id), Version: kuberikrolloutv1alpha1.VersionInfo{Tag: tag, Revision: k8sptr.To(rev)},
			BakeStatus: k8sptr.To(bake), Timestamp: metav1.Now(),
		}
	}
	mkEnv := func(name, site string, waveN *int32, rel bool) *kuberikv1alpha1.Environment {
		e := &kuberikv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: kuberikv1alpha1.EnvironmentSpec{
				RolloutRef: corev1.LocalObjectReference{Name: "rollout"},
				Name:       wApp, Environment: "prod", Site: site, Wave: waveN,
				Backend: kuberikv1alpha1.BackendConfig{Type: "github", Project: "o/r", Secret: "github-token"},
			},
		}
		if rel {
			e.Spec.Relationship = &kuberikv1alpha1.EnvironmentRelationship{Environment: "staging", Type: kuberikv1alpha1.RelationshipTypeAfter}
		}
		Expect(k8sClient.Create(ctx, e)).To(Succeed())
		return e
	}
	reconcile := func(e *kuberikv1alpha1.Environment) {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: e.Name, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())
	}
	get := func(e *kuberikv1alpha1.Environment) *kuberikv1alpha1.Environment {
		out := &kuberikv1alpha1.Environment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: e.Name, Namespace: ns}, out)).To(Succeed())
		return out
	}
	gateAllowed := func(e *kuberikv1alpha1.Environment) *[]string {
		cur := get(e)
		Expect(cur.Status.RolloutGateRef).NotTo(BeNil())
		g := &kuberikrolloutv1alpha1.RolloutGate{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cur.Status.RolloutGateRef.Name, Namespace: ns}, g)).To(Succeed())
		return g.Spec.AllowedVersions
	}
	cond := func(e *kuberikv1alpha1.Environment, t string) *metav1.Condition {
		return meta.FindStatusCondition(get(e).Status.Conditions, t)
	}
	// seedStaging: staging (sited, one site, wave 1) completed A and B, still baking C.
	seedStaging := func() {
		seed(stagGH, "stg", 1, false, wRevA, "v1", "Succeeded")
		seed(stagGH, "stg", 1, false, wRevB, "v2", "Succeeded")
		seed(stagGH, "stg", 1, false, wRevC, "v3", "InProgress")
	}

	BeforeEach(func() {
		fake = newFakeGitHub()
		histSeq = 0
		ns = fmt.Sprintf("waves-%d", atomic.AddInt32(&waveNS, 1))
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: ns},
			Data:       map[string][]byte{"token": []byte("fake")},
		})).To(Succeed())
		rec = &GitHubEnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme, GitHubBaseURL: fake.URL()}
	})
	AfterEach(func() {
		list := &kuberikv1alpha1.EnvironmentList{}
		Expect(k8sClient.List(ctx, list)).To(Succeed())
		for i := range list.Items {
			e := &list.Items[i]
			if e.Namespace == ns {
				controllerutil.RemoveFinalizer(e, DeregisterFinalizer)
				_ = k8sClient.Update(ctx, e)
				_ = k8sClient.Delete(ctx, e)
			}
		}
		fake.Close()
	})

	It("gives the wave-1 leader the upstream-complete tags and records status.wave", func() {
		seedStaging()
		seed(prodGH, "us-east-1", 1, false, wRevA, "v1", "Succeeded")
		mkRollout()
		e := mkEnv("eu", "eu-west-1", nil, true)
		reconcile(e)

		cur := get(e)
		Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeTrue())
		Expect(cur.Status.Wave).NotTo(BeNil())
		Expect(cur.Status.Wave.Site).To(Equal("eu-west-1"))
		Expect(cur.Status.Wave.Wave).To(Equal(int32(1)))
		Expect(cur.Status.Wave.Role).To(Equal(kuberikv1alpha1.WaveRoleLeader))
		Expect(*gateAllowed(e)).To(ConsistOf("v1", "v2"))
		Expect(cond(e, ConditionWaveReady).Status).To(Equal(metav1.ConditionTrue))
		Expect(cond(e, ConditionLockstepConsistent).Status).To(Equal(metav1.ConditionTrue))
	})

	It("makes a follower deploy exactly the leader's selection", func() {
		seedStaging()
		seed(prodGH, "eu-west-1", 1, true, wRevA, "v1", "InProgress") // leader selected A, still baking
		mkRollout()
		e := mkEnv("us", "us-east-1", nil, true)
		reconcile(e)

		Expect(get(e).Status.Wave.Role).To(Equal(kuberikv1alpha1.WaveRoleFollower))
		Expect(get(e).Status.Wave.Leader).To(Equal("eu-west-1"))
		Expect(*gateAllowed(e)).To(ConsistOf("v1"))
	})

	It("reports WaitingForCandidate when the selected revision has no tag", func() {
		seed(prodGH, "eu-west-1", 1, true, "dddddddd44444444444444444444444444444444", "v9", "InProgress")
		mkRollout()
		e := mkEnv("us", "us-east-1", nil, false)
		reconcile(e)

		Expect(*gateAllowed(e)).To(BeEmpty())
		c := cond(e, ConditionWaveReady)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(wave.ReasonWaitingForCandidate))
	})

	It("blocks the wave-2 leader until every wave-1 member baked, then opens", func() {
		seed(prodGH, "eu-west-1", 1, true, wRevA, "v1", "Succeeded")
		usID := seed(prodGH, "us-east-1", 1, false, wRevA, "v1", "InProgress")
		mkRollout()
		e := mkEnv("ap", "ap-south-1", k8sptr.To(int32(2)), false)
		reconcile(e)

		Expect(get(e).Status.Wave.Role).To(Equal(kuberikv1alpha1.WaveRoleLeader))
		Expect(*gateAllowed(e)).To(BeEmpty())
		c := cond(e, ConditionWaveReady)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(wave.ReasonWaitingFor))
		Expect(c.Message).To(ContainSubstring("us-east-1"))

		fake.addStatus(usID, "success", "Bake succeeded")
		reconcile(e)
		Expect(*gateAllowed(e)).To(ConsistOf("v1"))
		Expect(cond(e, ConditionWaveReady).Status).To(Equal(metav1.ConditionTrue))
	})

	It("blocks everything on a wave gap with InvalidTopology", func() {
		seed(prodGH, "eu-west-1", 1, true, wRevA, "v1", "Succeeded")
		mkRollout()
		e := mkEnv("ap", "ap-south-1", k8sptr.To(int32(3)), false)
		reconcile(e)

		Expect(*gateAllowed(e)).To(BeEmpty())
		c := cond(e, ConditionWaveReady)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(wave.ReasonInvalidTopology))
	})

	It("writes site metadata, selection and auto_inactive=false on sited deployments", func() {
		seed(prodGH, "us-east-1", 1, false, wRevA, "v1", "Succeeded")
		mkRollout(histEntry(1, wRevA, "v1", "Succeeded"))
		e := mkEnv("eu", "eu-west-1", nil, false)
		reconcile(e) // creates deployment 1 (role not yet known: selection=false)
		reconcile(e) // role Leader is now recorded

		ro := &kuberikrolloutv1alpha1.Rollout{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rollout", Namespace: ns}, ro)).To(Succeed())
		ro.Status.History = append([]kuberikrolloutv1alpha1.DeploymentHistoryEntry{histEntry(2, wRevB, "v2", "Succeeded")}, ro.Status.History...)
		Expect(k8sClient.Status().Update(ctx, ro)).To(Succeed())
		reconcile(e)

		var mine []fakeDep
		for _, d := range fake.deploymentsOf(prodGH) {
			var p map[string]any
			_ = json.Unmarshal(decodedPayload(d.Payload), &p)
			if p["site"] == "eu-west-1" {
				mine = append(mine, d)
				Expect(p["wave"]).To(BeEquivalentTo(1))
			}
		}
		Expect(mine).To(HaveLen(2))
		var first, second map[string]any
		_ = json.Unmarshal(decodedPayload(mine[0].Payload), &first)
		_ = json.Unmarshal(decodedPayload(mine[1].Payload), &second)
		Expect(first).NotTo(HaveKey("selection"))
		Expect(second["selection"]).To(BeTrue())
		for _, d := range mine {
			Expect(d.Statuses).NotTo(BeEmpty())
			for _, s := range d.Statuses {
				Expect(s.AutoInactive).NotTo(BeNil())
				Expect(*s.AutoInactive).To(BeFalse())
			}
		}
		// the other site's deployment must not have been auto-inactivated
		for _, d := range fake.deploymentsOf(prodGH) {
			if strings.Contains(string(d.Payload), "us-east-1") {
				for _, s := range d.Statuses {
					Expect(s.State).NotTo(Equal("inactive"))
				}
			}
		}
		// two sites with colliding history IDs must not be merged in status
		for _, info := range get(e).Status.EnvironmentInfos {
			if info.Environment == "prod" {
				ids := map[int64]int{}
				for _, h := range info.History {
					ids[*h.ID]++
				}
				for _, n := range ids {
					Expect(n).To(Equal(1))
				}
			}
		}
	})

	It("keeps a site-less Environment exactly as before", func() {
		mkRollout(histEntry(1, wRevA, "v1", "Succeeded"))
		e := mkEnv("plain", "", nil, false)
		reconcile(e)

		cur := get(e)
		Expect(cur.Finalizers).To(BeEmpty())
		Expect(cur.Status.Wave).To(BeNil())
		Expect(cur.Status.Conditions).To(BeEmpty())
		deps := fake.deploymentsOf(prodGH) // same GitHub environment name as always
		Expect(deps).To(HaveLen(1))
		raw := string(decodedPayload(deps[0].Payload))
		for _, k := range []string{"site", "wave", "lockstep", "selection"} {
			Expect(raw).NotTo(ContainSubstring(`"` + k + `"`))
		}
		for _, s := range deps[0].Statuses {
			Expect(s.AutoInactive).To(BeNil())
		}
	})

	It("defaults the site from the controller", func() {
		rec.DefaultSite = "eu-west-1"
		mkRollout()
		e := mkEnv("def", "", nil, false)
		reconcile(e)
		cur := get(e)
		Expect(cur.Status.Wave.Site).To(Equal("eu-west-1"))
		Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeTrue())
	})

	It("posts the Deregistered marker on deletion and removes the finalizer", func() {
		mkRollout(histEntry(1, wRevA, "v1", "Succeeded"))
		e := mkEnv("eu", "eu-west-1", nil, false)
		reconcile(e)
		Expect(k8sClient.Delete(ctx, get(e))).To(Succeed())
		reconcile(e)

		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: e.Name, Namespace: ns}, &kuberikv1alpha1.Environment{}))).To(BeTrue())
		deps := fake.deploymentsOf(prodGH)
		Expect(deps).To(HaveLen(1))
		last := deps[0].Statuses[len(deps[0].Statuses)-1]
		Expect(last.State).To(Equal("inactive"))
		Expect(last.Description).To(Equal("Deregistered: site eu-west-1 removed"))
		Expect(*last.AutoInactive).To(BeFalse())
	})

	It("skips GitHub entirely with the skip-deregister annotation", func() {
		mkRollout(histEntry(1, wRevA, "v1", "Succeeded"))
		e := mkEnv("eu", "eu-west-1", nil, false)
		reconcile(e)
		cur := get(e)
		cur.Annotations = map[string]string{SkipDeregisterAnnotation: "true"}
		Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		Expect(k8sClient.Delete(ctx, cur)).To(Succeed())
		before := fake.callCount()
		reconcile(e)

		Expect(fake.callCount()).To(Equal(before))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: e.Name, Namespace: ns}, &kuberikv1alpha1.Environment{}))).To(BeTrue())
	})

	It("fetches each deployment's statuses at most once per reconcile", func() {
		seed(prodGH, "us-east-1", 1, false, wRevA, "v1", "Succeeded")
		seed(prodGH, "eu-west-1", 1, true, wRevA, "v1", "Succeeded")
		mkRollout()
		e := mkEnv("ap", "ap-south-1", k8sptr.To(int32(2)), false)
		reconcile(e)
		// 2 seeded deployments; each status list is requested once per reconcile
		// (page size 100 => one call each), though history building and the wave
		// reader both need them.
		Expect(fake.countCalls("GET /repos/o/r/deployments/")).To(BeNumerically("<=", 2*2))
	})
})

// decodedPayload returns the payload JSON whether stored as object or base64 string.
func decodedPayload(raw json.RawMessage) []byte {
	r := &GitHubEnvironmentReconciler{}
	// reuse the reconciler's tolerant decoder via a throwaway deployment
	_ = r
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if b, err := base64Decode(s); err == nil {
			return b
		}
		return []byte(s)
	}
	return raw
}

func base64Decode(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }
