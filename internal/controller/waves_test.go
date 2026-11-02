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
	legacy := struct {
		ID      string                              `json:"id"`
		Version *kuberikrolloutv1alpha1.VersionInfo `json:"version,omitempty"`
	}{"7", &version}
	want, _ := json.Marshal(legacy)
	got, _ := json.Marshal(deploymentPayload{ID: "7", Version: &version})
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
	wRevA  = "aaaaaaa1111111111111111111111111111111aa"
	wRevB  = "bbbbbbb2222222222222222222222222222222bb"
	wRevC  = "ccccccc3333333333333333333333333333333cc"
	wApp   = "waves-app"
	euSite = "eu-west-1"
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
	histEntry := func(id int64, rev, tag string) kuberikrolloutv1alpha1.DeploymentHistoryEntry {
		return kuberikrolloutv1alpha1.DeploymentHistoryEntry{
			ID: k8sptr.To(id), Version: kuberikrolloutv1alpha1.VersionInfo{Tag: tag, Revision: k8sptr.To(rev)},
			BakeStatus: k8sptr.To("Succeeded"), Timestamp: metav1.Now(),
		}
	}
	mkEnv := func(name, site string, waveN *int32) *kuberikv1alpha1.Environment {
		e := &kuberikv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: kuberikv1alpha1.EnvironmentSpec{
				RolloutRef: corev1.LocalObjectReference{Name: "rollout"},
				Name:       wApp, Environment: "prod", Site: site, Wave: waveN,
				Backend: kuberikv1alpha1.BackendConfig{Type: "github", Project: "o/r", Secret: "github-token"},
			},
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
		// Self must have deployed once to count as registered (and so to lead).
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("eu", euSite, nil)
		reconcile(e)

		cur := get(e)
		Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeTrue())
		Expect(cur.Status.Wave).NotTo(BeNil())
		Expect(cur.Status.Wave.Site).To(Equal(euSite))
		Expect(cur.Status.Wave.Wave).To(Equal(int32(1)))
		Expect(cur.Status.Wave.Role).To(Equal(kuberikv1alpha1.WaveRoleLeader))
		Expect(*gateAllowed(e)).To(ConsistOf("v1", "v2"))
		Expect(cond(e, ConditionWaveReady).Status).To(Equal(metav1.ConditionTrue))
		Expect(cond(e, ConditionLockstepConsistent).Status).To(Equal(metav1.ConditionTrue))
	})

	It("makes a follower deploy exactly the leader's selection", func() {
		seedStaging()
		seed(prodGH, euSite, 1, true, wRevA, "v1", "InProgress") // leader selected A, still baking
		mkRollout()
		e := mkEnv("us", "us-east-1", nil)
		reconcile(e)

		Expect(get(e).Status.Wave.Role).To(Equal(kuberikv1alpha1.WaveRoleFollower))
		Expect(get(e).Status.Wave.Leader).To(Equal(euSite))
		Expect(*gateAllowed(e)).To(ConsistOf("v1"))
	})

	It("reports WaitingForCandidate when the selected revision has no tag", func() {
		seed(prodGH, euSite, 1, true, "dddddddd44444444444444444444444444444444", "v9", "InProgress")
		mkRollout()
		e := mkEnv("us", "us-east-1", nil)
		reconcile(e)

		Expect(*gateAllowed(e)).To(BeEmpty())
		c := cond(e, ConditionWaveReady)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(wave.ReasonWaitingForCandidate))
	})

	It("blocks the wave-2 leader until every wave-1 member baked, then opens", func() {
		seed(prodGH, euSite, 1, true, wRevA, "v1", "Succeeded")
		usID := seed(prodGH, "us-east-1", 1, false, wRevA, "v1", "InProgress")
		mkRollout()
		e := mkEnv("ap", "ap-south-1", k8sptr.To(int32(2)))
		reconcile(e)

		Expect(get(e).Status.Wave.Role).To(Equal(kuberikv1alpha1.WaveRoleLeader))
		Expect(*gateAllowed(e)).To(BeEmpty())
		c := cond(e, ConditionWaveReady)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(wave.ReasonWaitingFor))
		Expect(c.Message).To(ContainSubstring("us-east-1"))

		fake.addStatus(usID, stateSuccess, "Bake succeeded")
		reconcile(e)
		Expect(*gateAllowed(e)).To(ConsistOf("v1"))
		Expect(cond(e, ConditionWaveReady).Status).To(Equal(metav1.ConditionTrue))
	})

	It("blocks everything on a wave gap with InvalidTopology", func() {
		seed(prodGH, euSite, 1, true, wRevA, "v1", "Succeeded")
		mkRollout()
		e := mkEnv("ap", "ap-south-1", k8sptr.To(int32(3)))
		reconcile(e)

		Expect(*gateAllowed(e)).To(BeEmpty())
		c := cond(e, ConditionWaveReady)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(wave.ReasonInvalidTopology))
	})

	It("writes site metadata, selection and auto_inactive=false on sited deployments", func() {
		seed(prodGH, "us-east-1", 1, false, wRevA, "v1", "Succeeded")
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("eu", euSite, nil)
		reconcile(e) // creates deployment 1 (role not yet known: selection=false)
		reconcile(e) // role Leader is now recorded

		ro := &kuberikrolloutv1alpha1.Rollout{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rollout", Namespace: ns}, ro)).To(Succeed())
		ro.Status.History = append([]kuberikrolloutv1alpha1.DeploymentHistoryEntry{histEntry(2, wRevB, "v2")}, ro.Status.History...)
		Expect(k8sClient.Status().Update(ctx, ro)).To(Succeed())
		reconcile(e)

		var mine []fakeDep
		for _, d := range fake.deploymentsOf(prodGH) {
			var p map[string]any
			_ = json.Unmarshal(decodedPayload(d.Payload), &p)
			if p["site"] == euSite {
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
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("plain", "", nil)
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
		rec.DefaultSite = euSite
		mkRollout()
		e := mkEnv("def", "", nil)
		reconcile(e)
		cur := get(e)
		Expect(cur.Status.Wave.Site).To(Equal(euSite))
		Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeTrue())
	})

	It("posts the Deregistered marker on deletion and removes the finalizer", func() {
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("eu", euSite, nil)
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
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("eu", euSite, nil)
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
		seed(prodGH, euSite, 1, true, wRevA, "v1", "Succeeded")
		seed(stagGH, "stg", 1, false, wRevA, "v1", "Succeeded")
		mkRollout()
		e := mkEnv("ap", "ap-south-1", k8sptr.To(int32(2)))
		reconcile(e)
		counts := fake.statusListCalls()
		Expect(counts).To(HaveLen(3)) // every seeded deployment was read
		for id, n := range counts {
			Expect(n).To(Equal(1), "statuses of deployment %s fetched %d times", id, n)
		}
		// payloads are readable in list responses: no per-deployment GetDeployment
		Expect(fake.getDeploymentCalls()).To(BeZero())
	})

	Describe("history display with deregistered sites", func() {
		It("follows newer site-less deployments once every upstream site is deregistered", func() {
			stg := seed(stagGH, "stg", 1, false, wRevA, "v1", "Succeeded")
			fake.addStatus(stg, "inactive", "Deregistered: site stg removed")
			seed(stagGH, "", 0, false, wRevA, "v1", "Succeeded")
			seed(stagGH, "", 0, false, wRevB, "v2", "Succeeded")
			mkRollout()
			e := mkEnv("plain", "", nil)
			reconcile(e)
			Expect(*gateAllowed(e)).To(ConsistOf("v1", "v2"))
		})

		It("shows a live site-less cluster beside deregistered sited ones", func() {
			seed(stagGH, "stg", 1, false, wRevA, "v1", "Succeeded")
			stgB := seed(stagGH, "stg", 1, false, wRevB, "v2", "Succeeded")
			fake.addStatus(stgB, "inactive", "Deregistered: site stg removed")
			seed(stagGH, "", 0, false, wRevC, "v3", "Succeeded")
			mkRollout()
			e := mkEnv("plain", "", nil)
			reconcile(e)
			Expect(*gateAllowed(e)).To(ConsistOf("v3"))
		})
	})

	Describe("registration identity", func() {
		lastStatus := func(d fakeDep) fakeStatus { return d.Statuses[len(d.Statuses)-1] }
		sitePayload := func(d fakeDep) string {
			var p map[string]any
			_ = json.Unmarshal(decodedPayload(d.Payload), &p)
			s, _ := p["site"].(string)
			return s
		}

		It("deregisters the old site when the site changes", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(get(e).Status.Wave.Environment).To(Equal("prod"))
			Expect(get(e).Status.Wave.Name).To(Equal("kuberik/" + wApp))

			cur := get(e)
			cur.Spec.Site = "us-east-1"
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
			reconcile(e)

			var eu, us *fakeDep
			for _, d := range fake.deploymentsOf(prodGH) {
				switch sitePayload(d) {
				case euSite:
					eu = &d
				case "us-east-1":
					us = &d
				}
			}
			Expect(eu).NotTo(BeNil())
			Expect(us).NotTo(BeNil())
			Expect(lastStatus(*eu).Description).To(Equal("Deregistered: site eu-west-1 removed"))
			Expect(*lastStatus(*eu).AutoInactive).To(BeFalse())
			Expect(lastStatus(*us).Description).NotTo(HavePrefix("Deregistered"))
			Expect(get(e).Status.Wave.Site).To(Equal("us-east-1"))
		})

		It("deregisters the old environment's member when spec.environment changes", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)

			cur := get(e)
			cur.Spec.Environment = "production"
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
			reconcile(e)

			old := fake.deploymentsOf(prodGH)
			Expect(old).To(HaveLen(1))
			Expect(lastStatus(old[0]).Description).To(Equal("Deregistered: site eu-west-1 removed"))
			fresh := fake.deploymentsOf("kuberik/" + wApp + "/production")
			Expect(fresh).To(HaveLen(1))
			Expect(lastStatus(fresh[0]).Description).NotTo(HavePrefix("Deregistered"))
			Expect(get(e).Status.Wave.Environment).To(Equal("production"))
		})

		It("deregisters, releases the finalizer and clears wave state when the site is removed", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady)).NotTo(BeNil())

			cur := get(e)
			cur.Spec.Site = ""
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
			reconcile(e)

			cur = get(e)
			Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeFalse())
			Expect(cur.Status.Wave).To(BeNil())
			Expect(cond(e, ConditionWaveReady)).To(BeNil())
			Expect(cond(e, ConditionLockstepConsistent)).To(BeNil())
			deps := fake.deploymentsOf(prodGH)
			var eu *fakeDep
			for _, d := range deps {
				if sitePayload(d) == euSite {
					eu = &d
				}
			}
			Expect(eu).NotTo(BeNil())
			// The site-less status that follows may append GitHub's automatic inactive
			// status after the marker, so look for the marker itself.
			var marker *fakeStatus
			for _, st := range eu.Statuses {
				if st.Description == "Deregistered: site eu-west-1 removed" {
					marker = &st
				}
			}
			Expect(marker).NotTo(BeNil())
			Expect(marker.AutoInactive).NotTo(BeNil())
			Expect(*marker.AutoInactive).To(BeFalse()) // even though the Environment is site-less now
		})

		It("re-registers a recreated Environment and other members see it again", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			ap := mkEnv("ap", "ap-south-1", k8sptr.To(int32(2)))
			member := func() *kuberikv1alpha1.WaveMemberStatus {
				reconcile(ap)
				for _, m := range get(ap).Status.Wave.Members {
					if m.Site == euSite {
						return &m
					}
				}
				return nil
			}
			Expect(member()).NotTo(BeNil())
			Expect(member().Deregistered).To(BeFalse())

			Expect(k8sClient.Delete(ctx, get(e))).To(Succeed())
			reconcile(e)
			Expect(member().Deregistered).To(BeTrue())

			mkEnv("eu", euSite, nil)
			reconcile(e)
			var eu fakeDep
			for _, d := range fake.deploymentsOf(prodGH) {
				if sitePayload(d) == euSite {
					eu = d
				}
			}
			Expect(lastStatus(eu).Description).NotTo(HavePrefix("Deregistered"))
			Expect(lastStatus(eu).State).To(Equal(stateSuccess))
			Expect(member().Deregistered).To(BeFalse())
		})

		It("gives up deregistration when the namespace is terminating and the token Secret is gone", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			before := fake.callCount()

			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: ns}}
			Expect(k8sClient.Delete(ctx, sec)).To(Succeed())
			// envtest runs no namespace controller: the namespace stays Terminating.
			Expect(k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
			Expect(k8sClient.Delete(ctx, get(e))).To(Succeed())
			reconcile(e)

			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: e.Name, Namespace: ns}, &kuberikv1alpha1.Environment{}))).To(BeTrue())
			Expect(fake.callCount()).To(Equal(before))
		})

		It("keeps retrying deregistration when the Secret is gone but the namespace is not terminating", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: ns}})).To(Succeed())
			Expect(k8sClient.Delete(ctx, get(e))).To(Succeed())
			_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: e.Name, Namespace: ns}})
			Expect(err).To(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(get(e), DeregisterFinalizer)).To(BeTrue())
		})

		It("posts the marker on OUR newest deployment when other sites have newer ones", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			ro := &kuberikrolloutv1alpha1.Rollout{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rollout", Namespace: ns}, ro)).To(Succeed())
			ro.Status.History = append([]kuberikrolloutv1alpha1.DeploymentHistoryEntry{histEntry(2, wRevB, "v2")}, ro.Status.History...)
			Expect(k8sClient.Status().Update(ctx, ro)).To(Succeed())
			reconcile(e)
			usID := seed(prodGH, "us-east-1", 1, false, wRevB, "v2", "Succeeded")

			Expect(k8sClient.Delete(ctx, get(e))).To(Succeed())
			reconcile(e)

			var euDeps []fakeDep
			for _, d := range fake.deploymentsOf(prodGH) {
				switch {
				case d.ID == usID:
					Expect(lastStatus(d).Description).NotTo(HavePrefix("Deregistered"))
				case sitePayload(d) == euSite:
					euDeps = append(euDeps, d)
				}
			}
			Expect(euDeps).To(HaveLen(2))
			Expect(lastStatus(euDeps[0]).Description).NotTo(HavePrefix("Deregistered"))
			Expect(lastStatus(euDeps[1]).Description).To(Equal("Deregistered: site eu-west-1 removed"))
		})
	})

	Describe("site-less Environment with a sited upstream", func() {
		It("uses only the last wave for upstream completion", func() {
			seed(stagGH, "stg1", 1, false, wRevA, "v1", "Succeeded")
			seed(stagGH, "stg1", 1, false, wRevB, "v2", "Succeeded")
			seed(stagGH, "stg2", 2, false, wRevA, "v1", "Succeeded")
			seed(stagGH, "stg2", 2, false, wRevB, "v2", "InProgress")
			mkRollout()
			e := mkEnv("plain", "", nil)
			reconcile(e)
			Expect(*gateAllowed(e)).To(ConsistOf("v1"))
		})

		It("excludes a deregistered member from completion", func() {
			seed(stagGH, "stg1", 1, false, wRevA, "v1", "Succeeded")
			seed(stagGH, "stg1", 1, false, wRevB, "v2", "Succeeded")
			seed(stagGH, "stg2", 1, false, wRevA, "v1", "Succeeded")
			b := seed(stagGH, "stg2", 1, false, wRevB, "v2", "InProgress")
			mkRollout()
			e := mkEnv("plain", "", nil)
			reconcile(e)
			Expect(*gateAllowed(e)).To(ConsistOf("v1"))

			fake.addStatus(b, "inactive", "Deregistered: site stg2 removed")
			reconcile(e)
			Expect(*gateAllowed(e)).To(ConsistOf("v1", "v2"))
		})
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

func TestCandidatesOfIncludesDeployedRelease(t *testing.T) {
	rev := func(s string) *string { return &s }
	r := &kuberikrolloutv1alpha1.Rollout{}
	r.Status.AvailableReleases = []kuberikrolloutv1alpha1.VersionInfo{
		{Tag: "v0", Revision: rev("000")},
		{Tag: "v1", Revision: rev("aaa")},
		{Tag: "v2", Revision: rev("bbb")},
	}
	// The running version (v1) is not a release candidate.
	r.Status.ReleaseCandidates = []kuberikrolloutv1alpha1.VersionInfo{
		{Tag: "v2", Revision: rev("bbb")},
	}
	r.Status.History = []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
		{Version: kuberikrolloutv1alpha1.VersionInfo{Tag: "v1", Revision: rev("aaa")}},
	}
	c := candidatesOf(r)
	if c["aaa"] != "v1" || c["bbb"] != "v2" || len(c) != 2 {
		t.Fatalf("candidatesOf = %v, want aaa->v1, bbb->v2 only", c)
	}
}
