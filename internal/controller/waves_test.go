package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	k8sptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	"github.com/kuberik/environment-controller/internal/wave"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// A payload without site, wave and owner (written before sites existed) is read
// as site "default", wave 1, with no owner.
func TestLegacyPayloadIsReadAsDefaultSite(t *testing.T) {
	r := &GitHubEnvironmentReconciler{}
	raw := `{"id":"7","version":{"tag":"v1","revision":"abc"}}`
	p := r.extractDeploymentPayload(&github.Deployment{Payload: json.RawMessage(raw)})
	if p == nil || p.Site != DefaultSiteName || p.Wave != 1 || p.Owner != nil {
		t.Fatalf("legacy payload read as %+v", p)
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
		extraNS []string
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
		rec = &GitHubEnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme, GitHubBaseURL: fake.URL(), ClusterID: "cluster-self"}
		extraNS = nil
	})
	AfterEach(func() {
		list := &kuberikv1alpha1.EnvironmentList{}
		Expect(k8sClient.List(ctx, list)).To(Succeed())
		for i := range list.Items {
			e := &list.Items[i]
			if e.Namespace == ns || slices.Contains(extraNS, e.Namespace) {
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

	It("registers an Environment without a site under the site \"default\"", func() {
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("plain", "", nil)
		reconcile(e)

		cur := get(e)
		Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeTrue())
		Expect(cur.Status.Wave).NotTo(BeNil())
		Expect(cur.Status.Wave.Site).To(Equal(DefaultSiteName))
		Expect(cur.Status.Wave.Wave).To(Equal(int32(1)))
		deps := fake.deploymentsOf(prodGH)
		Expect(deps).To(HaveLen(1))
		var p map[string]any
		Expect(json.Unmarshal(decodedPayload(deps[0].Payload), &p)).To(Succeed())
		Expect(p).To(HaveKeyWithValue("site", DefaultSiteName))
		Expect(p).To(HaveKeyWithValue("wave", BeNumerically("==", 1)))
		Expect(p).To(HaveKeyWithValue("owner", HaveKeyWithValue("cluster", rec.ClusterID)))
		Expect(p).To(HaveKeyWithValue("owner", HaveKeyWithValue("namespace", ns)))
		Expect(p).To(HaveKeyWithValue("owner", HaveKeyWithValue("name", "plain")))
		for _, st := range deps[0].Statuses {
			Expect(st.AutoInactive).NotTo(BeNil())
			Expect(*st.AutoInactive).To(BeFalse())
		}
	})

	It("reads the cluster identity from namespace kube-system when none is set", func() {
		rec.ClusterID = ""
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("plain", "", nil)
		reconcile(e)
		kubeSystem := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "kube-system"}, kubeSystem)).To(Succeed())
		Expect(rec.ClusterID).To(Equal(string(kubeSystem.UID)))
		var p map[string]any
		Expect(json.Unmarshal(decodedPayload(fake.deploymentsOf(prodGH)[0].Payload), &p)).To(Succeed())
		Expect(p).To(HaveKeyWithValue("owner", HaveKeyWithValue("cluster", string(kubeSystem.UID))))
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
		It("follows newer legacy deployments (site default) once every upstream site is deregistered", func() {
			stg := seed(stagGH, "stg", 1, false, wRevA, "v1", "Succeeded")
			fake.addStatus(stg, "inactive", "Deregistered: site stg removed")
			seed(stagGH, "", 0, false, wRevA, "v1", "Succeeded")
			seed(stagGH, "", 0, false, wRevB, "v2", "Succeeded")
			mkRollout()
			e := mkEnv("plain", "", nil)
			reconcile(e)
			Expect(*gateAllowed(e)).To(ConsistOf("v1", "v2"))
		})

		It("shows a live legacy cluster (site default) beside deregistered sites", func() {
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

		It("deregisters the old site and registers under \"default\" when the site is removed", func() {
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady)).NotTo(BeNil())

			cur := get(e)
			cur.Spec.Site = ""
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
			reconcile(e)

			cur = get(e)
			Expect(controllerutil.ContainsFinalizer(cur, DeregisterFinalizer)).To(BeTrue())
			Expect(cur.Status.Wave).NotTo(BeNil())
			Expect(cur.Status.Wave.Site).To(Equal(DefaultSiteName))
			Expect(cond(e, ConditionWaveReady)).NotTo(BeNil())
			deps := fake.deploymentsOf(prodGH)
			var eu *fakeDep
			sites := map[string]bool{}
			for _, d := range deps {
				sites[sitePayload(d)] = true
				if sitePayload(d) == euSite {
					eu = &d
				}
			}
			Expect(sites).To(HaveKey(DefaultSiteName))
			Expect(eu).NotTo(BeNil())
			var marker *fakeStatus
			for _, st := range eu.Statuses {
				if st.Description == "Deregistered: site eu-west-1 removed" {
					marker = &st
				}
			}
			Expect(marker).NotTo(BeNil())
			Expect(marker.AutoInactive).NotTo(BeNil())
			Expect(*marker.AutoInactive).To(BeFalse())
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

	Describe("Environment on the default site with an upstream", func() {
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

	Describe("site ownership", func() {
		// otherNS creates a second namespace with token and rollout; mkRollout and
		// mkEnv work in ns, so ns is switched while it is set up.
		otherNS := func(history ...kuberikrolloutv1alpha1.DeploymentHistoryEntry) string {
			first := ns
			ns = fmt.Sprintf("waves-%d", atomic.AddInt32(&waveNS, 1))
			extraNS = append(extraNS, ns)
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: ns},
				Data:       map[string][]byte{"token": []byte("fake")},
			})).To(Succeed())
			mkRollout(history...)
			second := ns
			ns = first
			return second
		}
		inNS := func(namespace string, f func()) {
			first := ns
			ns = namespace
			defer func() { ns = first }()
			f()
		}
		ownedBy := func(cluster, namespace, name string) map[string]any {
			return map[string]any{"cluster": cluster, "namespace": namespace, "name": name}
		}
		seedOwned := func(site string, o map[string]any, rev, tag, bake string) int64 {
			p := payload(site, 1, false, rev, tag)
			p["owner"] = o
			id := fake.addDeployment(taskGH, prodGH, rev, p)
			state, prefix := mapBakeToGitHubState(&bake)
			fake.addStatus(id, state, prefix)
			return id
		}

		It("gives a second Environment of the same name, tier and site DuplicateSite and posts nothing for it", func() {
			rec.Recorder = record.NewFakeRecorder(10)
			ns1 := ns
			mkRollout(histEntry(1, wRevA, "v1"))
			first := mkEnv("eu", euSite, nil)
			reconcile(first)
			Expect(cond(first, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
			owned := len(fake.deploymentsOf(prodGH))
			Expect(owned).To(Equal(1))

			ns2 := otherNS(histEntry(1, wRevA, "v1"), histEntry(2, wRevB, "v2"))
			var second *kuberikv1alpha1.Environment
			before := fake.postCount()
			inNS(ns2, func() {
				second = mkEnv("eu-dup", euSite, nil)
				reconcile(second)

				c := cond(second, ConditionWaveReady)
				Expect(c).NotTo(BeNil())
				Expect(c.Status).To(Equal(metav1.ConditionFalse))
				Expect(c.Reason).To(Equal(ReasonDuplicateSite))
				Expect(c.Message).To(Equal(fmt.Sprintf("site %s of prod for %s is owned by cluster cluster-self namespace %s name eu", euSite, wApp, ns1)))
				Expect(*gateAllowed(second)).To(BeEmpty())
				w := get(second).Status.Wave
				Expect(w).NotTo(BeNil())
				Expect(w.Site).To(Equal(euSite))
				Expect(w.Wave).To(Equal(int32(1)))
				Expect(w.Role).To(BeEmpty())
				Expect(w.Members).To(BeEmpty())
			})
			Expect(fake.postCount()).To(Equal(before))
			Expect(fake.deploymentsOf(prodGH)).To(HaveLen(owned))
			Expect(rec.Recorder.(*record.FakeRecorder).Events).To(Receive(ContainSubstring("DuplicateSite")))

			// The owner keeps working, and re-reconciling it never turns it into a duplicate.
			reconcile(first)
			Expect(cond(first, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))

			// Deleting the duplicate posts no deregistration marker.
			inNS(ns2, func() {
				Expect(k8sClient.Delete(ctx, get(second))).To(Succeed())
				reconcile(second)
				Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: second.Name, Namespace: ns2}, &kuberikv1alpha1.Environment{}))).To(BeTrue())
			})
			Expect(fake.postCount()).To(Equal(before))
			for _, d := range fake.deploymentsOf(prodGH) {
				for _, st := range d.Statuses {
					Expect(st.Description).NotTo(HavePrefix(deregisteredPrefix))
				}
			}
		})

		It("reports DuplicateSite when the owner is in another cluster", func() {
			seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevA, "v1", "Succeeded")
			mkRollout(histEntry(1, wRevA, "v1"), histEntry(2, wRevB, "v2"))
			e := mkEnv("eu", euSite, nil)
			before := fake.postCount()
			reconcile(e)

			c := cond(e, ConditionWaveReady)
			Expect(c.Reason).To(Equal(ReasonDuplicateSite))
			Expect(c.Message).To(ContainSubstring("owned by cluster cluster-other namespace team name eu-owner"))
			Expect(fake.postCount()).To(Equal(before))
			Expect(*gateAllowed(e)).To(BeEmpty())
			// A different site of the same tier is unaffected.
			other := mkEnv("us", "us-east-1", nil)
			reconcile(other)
			Expect(cond(other, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
		})

		It("adopts legacy deployments without site and owner as site default", func() {
			legacy := fake.addDeployment(taskGH, prodGH, wRevA, map[string]any{"id": "1", "version": map[string]any{"tag": "v1", "revision": wRevA}})
			fake.addStatus(legacy, stateSuccess, "Bake succeeded")
			mkRollout(histEntry(1, wRevA, "v1"), histEntry(2, wRevB, "v2"))
			e := mkEnv("plain", "", nil)
			reconcile(e)

			Expect(cond(e, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
			Expect(get(e).Status.Wave.Site).To(Equal(DefaultSiteName))
			deps := fake.deploymentsOf(prodGH)
			Expect(deps).To(HaveLen(2)) // history ID 1 re-used, ID 2 created
			Expect(deps[0].ID).To(Equal(legacy))
			var created map[string]any
			Expect(json.Unmarshal(decodedPayload(deps[1].Payload), &created)).To(Succeed())
			Expect(created).To(HaveKeyWithValue("id", "2"))
			Expect(created).To(HaveKeyWithValue("site", DefaultSiteName))
			Expect(created).To(HaveKeyWithValue("owner", HaveKeyWithValue("name", "plain")))
		})

		It("never matches a deployment of another owner by history ID", func() {
			other := seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevA, "v1", "Succeeded")
			fake.addStatus(other, "inactive", "Deregistered: site eu-west-1 removed")
			// Same history ID 1 as the other cluster's deployment.
			histSeq = 0
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
			deps := fake.deploymentsOf(prodGH)
			Expect(deps).To(HaveLen(2))
			Expect(deps[0].ID).To(Equal(other))
			Expect(deps[0].Statuses[len(deps[0].Statuses)-1].Description).To(HavePrefix(deregisteredPrefix))
		})

		It("frees the site when the other owner deregistered", func() {
			seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevA, "v1", "Succeeded")
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).To(Equal(ReasonDuplicateSite))

			dep := fake.deploymentsOf(prodGH)[0]
			fake.addStatus(dep.ID, "inactive", "Deregistered: site eu-west-1 removed")
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
			deps := fake.deploymentsOf(prodGH)
			Expect(deps).To(HaveLen(2))
			var p map[string]any
			Expect(json.Unmarshal(decodedPayload(deps[1].Payload), &p)).To(Succeed())
			Expect(p).To(HaveKeyWithValue("owner", HaveKeyWithValue("cluster", "cluster-self")))
		})

		It("does not post for a sibling tier whose site another cluster owns", func() {
			seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevA, "v1", "Succeeded")
			mkRollout(histEntry(1, wRevA, "v1"))
			stg := mkEnv("app-staging", euSite, nil)
			cur := get(stg)
			cur.Spec.Environment = "staging"
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
			mkEnv("app-prod", euSite, nil) // fresh: no stored DuplicateSite condition yet

			reconcile(stg)
			deps := fake.deploymentsOf(prodGH)
			Expect(deps).To(HaveLen(1), "prod/%s is owned by another cluster", euSite)
			Expect(deps[0].Statuses).To(HaveLen(1))
			Expect(fake.deploymentsOf(stagGH)).NotTo(BeEmpty())
		})

		It("marks only the registering owner's own deployment on deregistration", func() {
			// Winner A (self) registered first; loser B has the newer deployment.
			a := seedOwned(euSite, ownedBy("cluster-self", ns, "eu"), wRevA, "v1", "Succeeded")
			b := seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevB, "v2", "Succeeded")
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))

			Expect(k8sClient.Delete(ctx, get(e))).To(Succeed())
			reconcile(e)

			last := func(id int64) string {
				for _, d := range fake.deploymentsOf(prodGH) {
					if d.ID == id {
						return d.Statuses[len(d.Statuses)-1].Description
					}
				}
				return ""
			}
			Expect(last(a)).To(HavePrefix(deregisteredPrefix))
			Expect(last(b)).NotTo(HavePrefix(deregisteredPrefix))

			// B is now the only active owner.
			gh, err := rec.getGitHubClient(ctx, &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns},
				Spec:       kuberikv1alpha1.EnvironmentSpec{Backend: kuberikv1alpha1.BackendConfig{Type: "github", Project: "o/r", Secret: "github-token"}},
			})
			Expect(err).NotTo(HaveOccurred())
			conflict, err := rec.findSiteConflict(ctx, gh, "o", "r", ensureKuberikPrefix(wApp), "prod", euSite, deploymentOwner{Cluster: "cluster-other", Namespace: "team", Name: "eu-owner"})
			Expect(err).NotTo(HaveOccurred())
			Expect(conflict).To(BeNil())
		})

		It("takes over a site from another owner when annotated", func() {
			rec.Recorder = record.NewFakeRecorder(10)
			old := seedOwned(euSite, ownedBy("cluster-old", "team", "eu-owner"), wRevA, "v1", "Succeeded")
			mkRollout(histEntry(1, wRevA, "v1"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).To(Equal(ReasonDuplicateSite))
			Expect(rec.Recorder.(*record.FakeRecorder).Events).To(Receive(ContainSubstring("DuplicateSite")))

			cur := get(e)
			cur.Annotations = map[string]string{SiteTakeOverAnnotation: "true"}
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
			reconcile(e)

			Expect(cond(e, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
			deps := fake.deploymentsOf(prodGH)
			Expect(deps).To(HaveLen(2))
			Expect(deps[0].ID).To(Equal(old))
			marker := deps[0].Statuses[len(deps[0].Statuses)-1]
			Expect(marker.Description).To(HavePrefix(deregisteredPrefix))
			Expect(marker.Description).To(Equal(fmt.Sprintf("Deregistered: site %s taken over by cluster-self/%s/eu", euSite, ns)))
			var p map[string]any
			Expect(json.Unmarshal(decodedPayload(deps[1].Payload), &p)).To(Succeed())
			Expect(p).To(HaveKeyWithValue("owner", HaveKeyWithValue("cluster", "cluster-self")))
			Expect(rec.Recorder.(*record.FakeRecorder).Events).To(Receive(ContainSubstring("SiteTakenOver")))
		})

		It("lets the first registration win a race", func() {
			// Both registered; the other owner's first deployment is older.
			seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevA, "v1", "Succeeded")
			seedOwned(euSite, ownedBy("cluster-self", ns, "eu"), wRevB, "v2", "Succeeded")
			mkRollout(histEntry(1, wRevB, "v2"))
			e := mkEnv("eu", euSite, nil)
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).To(Equal(ReasonDuplicateSite))

			// The mirror image: self registered first, so the other one is the duplicate.
			fake.deps = nil
			seedOwned(euSite, ownedBy("cluster-self", ns, "eu"), wRevA, "v1", "Succeeded")
			seedOwned(euSite, ownedBy("cluster-other", "team", "eu-owner"), wRevB, "v2", "Succeeded")
			reconcile(e)
			Expect(cond(e, ConditionWaveReady).Reason).NotTo(Equal(ReasonDuplicateSite))
		})
	})

	It("accepts the deprecated spec.relationship, ignores it and reports Deprecated", func() {
		seedStaging()
		mkRollout(histEntry(1, wRevA, "v1"))
		e := mkEnv("eu", euSite, nil)
		cur := get(e)
		cur.Spec.Relationship = &kuberikv1alpha1.EnvironmentRelationship{Environment: "staging", Type: kuberikv1alpha1.RelationshipTypeParallel}
		Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		reconcile(e)

		c := cond(e, ConditionDeprecated)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(ReasonRelationshipIgnored))
		Expect(c.Message).To(Equal("spec.relationship is ignored; environment order is fixed (dev, test, staging, prod)"))
		// The gate is still computed from the fixed tier order.
		Expect(*gateAllowed(e)).To(ConsistOf("v1", "v2"))
		Expect(cond(e, ConditionWaveReady).Status).To(Equal(metav1.ConditionTrue))

		cur = get(e)
		cur.Spec.Relationship = nil
		Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		reconcile(e)
		Expect(cond(e, ConditionDeprecated)).To(BeNil())
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

// clusterID reads kube-system through APIReader when set (the cached client has
// no Namespace informer in the manager) and does not cache errors.
func TestClusterIDUsesAPIReader(t *testing.T) {
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: metav1.NamespaceSystem, UID: "uid-123"}}
	// Client is nil: touching it would panic.
	r := &GitHubEnvironmentReconciler{APIReader: fakeclient.NewClientBuilder().WithScheme(sch).Build()}
	if _, err := r.clusterID(context.Background()); err == nil {
		t.Fatal("expected an error when kube-system is missing")
	}
	if r.ClusterID != "" {
		t.Fatal("error must not be cached")
	}
	r.APIReader = fakeclient.NewClientBuilder().WithScheme(sch).WithObjects(ns).Build()
	id, err := r.clusterID(context.Background())
	if err != nil || id != "uid-123" {
		t.Fatalf("clusterID = %q, %v", id, err)
	}
}

// namespaceTerminating must read through APIReader: the manager cache has no
// Namespace informer and RBAC only grants get, so a cached Get would block
// forever on the failing informer list.
func TestNamespaceTerminatingUsesAPIReader(t *testing.T) {
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "gone", DeletionTimestamp: &metav1.Time{Time: time.Now()}, Finalizers: []string{"x"}}}
	// Client is nil: touching it would panic.
	r := &GitHubEnvironmentReconciler{APIReader: fakeclient.NewClientBuilder().WithScheme(sch).WithObjects(ns).Build()}
	got, err := r.namespaceTerminating(context.Background(), "gone")
	if err != nil || !got {
		t.Fatalf("namespaceTerminating = %v, %v; want true, nil", got, err)
	}
}

type blockingReader struct{ client.Reader }

func (blockingReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}

// A stuck API read must time out instead of freezing the single worker, and is
// reported as "not terminating" with the error.
func TestNamespaceTerminatingTimesOut(t *testing.T) {
	old := namespaceReadTimeout
	namespaceReadTimeout = 50 * time.Millisecond
	defer func() { namespaceReadTimeout = old }()
	r := &GitHubEnvironmentReconciler{APIReader: blockingReader{}}
	done := make(chan struct{})
	var got bool
	var err error
	go func() { got, err = r.namespaceTerminating(context.Background(), "x"); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("namespaceTerminating did not respect its timeout")
	}
	if got || err == nil {
		t.Fatalf("got %v, %v; want false and an error", got, err)
	}
}

// Finalizers are added and removed with a merge patch, not a full-object
// Update: an object that fails full-object validation (e.g. a spec.environment
// outside the new enum) must still get its finalizer handled.
func TestFinalizerUsesPatchNotUpdate(t *testing.T) {
	sch := runtime.NewScheme()
	if err := kuberikv1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	env := &kuberikv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns"}, Spec: kuberikv1alpha1.EnvironmentSpec{Environment: "canary"}}
	var patches int
	c := fakeclient.NewClientBuilder().WithScheme(sch).WithObjects(env).WithInterceptorFuncs(interceptor.Funcs{
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			return fmt.Errorf("spec.environment: Unsupported value")
		},
		Patch: func(ctx context.Context, c client.WithWatch, o client.Object, p client.Patch, opts ...client.PatchOption) error {
			patches++
			return c.Patch(ctx, o, p, opts...)
		},
	}).Build()
	r := &GitHubEnvironmentReconciler{Client: c}
	ctx := context.Background()
	cur := &kuberikv1alpha1.Environment{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(env), cur); err != nil {
		t.Fatal(err)
	}
	if err := r.addFinalizer(ctx, cur); err != nil {
		t.Fatalf("addFinalizer: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(env), cur); err != nil || !controllerutil.ContainsFinalizer(cur, DeregisterFinalizer) {
		t.Fatalf("finalizer not added: %v", err)
	}
	if err := r.removeFinalizer(ctx, cur); err != nil {
		t.Fatalf("removeFinalizer: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(env), cur); err != nil || controllerutil.ContainsFinalizer(cur, DeregisterFinalizer) {
		t.Fatalf("finalizer not removed: %v", err)
	}
	if patches != 2 {
		t.Fatalf("patches = %d, want 2", patches)
	}
}
