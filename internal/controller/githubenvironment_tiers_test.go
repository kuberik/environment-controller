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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/google/go-github/v62/github"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	k8sptr "k8s.io/utils/ptr"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

var _ = Describe("Environment tiers", func() {
	const namespace = "default"

	var reconciler *GitHubEnvironmentReconciler

	BeforeEach(func() {
		reconciler = &GitHubEnvironmentReconciler{
			Client: k8sClient,
			Scheme: scheme.Scheme,
		}
	})

	// fakeDeployment is a GitHub deployment as returned by the REST API
	type fakeDeployment struct {
		ID          int64           `json:"id"`
		Ref         string          `json:"ref"`
		Task        string          `json:"task"`
		Environment string          `json:"environment"`
		Payload     json.RawMessage `json:"payload"`
	}

	// newFakeGitHub serves the deployment endpoints buildRelationshipGraph reads, with every
	// deployment reporting a successful bake.
	newFakeGitHub := func(deployments []fakeDeployment) *github.Client {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			path := strings.TrimPrefix(req.URL.Path, "/repos/kuberik/environment-controller-testing/deployments")
			switch {
			case path == "":
				result := []fakeDeployment{}
				for _, d := range deployments {
					if env := req.URL.Query().Get("environment"); env != "" && env != d.Environment {
						continue
					}
					if ref := req.URL.Query().Get("ref"); ref != "" && ref != d.Ref {
						continue
					}
					result = append(result, d)
				}
				_ = json.NewEncoder(w).Encode(result)
			case strings.HasSuffix(path, "/statuses"):
				_, _ = w.Write([]byte(`[{"id":1,"state":"success","description":"Bake succeeded","environment_url":"https://eu.example.com"}]`))
			default:
				http.NotFound(w, req)
			}
		}))
		DeferCleanup(server.Close)

		client := github.NewClient(server.Client())
		baseURL, err := url.Parse(server.URL + "/")
		Expect(err).ToNot(HaveOccurred())
		client.BaseURL = baseURL
		return client
	}

	Context("CRD validation", func() {
		newEnvironment := func(name, environment string) *kuberikv1alpha1.Environment {
			return &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef:  corev1.LocalObjectReference{Name: "some-rollout"},
					Name:        "tier-validation",
					Environment: environment,
					Backend:     kuberikv1alpha1.BackendConfig{Type: "github", Project: "kuberik/environment-controller-testing"},
				},
			}
		}

		DescribeTable("accepts exactly the tier names",
			func(environment string) {
				env := newEnvironment("tier-validation-"+environment, environment)
				Expect(k8sClient.Create(context.Background(), env)).To(Succeed())
				DeferCleanup(func() { Expect(k8sClient.Delete(context.Background(), env)).To(Succeed()) })
			},
			Entry("dev", "dev"), Entry("development", "development"),
			Entry("test", "test"), Entry("qa", "qa"),
			Entry("staging", "staging"), Entry("stage", "stage"), Entry("preprod", "preprod"),
			Entry("prod", "prod"), Entry("production", "production"),
		)

		DescribeTable("rejects anything else",
			func(environment string) {
				err := k8sClient.Create(context.Background(), newEnvironment("tier-validation-bad", environment))
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got %v", err)
			},
			Entry("a region suffix", "prod-eu-1"),
			Entry("an unknown name", "canary"),
			Entry("an empty name", ""),
			Entry("a different case", "Prod"),
		)
	})

	Context("upstream gating", func() {
		deployment := func(id int64, rev, environment, site string) fakeDeployment {
			payload := `{"id":"` + "1" + `","version":{"tag":"v1","revision":"` + rev + `"}}`
			if site != "" {
				payload = `{"id":"1","site":"` + site + `","wave":1,"version":{"tag":"v1","revision":"` + rev + `"}}`
			}
			return fakeDeployment{ID: id, Ref: rev, Task: "deploy:kuberik/tier-gate", Environment: "kuberik/tier-gate/" + environment, Payload: json.RawMessage(payload)}
		}

		DescribeTable("findUpstream picks the nearest earlier tier present",
			func(self string, deployments []fakeDeployment, want string) {
				ghClient := newFakeGitHub(deployments)
				got, _, err := reconciler.findUpstream(context.Background(), ghClient, "kuberik", "environment-controller-testing", "kuberik/tier-gate", self)
				Expect(err).ToNot(HaveOccurred())
				Expect(got).To(Equal(want))
			},
			Entry("prod looks at staging first", "prod",
				[]fakeDeployment{deployment(1, "rev-1", "staging", ""), deployment(2, "rev-1", "dev", "")}, "staging"),
			Entry("aliases count as the tier", "production",
				[]fakeDeployment{deployment(1, "rev-1", "preprod", "")}, "staging"),
			Entry("empty tiers are skipped", "prod",
				[]fakeDeployment{deployment(1, "rev-1", "dev", "")}, "dev"),
			Entry("a sited tier counts when registered", "prod",
				[]fakeDeployment{deployment(1, "rev-1", "qa", "eu-west-1")}, "test"),
			Entry("a later tier is never an upstream", "dev",
				[]fakeDeployment{deployment(1, "rev-1", "prod", "")}, ""),
			Entry("no earlier tier present", "staging",
				[]fakeDeployment{deployment(1, "rev-1", "staging", "")}, ""),
		)

		gateFor := func(name, environment string, deployments []fakeDeployment) *kuberikrolloutv1alpha1.RolloutGate {
			ctx := context.Background()
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec:       kuberikrolloutv1alpha1.RolloutSpec{ReleasesImagePolicy: corev1.LocalObjectReference{Name: "test-policy"}},
			}
			Expect(k8sClient.Create(ctx, rollout)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, rollout)).To(Succeed()) })
			rels := make([]kuberikrolloutv1alpha1.VersionInfo, 0, 2)
			for i, tag := range []string{"v1", "v2"} {
				rels = append(rels, kuberikrolloutv1alpha1.VersionInfo{Tag: tag, Revision: k8sptr.To("rev-" + string(rune('1'+i)))})
			}
			var err error
			rollout.Status.Candidates, rollout.Status.ReleaseSource, err = publishCatalogue(ctx, k8sClient, namespace, name+"-src", rels)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Status().Update(ctx, rollout)).To(Succeed())

			env := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef:  corev1.LocalObjectReference{Name: name},
					Name:        "tier-gate",
					Environment: environment,
					Backend:     kuberikv1alpha1.BackendConfig{Type: "github", Project: "kuberik/environment-controller-testing"},
				},
			}
			Expect(k8sClient.Create(ctx, env)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, env)).To(Succeed()) })
			Expect(reconciler.createOrUpdateRolloutGate(ctx, env)).To(Succeed())
			Expect(reconciler.reconcileWave(ctx, newFakeGitHub(deployments), env, rollout)).To(Succeed())

			gate := &kuberikrolloutv1alpha1.RolloutGate{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: env.Status.RolloutGateRef.Name, Namespace: namespace}, gate)).To(Succeed())
			return gate
		}

		It("Should only pass versions the earlier tier baked successfully (alias on both sides)", func() {
			gate := gateFor("tier-gate-after", "production", []fakeDeployment{deployment(1, "rev-1", "stage", "")})
			Expect(gate.Spec.AllowedVersions).ToNot(BeNil())
			Expect(*gate.Spec.AllowedVersions).To(ConsistOf("v1"))
		})

		It("Should leave the gate unrestricted when no earlier tier is present", func() {
			gate := gateFor("tier-gate-none", "staging", []fakeDeployment{deployment(1, "rev-1", "staging", "")})
			Expect(gate.Spec.AllowedVersions).To(BeNil())
		})
	})

	Context("environment graph", func() {
		It("Should key environments by canonical tier and treat aliases as one", func() {
			ctx := context.Background()
			ghClient := newFakeGitHub([]fakeDeployment{
				{ID: 7, Ref: "rev-1", Task: "deploy:kuberik/tier-graph", Environment: "kuberik/tier-graph/production", Payload: json.RawMessage(`{"id":"1","version":{"tag":"v1","revision":"rev-1"}}`)},
				{ID: 8, Ref: "rev-2", Task: "deploy:kuberik/tier-graph", Environment: "kuberik/tier-graph/prod", Payload: json.RawMessage(`{"id":"2","version":{"tag":"v2","revision":"rev-2"}}`)},
				{ID: 5, Ref: "rev-1", Task: "deploy:kuberik/tier-graph", Environment: "kuberik/tier-graph/stage", Payload: json.RawMessage(`{"id":"1","version":{"tag":"v1","revision":"rev-1"}}`)},
			})
			env := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "tier-graph", Namespace: namespace},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef:  corev1.LocalObjectReference{Name: "tier-graph"},
					Name:        "tier-graph",
					Environment: "production",
					Backend:     kuberikv1alpha1.BackendConfig{Type: "github", Project: "kuberik/environment-controller-testing"},
				},
			}
			Expect(k8sClient.Create(ctx, env)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, env)).To(Succeed()) })

			graphData, err := reconciler.buildEnvironmentGraph(ctx, env, ghClient)
			Expect(err).ToNot(HaveOccurred())
			Expect(graphData.environmentInfos).To(HaveKey("prod"))
			Expect(graphData.environmentInfos).To(HaveKey("staging"))
			Expect(graphData.environmentInfos).ToNot(HaveKey("production"))
			Expect(graphData.environmentInfos).ToNot(HaveKey("stage"))
		})
	})
})
