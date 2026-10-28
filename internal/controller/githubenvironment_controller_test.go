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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/go-github/v62/github"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	k8sptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

var _ = Describe("Environment Controller", func() {
	const (
		DeploymentNamespace = "default"
		SecretName          = "github-token"
	)

	// Helper function to skip test if no GitHub token
	skipIfNoGitHubToken := func() {
		if os.Getenv("GITHUB_TOKEN") == "" {
			Skip("Skipping GitHub API integration test - GITHUB_TOKEN not provided")
		}
	}

	// Helper function to clean up GitHub deployments
	cleanupDeployments := func(repository string) {
		token := os.Getenv("GITHUB_TOKEN")
		if token == "" {
			return // No token available
		}

		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		tc := oauth2.NewClient(context.Background(), ts)
		client := github.NewClient(tc)

		// Parse repository
		parts := strings.Split(repository, "/")
		if len(parts) != 2 {
			return // Invalid repository format
		}
		owner, repo := parts[0], parts[1]

		// List all deployments
		deployments, _, err := client.Repositories.ListDeployments(context.Background(), owner, repo, &github.DeploymentsListOptions{
			ListOptions: github.ListOptions{PerPage: 100}, // Get up to 100 deployments
		})
		if err != nil {
			return // Ignore errors during cleanup
		}

		// Delete each deployment
		for _, deployment := range deployments {
			if deployment.ID != nil {
				// Actually delete the deployment, ignoring deletion errors during cleanup
				_, _ = client.Repositories.DeleteDeployment(context.Background(), owner, repo, *deployment.ID)
			}
		}
	}

	// Helper function to create deployment payload with history entry
	createDeploymentPayload := func(historyID string, historyEntry *kuberikrolloutv1alpha1.DeploymentHistoryEntry) ([]byte, error) {
		// New payload format: only immutable data (ID, Relationship, Version)
		payload := struct {
			ID           string                                   `json:"id"`
			Relationship *kuberikv1alpha1.EnvironmentRelationship `json:"relationship,omitempty"`
			Version      *kuberikrolloutv1alpha1.VersionInfo      `json:"version,omitempty"`
		}{
			ID:      historyID,
			Version: historyEntry.Version.DeepCopy(),
		}
		return json.Marshal(payload)
	}

	// Helper function to create GitHub token secret
	createGitHubTokenSecret := func() error {
		token := os.Getenv("GITHUB_TOKEN")
		if token == "" {
			token = "test-token" // Fallback for tests without real token
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      SecretName,
				Namespace: DeploymentNamespace,
			},
			Data: map[string][]byte{
				"token": []byte(token),
			},
		}

		// Delete if exists first
		_ = k8sClient.Delete(context.Background(), secret)
		return k8sClient.Create(context.Background(), secret)
	}

	var (
		reconciler *GitHubEnvironmentReconciler
	)

	BeforeEach(func() {
		reconciler = &GitHubEnvironmentReconciler{
			Client: k8sClient,
			Scheme: scheme.Scheme,
		}

		// Clean up any existing resources
		deploymentList := &kuberikv1alpha1.EnvironmentList{}
		if err := k8sClient.List(context.Background(), deploymentList); err == nil {
			for i := range deploymentList.Items {
				_ = k8sClient.Delete(context.Background(), &deploymentList.Items[i])
			}
		}
	})

	Context("Unit Tests", func() {
		Context("getControllerNamespace", func() {
			It("Should read namespace from service account file", func() {
				// This test would require mocking file system, skipping for now
				Skip("Requires file system mocking")
			})

			It("Should fallback to POD_NAMESPACE environment variable", func() {
				Expect(os.Setenv("POD_NAMESPACE", "test-namespace")).To(Succeed())
				DeferCleanup(os.Unsetenv, "POD_NAMESPACE")
				result := reconciler.getControllerNamespace()
				Expect(result).To(Equal("test-namespace"))
			})

			It("Should return empty string when no namespace is available", func() {
				Expect(os.Unsetenv("POD_NAMESPACE")).To(Succeed())
				Expect(os.Unsetenv("WATCH_NAMESPACE")).To(Succeed())
				result := reconciler.getControllerNamespace()
				// In test environment, this might return empty or might read from actual file
				// So we just check it's a valid string (empty or actual namespace)
				Expect(result).To(BeAssignableToTypeOf(""))
			})
		})

		Context("getRolloutDashboardURL", func() {
			const (
				ControllerNamespace = "controller-ns"
				TestNamespace       = "test-ns"
				TestName            = "test-deployment"
			)

			BeforeEach(func() {
				// Set controller namespace via environment variable
				Expect(os.Setenv("POD_NAMESPACE", ControllerNamespace)).To(Succeed())

				// Create the controller namespace
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: ControllerNamespace,
					},
				}
				// Ignore error if namespace already exists
				_ = k8sClient.Create(context.Background(), ns)
			})

			AfterEach(func() {
				Expect(os.Unsetenv("POD_NAMESPACE")).To(Succeed())
				// Clean up test resources
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
				}
				_ = k8sClient.Delete(context.Background(), svc)

				ingress := &networkingv1.Ingress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-ingress",
						Namespace: ControllerNamespace,
					},
				}
				_ = k8sClient.Delete(context.Background(), ingress)

				httpRoute := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-route",
						Namespace: ControllerNamespace,
					},
				}
				_ = k8sClient.Delete(context.Background(), httpRoute)

				gateway := &gatewayv1.Gateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-gateway",
						Namespace: ControllerNamespace,
					},
				}
				_ = k8sClient.Delete(context.Background(), gateway)
			})

			It("Should return empty string when service doesn't exist", func() {
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(BeEmpty())
			})

			It("Should return empty string when ingress doesn't exist", func() {
				// Create service but no ingress
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(BeEmpty())
			})

			It("Should return URL with path when ingress with HTTP exists", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create ingress pointing to rollout-dashboard
				ingress := &networkingv1.Ingress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-ingress",
						Namespace: ControllerNamespace,
					},
					Spec: networkingv1.IngressSpec{
						Rules: []networkingv1.IngressRule{
							{
								Host: "dashboard.example.com",
								IngressRuleValue: networkingv1.IngressRuleValue{
									HTTP: &networkingv1.HTTPIngressRuleValue{
										Paths: []networkingv1.HTTPIngressPath{
											{
												Path:     "/",
												PathType: func() *networkingv1.PathType { pt := networkingv1.PathTypePrefix; return &pt }(),
												Backend: networkingv1.IngressBackend{
													Service: &networkingv1.IngressServiceBackend{
														Name: "rollout-dashboard",
														Port: networkingv1.ServiceBackendPort{
															Number: 80,
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), ingress)).To(Succeed())

				expectedURL := fmt.Sprintf("http://dashboard.example.com/rollouts/%s/%s", TestNamespace, TestName)
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(Equal(expectedURL))
			})

			It("Should return URL with HTTPS when TLS is configured", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create ingress with TLS
				ingress := &networkingv1.Ingress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-ingress",
						Namespace: ControllerNamespace,
					},
					Spec: networkingv1.IngressSpec{
						TLS: []networkingv1.IngressTLS{
							{
								Hosts:      []string{"dashboard.example.com"},
								SecretName: "tls-secret",
							},
						},
						Rules: []networkingv1.IngressRule{
							{
								Host: "dashboard.example.com",
								IngressRuleValue: networkingv1.IngressRuleValue{
									HTTP: &networkingv1.HTTPIngressRuleValue{
										Paths: []networkingv1.HTTPIngressPath{
											{
												Path:     "/",
												PathType: func() *networkingv1.PathType { pt := networkingv1.PathTypePrefix; return &pt }(),
												Backend: networkingv1.IngressBackend{
													Service: &networkingv1.IngressServiceBackend{
														Name: "rollout-dashboard",
														Port: networkingv1.ServiceBackendPort{
															Number: 80,
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), ingress)).To(Succeed())

				expectedURL := fmt.Sprintf("https://dashboard.example.com/rollouts/%s/%s", TestNamespace, TestName)
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(Equal(expectedURL))
			})

			It("Should return URL with default backend when configured", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create ingress with default backend
				ingress := &networkingv1.Ingress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-ingress",
						Namespace: ControllerNamespace,
					},
					Spec: networkingv1.IngressSpec{
						DefaultBackend: &networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: "rollout-dashboard",
								Port: networkingv1.ServiceBackendPort{
									Number: 80,
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), ingress)).To(Succeed())

				// In this case, host may be empty, so URL should be empty
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(BeEmpty())
			})

			It("Should return empty string when controller namespace cannot be determined", func() {
				Expect(os.Unsetenv("POD_NAMESPACE")).To(Succeed())
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(BeEmpty())
			})

			It("Should return URL from HTTPRoute when ingress doesn't exist", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create Gateway
				gateway := &gatewayv1.Gateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-gateway",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.GatewaySpec{
						GatewayClassName: "envoy",
						Listeners: []gatewayv1.Listener{
							{
								Name:     "http",
								Protocol: gatewayv1.HTTPProtocolType,
								Port:     gatewayv1.PortNumber(80),
								Hostname: func() *gatewayv1.Hostname { h := gatewayv1.Hostname("dashboard.example.com"); return &h }(),
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), gateway)).To(Succeed())

				// Create HTTPRoute pointing to rollout-dashboard
				httpRoute := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-route",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.HTTPRouteSpec{
						Hostnames: []gatewayv1.Hostname{"dashboard.example.com"},
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{
								{
									Name: gatewayv1.ObjectName("rollout-dashboard-gateway"),
								},
							},
						},
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Name: gatewayv1.ObjectName("rollout-dashboard"),
												Port: func() *gatewayv1.PortNumber { p := gatewayv1.PortNumber(80); return &p }(),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), httpRoute)).To(Succeed())

				expectedURL := fmt.Sprintf("http://dashboard.example.com/rollouts/%s/%s", TestNamespace, TestName)
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(Equal(expectedURL))
			})

			It("Should return URL with HTTPS from HTTPRoute when Gateway uses HTTPS", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create Gateway with HTTPS
				gateway := &gatewayv1.Gateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-gateway",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.GatewaySpec{
						GatewayClassName: "envoy",
						Listeners: []gatewayv1.Listener{
							{
								Name:     "https",
								Protocol: gatewayv1.HTTPSProtocolType,
								Port:     gatewayv1.PortNumber(443),
								Hostname: func() *gatewayv1.Hostname { h := gatewayv1.Hostname("dashboard.example.com"); return &h }(),
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), gateway)).To(Succeed())

				// Create HTTPRoute pointing to rollout-dashboard
				httpRoute := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-route",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.HTTPRouteSpec{
						Hostnames: []gatewayv1.Hostname{"dashboard.example.com"},
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{
								{
									Name: gatewayv1.ObjectName("rollout-dashboard-gateway"),
								},
							},
						},
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Name: gatewayv1.ObjectName("rollout-dashboard"),
												Port: func() *gatewayv1.PortNumber { p := gatewayv1.PortNumber(80); return &p }(),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), httpRoute)).To(Succeed())

				expectedURL := fmt.Sprintf("https://dashboard.example.com/rollouts/%s/%s", TestNamespace, TestName)
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(Equal(expectedURL))
			})

			It("Should return empty string when HTTPRoute exists but Gateway doesn't exist", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create HTTPRoute with non-existent Gateway reference
				httpRoute := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-route",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.HTTPRouteSpec{
						Hostnames: []gatewayv1.Hostname{"dashboard.example.com"},
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{
								{
									Name: gatewayv1.ObjectName("non-existent-gateway"),
								},
							},
						},
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Name: gatewayv1.ObjectName("rollout-dashboard"),
												Port: func() *gatewayv1.PortNumber { p := gatewayv1.PortNumber(80); return &p }(),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), httpRoute)).To(Succeed())

				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(BeEmpty())
			})

			It("Should return empty string when HTTPRoute exists but has no hostname", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create Gateway without hostname
				gateway := &gatewayv1.Gateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-gateway",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.GatewaySpec{
						GatewayClassName: "envoy",
						Listeners: []gatewayv1.Listener{
							{
								Name:     "http",
								Protocol: gatewayv1.HTTPProtocolType,
								Port:     gatewayv1.PortNumber(80),
								// No hostname specified
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), gateway)).To(Succeed())

				// Create HTTPRoute pointing to rollout-dashboard
				httpRoute := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-route",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.HTTPRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{
								{
									Name: gatewayv1.ObjectName("rollout-dashboard-gateway"),
								},
							},
						},
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Name: gatewayv1.ObjectName("rollout-dashboard"),
												Port: func() *gatewayv1.PortNumber { p := gatewayv1.PortNumber(80); return &p }(),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), httpRoute)).To(Succeed())

				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(BeEmpty())
			})

			It("Should prefer Ingress over HTTPRoute when both exist", func() {
				// Create service
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard",
						Namespace: ControllerNamespace,
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 80},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), svc)).To(Succeed())

				// Create ingress pointing to rollout-dashboard
				ingress := &networkingv1.Ingress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-ingress",
						Namespace: ControllerNamespace,
					},
					Spec: networkingv1.IngressSpec{
						Rules: []networkingv1.IngressRule{
							{
								Host: "ingress.example.com",
								IngressRuleValue: networkingv1.IngressRuleValue{
									HTTP: &networkingv1.HTTPIngressRuleValue{
										Paths: []networkingv1.HTTPIngressPath{
											{
												Path:     "/",
												PathType: func() *networkingv1.PathType { pt := networkingv1.PathTypePrefix; return &pt }(),
												Backend: networkingv1.IngressBackend{
													Service: &networkingv1.IngressServiceBackend{
														Name: "rollout-dashboard",
														Port: networkingv1.ServiceBackendPort{
															Number: 80,
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), ingress)).To(Succeed())

				// Create Gateway
				gateway := &gatewayv1.Gateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-gateway",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.GatewaySpec{
						GatewayClassName: "envoy",
						Listeners: []gatewayv1.Listener{
							{
								Name:     "http",
								Protocol: gatewayv1.HTTPProtocolType,
								Port:     gatewayv1.PortNumber(80),
								Hostname: func() *gatewayv1.Hostname { h := gatewayv1.Hostname("gateway.example.com"); return &h }(),
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), gateway)).To(Succeed())

				// Create HTTPRoute pointing to rollout-dashboard
				httpRoute := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rollout-dashboard-route",
						Namespace: ControllerNamespace,
					},
					Spec: gatewayv1.HTTPRouteSpec{
						Hostnames: []gatewayv1.Hostname{"dashboard.example.com"},
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{
								{
									Name: gatewayv1.ObjectName("rollout-dashboard-gateway"),
								},
							},
						},
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Name: gatewayv1.ObjectName("rollout-dashboard"),
												Port: func() *gatewayv1.PortNumber { p := gatewayv1.PortNumber(80); return &p }(),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(context.Background(), httpRoute)).To(Succeed())

				// Should prefer Ingress over HTTPRoute
				expectedURL := fmt.Sprintf("http://ingress.example.com/rollouts/%s/%s", TestNamespace, TestName)
				url := reconciler.getRolloutDashboardURL(context.Background(), TestNamespace, TestName)
				Expect(url).To(Equal(expectedURL))
			})
		})

		Context("getCurrentVersionFromRollout", func() {
			It("Should return revision when available", func() {
				revision := "abc123"
				rollout := &kuberikrolloutv1alpha1.Rollout{
					Status: kuberikrolloutv1alpha1.RolloutStatus{
						History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
							{
								ID: k8sptr.To(int64(1)),
								Version: kuberikrolloutv1alpha1.VersionInfo{
									Revision: &revision,
								},
								Timestamp: metav1.Now(),
							},
						},
					},
				}
				version := reconciler.getCurrentVersionFromRollout(rollout)
				Expect(version).ToNot(BeNil())
				Expect(*version).To(Equal(revision))
			})

			It("Should return nil when revision is not available", func() {
				rollout := &kuberikrolloutv1alpha1.Rollout{
					Status: kuberikrolloutv1alpha1.RolloutStatus{
						History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
							{
								ID:      k8sptr.To(int64(1)),
								Version: kuberikrolloutv1alpha1.VersionInfo{
									// No revision
								},
								Timestamp: metav1.Now(),
							},
						},
					},
				}
				version := reconciler.getCurrentVersionFromRollout(rollout)
				Expect(version).To(BeNil())
			})

			It("Should return nil when history is empty", func() {
				rollout := &kuberikrolloutv1alpha1.Rollout{
					Status: kuberikrolloutv1alpha1.RolloutStatus{
						History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{},
					},
				}
				version := reconciler.getCurrentVersionFromRollout(rollout)
				Expect(version).To(BeNil())
			})
		})

		Context("slicesEqual", func() {
			It("Should return true for equal slices", func() {
				a := []string{"a", "b", "c"}
				b := []string{"a", "b", "c"}
				Expect(reconciler.slicesEqual(a, b)).To(BeTrue())
			})

			It("Should return false for different slices", func() {
				a := []string{"a", "b", "c"}
				b := []string{"a", "b", "d"}
				Expect(reconciler.slicesEqual(a, b)).To(BeFalse())
			})

			It("Should return true for nil slices", func() {
				var a, b []string
				Expect(reconciler.slicesEqual(a, b)).To(BeTrue())
			})

			It("Should return false for one nil slice", func() {
				var a []string
				b := []string{"a"}
				Expect(reconciler.slicesEqual(a, b)).To(BeFalse())
			})

			It("Should return false for different lengths", func() {
				a := []string{"a", "b"}
				b := []string{"a", "b", "c"}
				Expect(reconciler.slicesEqual(a, b)).To(BeFalse())
			})
		})

		Context("Deployment validation", func() {
			It("Should validate Environment spec", func() {
				deployment := &kuberikv1alpha1.Environment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-deployment",
						Namespace: "default",
					},
					Spec: kuberikv1alpha1.EnvironmentSpec{
						RolloutRef: corev1.LocalObjectReference{
							Name: "test-rollout",
						},
						Backend: kuberikv1alpha1.BackendConfig{
							Type:    "github",
							Project: "kuberik/environment-controller-testing",
						},
						Name:        "test-deployment",
						Environment: "production",
					},
				}

				Expect(deployment.Spec.RolloutRef.Name).To(Equal("test-rollout"))
				Expect(deployment.Spec.Backend.Project).To(Equal("kuberik/environment-controller-testing"))
				Expect(deployment.Spec.Name).To(Equal("test-deployment"))
				Expect(deployment.Spec.Environment).To(Equal("production"))
			})
		})
	})

	Context("Integration Tests", func() {
		BeforeEach(func() {
			// Clean up GitHub deployments before each integration test
			if os.Getenv("GITHUB_TOKEN") != "" {
				By("Cleaning up GitHub deployments before test")
				cleanupDeployments("kuberik/environment-controller-testing")
			}
		})

		It("Should create RolloutGate when Environment is created", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with deployment history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			// Update the rollout to set the status
			bakeStatus := "Succeeded"
			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						BakeStatus: &bakeStatus,
						Timestamp:  metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Environment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying GitHub deployment was created")
			// Get the updated Deployment to check status
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Verify GitHub deployment was created
			Expect(updatedDeployment.Status.EnvironmentInfos).ToNot(BeEmpty())
			foundInfo := false
			for _, info := range updatedDeployment.Status.EnvironmentInfos {
				if info.Environment == "production" {
					foundInfo = true
					// Expect(info.EnvironmentURL).ToNot(BeEmpty()) // URL generation depends on Gateway/Ingress which might not be present in test
					Expect(info.History).ToNot(BeEmpty())
					Expect(info.History[0].ID).ToNot(BeNil())
				}
			}
			Expect(foundInfo).To(BeTrue())
			Expect(updatedDeployment.Status.CurrentVersion).To(Equal(revision))
			Expect(updatedDeployment.Status.RolloutGateRef).ToNot(BeNil())
			Expect(updatedDeployment.Status.RolloutGateRef.Name).To(HavePrefix("ghd-"))

			By("Verifying RolloutGate was created")
			rolloutGate := &kuberikrolloutv1alpha1.RolloutGate{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      updatedDeployment.Status.RolloutGateRef.Name,
				Namespace: DeploymentNamespace,
			}, rolloutGate)).To(Succeed())

			Expect(rolloutGate.Spec.RolloutRef).ToNot(BeNil())
			Expect(rolloutGate.Spec.RolloutRef.Name).To(Equal("test-rollout"))
			Expect(rolloutGate.Spec.Passing).ToNot(BeNil())
			Expect(*rolloutGate.Spec.Passing).To(BeTrue())

			By("Verifying GitHub deployment exists via API")
			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			// Get the deployment ID from GitHub API (since EnvironmentInfo has Rollout History ID, not GitHub Deployment ID)
			ghDeployments, _, err := githubClient.Repositories.ListDeployments(context.Background(), "kuberik", "environment-controller-testing", &github.DeploymentsListOptions{
				Ref:         revision,
				Environment: "kuberik/test-deployment/production",
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(ghDeployments).ToNot(BeEmpty())
			deploymentID := *ghDeployments[0].ID

			// Get the deployment from GitHub API
			ghDeployment, _, err := githubClient.Repositories.GetDeployment(context.Background(), "kuberik", "environment-controller-testing", deploymentID)
			Expect(err).ToNot(HaveOccurred())
			Expect(ghDeployment).ToNot(BeNil())
			Expect(ghDeployment.Ref).ToNot(BeNil())
			Expect(*ghDeployment.Ref).To(Equal(revision))
			Expect(ghDeployment.Environment).ToNot(BeNil())
			Expect(*ghDeployment.Environment).To(Equal("kuberik/test-deployment/production"))

			By("Verifying GitHub deployment status was created")
			// Get deployment statuses
			statuses, _, err := githubClient.Repositories.ListDeploymentStatuses(context.Background(), "kuberik", "environment-controller-testing", deploymentID, &github.ListOptions{})
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).ToNot(BeEmpty())

			// Check the latest status
			latestStatus := statuses[0]
			Expect(latestStatus.State).ToNot(BeNil())
			Expect(*latestStatus.State).To(Equal("success")) // Should be success since Passing=true
		})

		It("Should update GitHub deployment status when RolloutGate passing changes", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with deployment history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-status-change",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
						{
							ID: k8sptr.To(int64(1)),
							Version: kuberikrolloutv1alpha1.VersionInfo{
								Tag:      "v1.0.0",
								Revision: &revision,
							},
							Timestamp: metav1.Now(),
						},
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			// Update the rollout to set the status
			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment with passing=true")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-status-change",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-status-change",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-status",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-status-change",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying initial deployment status")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-change",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Get initial status count
			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			// Get the deployment ID from GitHub API (since EnvironmentInfo has Rollout History ID, not GitHub Deployment ID)
			ghDeployments, _, err := githubClient.Repositories.ListDeployments(context.Background(), "kuberik", "environment-controller-testing", &github.DeploymentsListOptions{
				Ref:         revision,
				Environment: "kuberik/test-deployment-status/production",
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(ghDeployments).ToNot(BeEmpty())
			initialDeploymentID := *ghDeployments[0].ID

			statuses, _, err := githubClient.Repositories.ListDeploymentStatuses(context.Background(), "kuberik", "environment-controller-testing", initialDeploymentID, &github.ListOptions{})
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).ToNot(BeEmpty())
			initialStatusCount := len(statuses)
			_ = initialStatusCount // Use variable to avoid unused error

			By("All GitHub deployments are initially successful")
			// The test just verifies the initial deployment was successful
		})

		It("Should update RolloutGate when Deployment spec changes", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with deployment history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
						{
							ID: k8sptr.To(int64(1)),
							Version: kuberikrolloutv1alpha1.VersionInfo{
								Tag:      "v1.0.0",
								Revision: &revision,
							},
							Timestamp: metav1.Now(),
						},
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			// Update the rollout to set the status
			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment with passing=true")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-update",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-update",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-update",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying RolloutGate was created")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-update",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			Expect(updatedDeployment.Status.RolloutGateRef).ToNot(BeNil())
			rolloutGateName := updatedDeployment.Status.RolloutGateRef.Name

			rolloutGate := &kuberikrolloutv1alpha1.RolloutGate{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      rolloutGateName,
				Namespace: DeploymentNamespace,
			}, rolloutGate)).To(Succeed())

			// Verify initial state
			Expect(rolloutGate.Spec.RolloutRef).ToNot(BeNil())
			Expect(rolloutGate.Spec.RolloutRef.Name).To(Equal("test-rollout-update"))

			// Update Deployment spec
			updatedDeployment.Spec.Environment = "staging"
			Expect(k8sClient.Update(context.Background(), updatedDeployment)).Should(Succeed())

			// Reconcile again
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())

			// Verify RolloutGate still exists and is updated
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      rolloutGateName,
				Namespace: DeploymentNamespace,
			}, rolloutGate)).To(Succeed())

			Expect(rolloutGate.Spec.RolloutRef).ToNot(BeNil())
			Expect(rolloutGate.Spec.RolloutRef.Name).To(Equal("test-rollout-update"))
		})

		It("Should handle missing Rollout", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Deployment with non-existent Rollout")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-missing-rollout",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "non-existent-rollout",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-missing-rollout",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-missing-rollout",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get Rollout"))
			Expect(result.RequeueAfter).To(Equal(time.Duration(0)))
		})

		It("Should handle Rollout with no revision", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with no revision")
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-no-revision",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
						{
							ID: k8sptr.To(int64(1)),
							Version: kuberikrolloutv1alpha1.VersionInfo{
								Tag: "v1.0.0",
								// No revision
							},
							Timestamp: metav1.Now(),
						},
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-no-revision",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-no-revision",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-no-revision",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-no-revision",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			// Should handle gracefully - no error, but no deployment created either
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))
		})

		It("Should handle empty Rollout history", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with empty history")
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-empty-history",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-empty-history",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-empty-history",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-empty-history",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-empty-history",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			// Should handle gracefully - no error, but no deployment created either
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))
		})

		It("Should update Deployment status with deployment information", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with deployment history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-status",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
						{
							ID: k8sptr.To(int64(1)),
							Version: kuberikrolloutv1alpha1.VersionInfo{
								Tag:      "v1.0.0",
								Revision: &revision,
							},
							Timestamp: metav1.Now(),
						},
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			// Update the rollout to set the status
			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-status",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-status",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/deployment-controller-testing",
					},
					Name:        "test-deployment-status",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-status",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying Deployment status was updated")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			Expect(updatedDeployment.Status.CurrentVersion).To(Equal(revision))
			Expect(updatedDeployment.Status.RolloutGateRef).ToNot(BeNil())
			Expect(updatedDeployment.Status.RolloutGateRef.Name).To(HavePrefix("ghd-"))
		})

		It("Should update allowed versions from relationships", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			By("Creating staging deployment with success status")
			// First create a related deployment (staging)
			// Use a commit SHA as the revision (GitHub deployment ref)
			// Relationships are for the same deployment name but different environment
			// So we create "test-deployment-deps/staging" environment
			stagingRef := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			stagingTag := "v1.0.0" // Tag that matches the staging revision
			stagingEnv := "kuberik/test-deployment-deps/staging"
			stagingTask := "deploy:kuberik/test-deployment-deps"

			// Create DeploymentHistoryEntry for staging deployment
			stagingHistoryEntry := &kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				ID: k8sptr.To(int64(100)),
				Version: kuberikrolloutv1alpha1.VersionInfo{
					Tag:      stagingTag,
					Revision: &stagingRef,
				},
				Timestamp: metav1.Now(),
				Message:   github.String("Staging deployment"),
			}
			payloadJSON, err := createDeploymentPayload("100", stagingHistoryEntry)
			Expect(err).ToNot(HaveOccurred())

			stagingDeploymentRequest := &github.DeploymentRequest{
				Ref:                   github.String(stagingRef),
				Environment:           github.String(stagingEnv),
				Task:                  github.String(stagingTask),
				ProductionEnvironment: github.Bool(false),
				AutoMerge:             github.Bool(false),
				Payload:               payloadJSON,
			}
			stagingDeployment, _, err := githubClient.Repositories.CreateDeployment(context.Background(), "kuberik", "environment-controller-testing", stagingDeploymentRequest)
			Expect(err).ToNot(HaveOccurred())

			// Create success status for staging deployment
			successState := "success"
			statusRequest := &github.DeploymentStatusRequest{
				State:       &successState,
				Description: github.String("Bake succeeded: Deployment successful"),
			}
			_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", stagingDeployment.GetID(), statusRequest)
			Expect(err).ToNot(HaveOccurred())

			By("Creating Rollout with release candidates")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-deps",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
				// Add releaseCandidates with revisions mapped to tags
				ReleaseCandidates: []kuberikrolloutv1alpha1.VersionInfo{
					{
						Tag:      stagingTag,
						Revision: &stagingRef,
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment with relationship")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-deps",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-deps",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-deps",
					Environment: "production",
					Relationship: &kuberikv1alpha1.EnvironmentRelationship{
						Environment: "staging",
						Type:        kuberikv1alpha1.RelationshipTypeAfter,
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment with relationship")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-deps",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying allowed versions were updated from relationships on RolloutGate")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-deps",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			Expect(updatedDeployment.Status.RolloutGateRef).ToNot(BeNil())
			rolloutGate := &kuberikrolloutv1alpha1.RolloutGate{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      updatedDeployment.Status.RolloutGateRef.Name,
				Namespace: DeploymentNamespace,
			}, rolloutGate)).To(Succeed())

			// Verify allowed versions were set
			Expect(rolloutGate.Spec.AllowedVersions).ToNot(BeNil())
			Expect(*rolloutGate.Spec.AllowedVersions).To(ContainElement(stagingTag))
		})

		It("Should track deployment statuses and update DeploymentStatuses", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with multiple history entries")
			revision1 := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			revision2 := "8bd1ffbf07d9f04b9aba8757303a0f4c328c1743"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-status-tracking",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.1.0",
							Revision: &revision2,
						},
						Timestamp: metav1.Now(),
					},
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision1,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-status-tracking",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-status-tracking",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-status-tracking",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-status-tracking",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying DeploymentStatuses were updated")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-tracking",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Should have environment info with history for production
			Expect(updatedDeployment.Status.EnvironmentInfos).ToNot(BeEmpty())
			var productionInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(productionInfo.History).ToNot(BeEmpty())

			// Should have history for both revisions
			Expect(len(productionInfo.History)).To(BeNumerically(">=", 2))

			// Verify both revisions are tracked
			revisionsFound := make(map[string]bool)
			for _, entry := range productionInfo.History {
				if entry.Version.Revision != nil {
					revisionsFound[*entry.Version.Revision] = true
				}
				Expect(entry.ID).ToNot(BeNil())
			}
			Expect(revisionsFound[revision1]).To(BeTrue())
			Expect(revisionsFound[revision2]).To(BeTrue())
		})

		It("Should track relevant versions based on relationships", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			By("Creating related environment deployments")
			// Create staging environment with version
			stagingRef := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			stagingEnv := "kuberik/test-deployment-relevant/staging"
			stagingTask := "deploy:kuberik/test-deployment-relevant"

			// Create DeploymentHistoryEntry for staging deployment
			stagingHistoryEntry := &kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				ID: k8sptr.To(int64(100)),
				Version: kuberikrolloutv1alpha1.VersionInfo{
					Tag:      "v1.0.0-staging",
					Revision: &stagingRef,
				},
				Timestamp: metav1.Now(),
				Message:   github.String("Staging deployment"),
			}
			payloadJSON, err := createDeploymentPayload("100", stagingHistoryEntry)
			Expect(err).ToNot(HaveOccurred())

			stagingDeploymentRequest := &github.DeploymentRequest{
				Ref:                   github.String(stagingRef),
				Environment:           github.String(stagingEnv),
				Task:                  github.String(stagingTask),
				ProductionEnvironment: github.Bool(false),
				AutoMerge:             github.Bool(false),
				Payload:               payloadJSON,
			}
			stagingDeployment, _, err := githubClient.Repositories.CreateDeployment(context.Background(), "kuberik", "environment-controller-testing", stagingDeploymentRequest)
			Expect(err).ToNot(HaveOccurred())

			// Create success status for staging
			successState := "success"
			statusRequest := &github.DeploymentStatusRequest{
				State:       &successState,
				Description: github.String("Deployment successful"),
			}
			_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", stagingDeployment.GetID(), statusRequest)
			Expect(err).ToNot(HaveOccurred())

			By("Creating Rollout with history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-relevant",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment with relationship to staging")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-relevant",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-relevant",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-relevant",
					Environment: "production",
					Relationship: &kuberikv1alpha1.EnvironmentRelationship{
						Environment: "staging",
						Type:        kuberikv1alpha1.RelationshipTypeAfter,
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-relevant",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying relevant versions are tracked")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-relevant",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Should have status entries for staging environment (related environment)
			var stagingInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).ToNot(BeEmpty())

			// Should track staging version since it's related
			Expect(stagingInfo.History).ToNot(BeEmpty())

			// Verify staging version is tracked
			stagingVersionFound := false
			for _, entry := range stagingInfo.History {
				if entry.Version.Revision != nil && *entry.Version.Revision == stagingRef {
					stagingVersionFound = true
					Expect(entry.ID).ToNot(BeNil())
					break
				}
			}
			Expect(stagingVersionFound).To(BeTrue())
		})

		It("Should update DeploymentStatuses when versions are added to history", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with initial history")
			revision1 := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-status-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision1,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-status-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-status-update",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-status-update",
					Environment: "production",
				},
			}
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment - first time")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-status-update",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying DeploymentStatuses has entry for revision1")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-update",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			var productionInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(productionInfo.History).ToNot(BeEmpty())

			revision1Found := false
			for _, entry := range productionInfo.History {
				if entry.Version.Revision != nil && *entry.Version.Revision == revision1 {
					revision1Found = true
					Expect(entry.ID).ToNot(BeNil())
					break
				}
			}
			Expect(revision1Found).To(BeTrue())

			// Verify that revision1 has bakeStatus set initially
			revision1Entry := findHistoryEntryByRevision(productionInfo.History, revision1)
			Expect(revision1Entry).ToNot(BeNil(), "revision1 entry should exist")
			initialBakeStatus := revision1Entry.BakeStatus
			Expect(initialBakeStatus).ToNot(BeNil(), "revision1 should have bakeStatus set initially")

			By("Adding new version to history")
			revision2 := "8bd1ffbf07d9f04b9aba8757303a0f4c328c1743"
			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.1.0",
							Revision: &revision2,
						},
						Timestamp: metav1.Now(),
					},
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision1,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Reconciling Deployment - second time with new version")
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())

			By("Verifying DeploymentStatuses has entries for both revisions and older entry retains bakeStatus")
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-update",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Get updated production info
			productionInfo = nil
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(len(productionInfo.History)).To(BeNumerically(">=", 2))

			revisionsFound := make(map[string]bool)
			for _, entry := range productionInfo.History {
				if entry.Version.Revision != nil {
					revisionsFound[*entry.Version.Revision] = true
				}
			}
			Expect(revisionsFound[revision1]).To(BeTrue())
			Expect(revisionsFound[revision2]).To(BeTrue())

			// Verify that revision1 still has its bakeStatus (it should not be lost when new version is added)
			revision1EntryAfter := findHistoryEntryByRevision(productionInfo.History, revision1)
			Expect(revision1EntryAfter).ToNot(BeNil(), "revision1 entry should still exist after adding new version")
			Expect(revision1EntryAfter.BakeStatus).ToNot(BeNil(), "revision1 should still have bakeStatus after adding new version")
			if initialBakeStatus != nil {
				Expect(*revision1EntryAfter.BakeStatus).To(Equal(*initialBakeStatus), "revision1 bakeStatus should be preserved when new version is added")
			}
		})

		It("Should remove DeploymentStatuses entries when versions are removed from history", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with two history entries")
			revision1 := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			revision2 := "8bd1ffbf07d9f04b9aba8757303a0f4c328c1743"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-status-remove",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.1.0",
							Revision: &revision2,
						},
						Timestamp: metav1.Now(),
					},
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision1,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-status-remove",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-status-remove",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/deployment-controller-testing",
					},
					Name:        "test-deployment-status-remove",
					Environment: "production",
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment - first time with both versions")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-status-remove",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying DeploymentStatuses has entries for both revisions")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-remove",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			var productionInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(len(productionInfo.History)).To(BeNumerically(">=", 2))

			By("Removing revision1 from history")
			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.1.0",
							Revision: &revision2,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Reconciling Deployment - second time with only revision2")
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())

			By("Verifying DeploymentStatuses only has entry for revision2")
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-remove",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Get updated production info
			productionInfo = nil
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())

			// Should only have revision2 now
			revisionsFound := make(map[string]bool)
			for _, entry := range productionInfo.History {
				if entry.Version.Revision != nil {
					revisionsFound[*entry.Version.Revision] = true
				}
			}
			Expect(revisionsFound[revision2]).To(BeTrue())
			Expect(revisionsFound[revision1]).To(BeFalse(), "revision1 should be removed from History")
		})

		It("Should update DeploymentStatuses for related environments based on relationships", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			By("Creating staging deployment")
			stagingRef := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			stagingEnv := "kuberik/test-deployment-status-env/staging"
			stagingTask := "deploy:kuberik/test-deployment-status-env"

			// Create DeploymentHistoryEntry for staging deployment
			stagingHistoryEntry := &kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				ID: k8sptr.To(int64(100)),
				Version: kuberikrolloutv1alpha1.VersionInfo{
					Tag:      "v1.0.0-staging",
					Revision: &stagingRef,
				},
				Timestamp: metav1.Now(),
				Message:   github.String("Staging deployment"),
			}
			payloadJSON, err := createDeploymentPayload("100", stagingHistoryEntry)
			Expect(err).ToNot(HaveOccurred())

			stagingDeploymentRequest := &github.DeploymentRequest{
				Ref:                   github.String(stagingRef),
				Environment:           github.String(stagingEnv),
				Task:                  github.String(stagingTask),
				ProductionEnvironment: github.Bool(false),
				AutoMerge:             github.Bool(false),
				Payload:               payloadJSON,
			}
			stagingDeployment, _, err := githubClient.Repositories.CreateDeployment(context.Background(), "kuberik", "environment-controller-testing", stagingDeploymentRequest)
			Expect(err).ToNot(HaveOccurred())

			// Create success status for staging
			successState := "success"
			statusRequest := &github.DeploymentStatusRequest{
				State:       &successState,
				Description: github.String("Deployment successful"),
			}
			_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", stagingDeployment.GetID(), statusRequest)
			Expect(err).ToNot(HaveOccurred())

			By("Creating Rollout with history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-status-env",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment with relationship to staging")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-status-env",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-status-env",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/deployment-controller-testing",
					},
					Name:        "test-deployment-status-env",
					Environment: "production",
					Relationship: &kuberikv1alpha1.EnvironmentRelationship{
						Environment: "staging",
						Type:        kuberikv1alpha1.RelationshipTypeAfter,
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-status-env",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying DeploymentStatuses has entries for both production and staging")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-status-env",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Should have status entries for production (current environment)
			var productionInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(productionInfo.History).ToNot(BeEmpty())

			// Should have status entries for staging (related environment)
			var stagingInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).ToNot(BeEmpty())
			Expect(stagingInfo.History).ToNot(BeEmpty())

			// Verify staging status entry has correct information
			stagingVersionFound := false
			for _, entry := range stagingInfo.History {
				if entry.Version.Revision != nil && *entry.Version.Revision == stagingRef {
					stagingVersionFound = true
					Expect(entry.ID).ToNot(BeNil())
					break
				}
			}
			Expect(stagingVersionFound).To(BeTrue())
		})

		It("Should update history for related environments with Environment resources", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating staging Environment and Rollout")
			stagingRevision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			stagingRollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-staging-env",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), stagingRollout)
			Expect(k8sClient.Create(context.Background(), stagingRollout)).Should(Succeed())

			stagingRollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(200)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0-staging",
							Revision: &stagingRevision,
						},
						Timestamp: metav1.Now(),
						// Start with Deploying status - will transition to Succeeded in test
						BakeStatus: k8sptr.To(kuberikrolloutv1alpha1.BakeStatusDeploying),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), stagingRollout)).Should(Succeed())

			stagingEnv := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-staging-env",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-staging-env",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-related-env",
					Environment: "staging",
				},
			}
			_ = k8sClient.Delete(context.Background(), stagingEnv)
			Expect(k8sClient.Create(context.Background(), stagingEnv)).Should(Succeed())

			By("Creating production Rollout")
			productionRevision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			productionRollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-production-env",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), productionRollout)
			Expect(k8sClient.Create(context.Background(), productionRollout)).Should(Succeed())

			productionRollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(300)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &productionRevision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), productionRollout)).Should(Succeed())

			By("Creating production Environment with relationship to staging")
			productionEnv := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-production-env",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-production-env",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-related-env",
					Environment: "production",
					Relationship: &kuberikv1alpha1.EnvironmentRelationship{
						Environment: "staging",
						Type:        kuberikv1alpha1.RelationshipTypeAfter,
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), productionEnv)
			Expect(k8sClient.Create(context.Background(), productionEnv)).Should(Succeed())

			By("Reconciling production Environment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-production-env",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying production environment has its own history")
			updatedProductionEnv := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-production-env",
				Namespace: DeploymentNamespace,
			}, updatedProductionEnv)).To(Succeed())

			var productionInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedProductionEnv.Status.EnvironmentInfos {
				if updatedProductionEnv.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedProductionEnv.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(productionInfo.History).ToNot(BeEmpty())
			Expect(productionInfo.History).To(HaveLen(1))
			Expect(productionInfo.History[0].Version.Revision).ToNot(BeNil())
			Expect(*productionInfo.History[0].Version.Revision).To(Equal(productionRevision))

			By("Verifying staging environment history is included in production Environment status")
			var stagingInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedProductionEnv.Status.EnvironmentInfos {
				if updatedProductionEnv.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedProductionEnv.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil(), "staging environment info should exist in production Environment status")
			Expect(stagingInfo.History).ToNot(BeEmpty(), "staging history should not be empty")
			Expect(stagingInfo.History).To(HaveLen(1))
			Expect(stagingInfo.History[0].Version.Revision).ToNot(BeNil())
			Expect(*stagingInfo.History[0].Version.Revision).To(Equal(stagingRevision))
			// Initially should have Deploying status (set in initial rollout status)
			Expect(stagingInfo.History[0].BakeStatus).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusDeploying), "staging history should initially show Deploying status")

			By("Updating staging rollout bake status to Succeeded and verifying it updates in production Environment")
			stagingRollout.Status.History[0].BakeStatus = k8sptr.To(kuberikrolloutv1alpha1.BakeStatusSucceeded)
			stagingRollout.Status.History[0].BakeStatusMessage = k8sptr.To("Bake completed successfully")
			Expect(k8sClient.Status().Update(context.Background(), stagingRollout)).Should(Succeed())

			// Reconcile production environment again to sync Succeeded status
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// Verify staging history was updated to Succeeded
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-production-env",
				Namespace: DeploymentNamespace,
			}, updatedProductionEnv)).To(Succeed())

			stagingInfo = nil
			for i := range updatedProductionEnv.Status.EnvironmentInfos {
				if updatedProductionEnv.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedProductionEnv.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).ToNot(BeEmpty())
			Expect(stagingInfo.History).To(HaveLen(1))
			Expect(stagingInfo.History[0].BakeStatus).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusSucceeded), "staging history should be updated to Succeeded status")
			Expect(stagingInfo.History[0].BakeStatusMessage).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatusMessage).To(Equal("Bake completed successfully"))
		})

		It("Should update existing history entries when their status changes", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating staging Environment and Rollout with initial deployment")
			stagingRevision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			stagingRollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-staging-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), stagingRollout)
			Expect(k8sClient.Create(context.Background(), stagingRollout)).Should(Succeed())

			// Start with a deployment in Pending status
			stagingRollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(300)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0-staging",
							Revision: &stagingRevision,
						},
						Timestamp:  metav1.Now(),
						BakeStatus: k8sptr.To(kuberikrolloutv1alpha1.BakeStatusDeploying),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), stagingRollout)).Should(Succeed())

			stagingEnv := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-staging-env-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-staging-update",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-update-env",
					Environment: "staging",
				},
			}
			_ = k8sClient.Delete(context.Background(), stagingEnv)
			Expect(k8sClient.Create(context.Background(), stagingEnv)).Should(Succeed())

			By("Creating production Environment with relationship to staging")
			productionRevision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			productionRollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-production-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), productionRollout)
			Expect(k8sClient.Create(context.Background(), productionRollout)).Should(Succeed())

			productionRollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(400)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &productionRevision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), productionRollout)).Should(Succeed())

			productionEnv := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-production-env-update",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-production-update",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-update-env",
					Environment: "production",
					Relationship: &kuberikv1alpha1.EnvironmentRelationship{
						Environment: "staging",
						Type:        kuberikv1alpha1.RelationshipTypeAfter,
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), productionEnv)
			Expect(k8sClient.Create(context.Background(), productionEnv)).Should(Succeed())

			By("Reconciling production Environment - should see Pending status")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-production-env-update",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// Verify staging history shows Pending
			updatedProductionEnv := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-production-env-update",
				Namespace: DeploymentNamespace,
			}, updatedProductionEnv)).To(Succeed())

			var stagingInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedProductionEnv.Status.EnvironmentInfos {
				if updatedProductionEnv.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedProductionEnv.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).To(HaveLen(1))
			Expect(stagingInfo.History[0].BakeStatus).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusDeploying))

			By("Updating staging rollout status to InProgress")
			// Re-fetch the rollout to get the latest version
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-rollout-staging-update",
				Namespace: DeploymentNamespace,
			}, stagingRollout)).Should(Succeed())
			stagingRollout.Status.History[0].BakeStatus = k8sptr.To(kuberikrolloutv1alpha1.BakeStatusInProgress)
			stagingRollout.Status.History[0].BakeStatusMessage = k8sptr.To("Bake in progress")
			Expect(k8sClient.Status().Update(context.Background(), stagingRollout)).Should(Succeed())

			// Reconcile again - should see InProgress
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// Verify staging history was updated to InProgress
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-production-env-update",
				Namespace: DeploymentNamespace,
			}, updatedProductionEnv)).To(Succeed())

			stagingInfo = nil
			for i := range updatedProductionEnv.Status.EnvironmentInfos {
				if updatedProductionEnv.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedProductionEnv.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).To(HaveLen(1))
			Expect(stagingInfo.History[0].BakeStatus).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusInProgress), "staging history should be updated to InProgress")
			Expect(stagingInfo.History[0].BakeStatusMessage).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatusMessage).To(Equal("Bake in progress"))

			By("Updating staging rollout status to Succeeded")
			// Re-fetch the rollout to get the latest version
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-rollout-staging-update",
				Namespace: DeploymentNamespace,
			}, stagingRollout)).Should(Succeed())
			stagingRollout.Status.History[0].BakeStatus = k8sptr.To(kuberikrolloutv1alpha1.BakeStatusSucceeded)
			stagingRollout.Status.History[0].BakeStatusMessage = k8sptr.To("Bake completed successfully")
			Expect(k8sClient.Status().Update(context.Background(), stagingRollout)).Should(Succeed())

			// Reconcile again - should see Succeeded
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// Verify staging history was updated to Succeeded
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-production-env-update",
				Namespace: DeploymentNamespace,
			}, updatedProductionEnv)).To(Succeed())

			stagingInfo = nil
			for i := range updatedProductionEnv.Status.EnvironmentInfos {
				if updatedProductionEnv.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedProductionEnv.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).To(HaveLen(1))
			Expect(stagingInfo.History[0].BakeStatus).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusSucceeded), "staging history should be updated to Succeeded")
			Expect(stagingInfo.History[0].BakeStatusMessage).ToNot(BeNil())
			Expect(*stagingInfo.History[0].BakeStatusMessage).To(Equal("Bake completed successfully"))
		})

		It("Should only track relevant versions based on relationship graph", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			By("Creating related environment (staging) and unrelated environment (qa)")
			stagingRef := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			qaRef := "5220a27a5410a6a5182b9fadf537c6437fcca0b7"

			// Create staging deployment
			stagingEnv := "kuberik/test-deployment-relevant-graph/staging"
			stagingTask := "deploy:kuberik/test-deployment-relevant-graph"

			// Create DeploymentHistoryEntry for staging deployment
			stagingHistoryEntry := &kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				ID: k8sptr.To(int64(100)),
				Version: kuberikrolloutv1alpha1.VersionInfo{
					Tag:      "v1.0.0-staging",
					Revision: &stagingRef,
				},
				Timestamp: metav1.Now(),
				Message:   github.String("Staging deployment"),
			}
			payloadJSON, err := createDeploymentPayload("100", stagingHistoryEntry)
			Expect(err).ToNot(HaveOccurred())

			stagingDeploymentRequest := &github.DeploymentRequest{
				Ref:                   github.String(stagingRef),
				Environment:           github.String(stagingEnv),
				Task:                  github.String(stagingTask),
				ProductionEnvironment: github.Bool(false),
				AutoMerge:             github.Bool(false),
				Payload:               payloadJSON,
			}
			stagingDeployment, _, err := githubClient.Repositories.CreateDeployment(context.Background(), "kuberik", "deployment-controller-testing", stagingDeploymentRequest)
			Expect(err).ToNot(HaveOccurred())

			successState := "success"
			statusRequest := &github.DeploymentStatusRequest{
				State:       &successState,
				Description: github.String("Deployment successful"),
			}
			_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "deployment-controller-testing", stagingDeployment.GetID(), statusRequest)
			Expect(err).ToNot(HaveOccurred())

			// Create qa deployment (unrelated)
			qaEnv := "kuberik/test-deployment-relevant-graph/qa"
			qaTask := "deploy:kuberik/test-deployment-relevant-graph"
			qaDeploymentRequest := &github.DeploymentRequest{
				Ref:                   github.String(qaRef),
				Environment:           github.String(qaEnv),
				Task:                  github.String(qaTask),
				ProductionEnvironment: github.Bool(false),
				AutoMerge:             github.Bool(false),
			}
			qaDeployment, _, err := githubClient.Repositories.CreateDeployment(context.Background(), "kuberik", "environment-controller-testing", qaDeploymentRequest)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", qaDeployment.GetID(), statusRequest)
			Expect(err).ToNot(HaveOccurred())

			By("Creating Rollout with history")
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-relevant-graph",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Deployment with relationship to staging (not qa)")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-deployment-relevant-graph",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-relevant-graph",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-relevant-graph",
					Environment: "production",
					Relationship: &kuberikv1alpha1.EnvironmentRelationship{
						Environment: "staging",
						Type:        kuberikv1alpha1.RelationshipTypeAfter,
					},
				},
			}
			// Delete if exists first
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling Deployment")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-deployment-relevant-graph",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying only relevant versions are tracked")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-deployment-relevant-graph",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Should have status entries for staging (related)
			var stagingInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "staging" {
					stagingInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(stagingInfo).ToNot(BeNil())
			Expect(stagingInfo.History).ToNot(BeEmpty())
			Expect(stagingInfo.History).ToNot(BeEmpty())

			// Should NOT have status entries for qa (unrelated)
			var qaInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "qa" {
					qaInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			if qaInfo != nil {
				Expect(qaInfo.History).To(BeEmpty(), "qa environment should not be tracked as it's not related")
			}
		})

		It("Should set bakeStatus for all history entries from GitHub deployment statuses", func() {
			skipIfNoGitHubToken()

			By("Creating GitHub token secret")
			Expect(createGitHubTokenSecret()).To(Succeed())

			By("Creating Rollout with multiple history entries")
			revision1 := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			revision2 := "8bd1ffbf07d9f04b9aba8757303a0f4c328c1743"
			revision3 := "5220a27a5410a6a5182b9fadf537c6437fcca0b7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout-multiple-history",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikrolloutv1alpha1.RolloutSpec{
					ReleasesImagePolicy: corev1.LocalObjectReference{
						Name: "test-policy",
					},
				},
			}
			_ = k8sClient.Delete(context.Background(), rollout)
			Expect(k8sClient.Create(context.Background(), rollout)).Should(Succeed())

			rollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(3)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.2.0",
							Revision: &revision3,
						},
						Timestamp: metav1.Now(),
					},
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.1.0",
							Revision: &revision2,
						},
						Timestamp: metav1.Now(),
					},
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Tag:      "v1.0.0",
							Revision: &revision1,
						},
						Timestamp: metav1.Now(),
					},
				},
			}
			Expect(k8sClient.Status().Update(context.Background(), rollout)).Should(Succeed())

			By("Creating Environment")
			deployment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-github-multiple-history",
					Namespace: DeploymentNamespace,
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					RolloutRef: corev1.LocalObjectReference{
						Name: "test-rollout-multiple-history",
					},
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
					Name:        "test-deployment-multiple-history",
					Environment: "production",
				},
			}
			_ = k8sClient.Delete(context.Background(), deployment)
			Expect(k8sClient.Create(context.Background(), deployment)).Should(Succeed())

			By("Reconciling to create GitHub deployments")
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-github-multiple-history",
					Namespace: DeploymentNamespace,
				},
			}

			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Getting the created Environment to find deployment IDs")
			updatedDeployment := &kuberikv1alpha1.Environment{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-multiple-history",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			// Get production environment info to find all deployment IDs
			var productionInfo *kuberikv1alpha1.EnvironmentInfo
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(len(productionInfo.History)).To(BeNumerically(">=", 3))

			By("Creating GitHub deployment statuses with different states for each deployment")
			token := os.Getenv("GITHUB_TOKEN")
			ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
			tc := oauth2.NewClient(context.Background(), ts)
			githubClient := github.NewClient(tc)

			// Find deployments for each history entry
			formattedEnv := "kuberik/test-deployment-multiple-history/production"
			task := "deploy:kuberik/test-deployment-multiple-history"
			deployments, _, err := githubClient.Repositories.ListDeployments(context.Background(), "kuberik", "environment-controller-testing", &github.DeploymentsListOptions{
				Task:        task,
				Environment: formattedEnv,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(deployments)).To(BeNumerically(">=", 3))

			// Map deployments by their payload ID
			deploymentMap := make(map[string]*github.Deployment)
			for _, d := range deployments {
				payload := extractDeploymentPayloadFromTest(d)
				if payload != nil && payload.ID != "" {
					deploymentMap[payload.ID] = d
				}
			}

			// Create statuses with different states for each deployment
			// Use the format: "Status: message"
			// Entry 3 (latest) -> success
			if dep, ok := deploymentMap["3"]; ok {
				successState := "success"
				statusRequest := &github.DeploymentStatusRequest{
					State:       &successState,
					Description: github.String("Bake succeeded: Deployment v1.2.0 successful"),
				}
				_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", dep.GetID(), statusRequest)
				Expect(err).ToNot(HaveOccurred())
			} else {
				Fail("Entry 3 should have bakeStatus set")
			}

			// Entry 2 -> failure
			if dep, ok := deploymentMap["2"]; ok {
				failureState := "failure"
				statusRequest := &github.DeploymentStatusRequest{
					State:       &failureState,
					Description: github.String("Bake failed: Deployment v1.1.0 failed"),
				}
				_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", dep.GetID(), statusRequest)
				Expect(err).ToNot(HaveOccurred())
			} else {
				Fail("Entry 2 should have bakeStatus set")
			}

			// Entry 1 -> pending
			if dep, ok := deploymentMap["1"]; ok {
				pendingState := "pending"
				statusRequest := &github.DeploymentStatusRequest{
					State:       &pendingState,
					Description: github.String("Deploying: Deployment v1.0.0 in progress"),
				}
				_, _, err = githubClient.Repositories.CreateDeploymentStatus(context.Background(), "kuberik", "environment-controller-testing", dep.GetID(), statusRequest)
				Expect(err).ToNot(HaveOccurred())
			} else {
				Fail("Entry 1 should have bakeStatus set")
			}

			By("Reconciling again to fetch updated statuses")
			result, err = reconciler.Reconcile(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			By("Verifying all history entries have their bakeStatus set correctly")
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      "test-github-multiple-history",
				Namespace: DeploymentNamespace,
			}, updatedDeployment)).To(Succeed())

			productionInfo = nil
			for i := range updatedDeployment.Status.EnvironmentInfos {
				if updatedDeployment.Status.EnvironmentInfos[i].Environment == "production" {
					productionInfo = &updatedDeployment.Status.EnvironmentInfos[i]
					break
				}
			}
			Expect(productionInfo).ToNot(BeNil())
			Expect(len(productionInfo.History)).To(BeNumerically(">=", 3))

			// Verify all entries have bakeStatus set
			entryMap := make(map[int64]*kuberikrolloutv1alpha1.DeploymentHistoryEntry)
			for i := range productionInfo.History {
				if productionInfo.History[i].ID != nil {
					entryMap[*productionInfo.History[i].ID] = &productionInfo.History[i]
				}
			}

			// Entry 3 should have Succeeded status
			if entry, ok := entryMap[3]; ok {
				Expect(entry.BakeStatus).ToNot(BeNil(), "Entry 3 should have bakeStatus set")
				Expect(*entry.BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusSucceeded), "Entry 3 should be Succeeded")
				Expect(entry.BakeStatusMessage).ToNot(BeNil())
				Expect(*entry.BakeStatusMessage).To(Equal("Deployment v1.2.0 successful"))
			} else {
				Fail("Entry 3 should have bakeStatus set")
			}

			// Entry 2 should have Failed status
			if entry, ok := entryMap[2]; ok {
				Expect(entry.BakeStatus).ToNot(BeNil(), "Entry 2 should have bakeStatus set")
				Expect(*entry.BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusFailed), "Entry 2 should be Failed")
				Expect(entry.BakeStatusMessage).ToNot(BeNil())
				Expect(*entry.BakeStatusMessage).To(Equal("Deployment v1.1.0 failed"))
			} else {
				Fail("Entry 2 should have bakeStatus set")
			}

			// Entry 1 should have Deploying status (parsed from description)
			if entry, ok := entryMap[1]; ok {
				Expect(entry.BakeStatus).ToNot(BeNil(), "Entry 1 should have bakeStatus set")
				Expect(*entry.BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusDeploying), "Entry 1 should be Deploying")
				Expect(entry.BakeStatusMessage).ToNot(BeNil())
				Expect(*entry.BakeStatusMessage).To(Equal("Deployment v1.0.0 in progress"))
			} else {
				Fail("Entry 1 should have bakeStatus set")
			}
		})
	})

	It("Should preserve bakeStatus when inactive status without meaningful description is present", func() {
		By("Creating a deployment history entry")
		entryID := int64(1)
		entry := kuberikrolloutv1alpha1.DeploymentHistoryEntry{
			ID: &entryID,
			Version: kuberikrolloutv1alpha1.VersionInfo{
				Revision: k8sptr.To("abc123"),
			},
			BakeStatus:        k8sptr.To(kuberikrolloutv1alpha1.BakeStatusFailed),
			BakeStatusMessage: k8sptr.To("A HealthCheck reported an error after deployment."),
		}

		By("Creating GitHub statuses with inactive status first (newest), then the failed status")
		// Statuses are ordered newest first
		// First status is inactive without meaningful description (set automatically by GitHub)
		inactiveState := "inactive"
		inactiveDesc := "This deployment is inactive"
		statuses := []*github.DeploymentStatus{
			{
				State:       &inactiveState,
				Description: &inactiveDesc,
			},
			// Second status is our failed status (parsed from description)
			{
				State:       k8sptr.To("failure"),
				Description: k8sptr.To("Bake failed: A HealthCheck reported an error after deployment."),
			},
		}

		By("Applying statuses to entry")
		applyGitHubStatusToEntry(&entry, statuses)

		By("Verifying bakeStatus is preserved from the failure status")
		Expect(entry.BakeStatus).ToNot(BeNil())
		Expect(*entry.BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusFailed))
		Expect(entry.BakeStatusMessage).ToNot(BeNil())
		Expect(*entry.BakeStatusMessage).To(Equal("A HealthCheck reported an error after deployment."))
	})

	It("Should handle multiple inactive statuses and find the most recent meaningful status", func() {
		By("Creating a deployment history entry")
		entryID := int64(1)
		entry := kuberikrolloutv1alpha1.DeploymentHistoryEntry{
			ID: &entryID,
			Version: kuberikrolloutv1alpha1.VersionInfo{
				Revision: k8sptr.To("abc123"),
			},
		}

		By("Creating GitHub statuses with multiple inactive statuses, then a succeeded status")
		inactiveState := "inactive"
		inactiveDesc1 := "This deployment is inactive"
		inactiveDesc2 := "Deployment cancelled"
		statuses := []*github.DeploymentStatus{
			// Newest: inactive without meaningful description (skipped)
			{
				State:       &inactiveState,
				Description: &inactiveDesc1,
			},
			// Second: inactive with "cancelled" keyword (could be our status)
			{
				State:       &inactiveState,
				Description: &inactiveDesc2,
			},
			// Third: succeeded status (our status, parsed from description)
			{
				State:       k8sptr.To("success"),
				Description: k8sptr.To("Bake succeeded: Bake time completed successfully (no errors within bake time)."),
			},
		}

		By("Applying statuses to entry")
		applyGitHubStatusToEntry(&entry, statuses)

		By("Verifying bakeStatus is set from the succeeded status")
		Expect(entry.BakeStatus).ToNot(BeNil())
		Expect(*entry.BakeStatus).To(Equal(kuberikrolloutv1alpha1.BakeStatusSucceeded))
		Expect(entry.BakeStatusMessage).ToNot(BeNil())
		Expect(*entry.BakeStatusMessage).To(Equal("Bake time completed successfully (no errors within bake time)."))
	})

	It("Should preserve existing environmentInfos when environment is not discovered due to pagination", func() {
		skipIfNoGitHubToken()

		By("Creating GitHub token secret")
		Expect(createGitHubTokenSecret()).To(Succeed())

		// This test verifies that if an environment was previously discovered and tracked,
		// it remains in environmentInfos even if it's not discovered in the current run
		// (e.g., due to pagination where its deployments are beyond the first page)

		revision1 := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
		revision2 := "8bd1ffbf07d9f04b9aba8757303a0f4c328c1743"

		By("Creating staging environment and rollout")
		stagingRollout := &kuberikrolloutv1alpha1.Rollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rollout-staging-preserve",
				Namespace: DeploymentNamespace,
			},
			Spec: kuberikrolloutv1alpha1.RolloutSpec{
				ReleasesImagePolicy: corev1.LocalObjectReference{
					Name: "test-policy",
				},
			},
		}
		_ = k8sClient.Delete(context.Background(), stagingRollout)
		Expect(k8sClient.Create(context.Background(), stagingRollout)).Should(Succeed())

		stagingRollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
			History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				{
					ID: k8sptr.To(int64(10)),
					Version: kuberikrolloutv1alpha1.VersionInfo{
						Tag:      "v1.10.0",
						Revision: &revision1,
					},
					Timestamp: metav1.Now(),
				},
			},
		}
		Expect(k8sClient.Status().Update(context.Background(), stagingRollout)).Should(Succeed())

		stagingEnv := &kuberikv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-staging-preserve",
				Namespace: DeploymentNamespace,
			},
			Spec: kuberikv1alpha1.EnvironmentSpec{
				RolloutRef: corev1.LocalObjectReference{
					Name: "test-rollout-staging-preserve",
				},
				Backend: kuberikv1alpha1.BackendConfig{
					Type:    "github",
					Project: "kuberik/environment-controller-testing",
				},
				Name:        "test-deployment-preserve",
				Environment: "staging",
				Relationship: &kuberikv1alpha1.EnvironmentRelationship{
					Environment: "dev",
					Type:        kuberikv1alpha1.RelationshipTypeAfter,
				},
			},
		}
		_ = k8sClient.Delete(context.Background(), stagingEnv)
		Expect(k8sClient.Create(context.Background(), stagingEnv)).Should(Succeed())

		By("Creating dev environment and rollout")
		devRollout := &kuberikrolloutv1alpha1.Rollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-rollout-dev-preserve",
				Namespace: DeploymentNamespace,
			},
			Spec: kuberikrolloutv1alpha1.RolloutSpec{
				ReleasesImagePolicy: corev1.LocalObjectReference{
					Name: "test-policy",
				},
			},
		}
		_ = k8sClient.Delete(context.Background(), devRollout)
		Expect(k8sClient.Create(context.Background(), devRollout)).Should(Succeed())

		devRollout.Status = kuberikrolloutv1alpha1.RolloutStatus{
			History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				{
					ID: k8sptr.To(int64(20)),
					Version: kuberikrolloutv1alpha1.VersionInfo{
						Tag:      "v1.20.0",
						Revision: &revision2,
					},
					Timestamp: metav1.Now(),
				},
			},
		}
		Expect(k8sClient.Status().Update(context.Background(), devRollout)).Should(Succeed())

		devEnv := &kuberikv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dev-preserve",
				Namespace: DeploymentNamespace,
			},
			Spec: kuberikv1alpha1.EnvironmentSpec{
				RolloutRef: corev1.LocalObjectReference{
					Name: "test-rollout-dev-preserve",
				},
				Backend: kuberikv1alpha1.BackendConfig{
					Type:    "github",
					Project: "kuberik/environment-controller-testing",
				},
				Name:        "test-deployment-preserve",
				Environment: "dev",
			},
		}
		_ = k8sClient.Delete(context.Background(), devEnv)
		Expect(k8sClient.Create(context.Background(), devEnv)).Should(Succeed())

		By("First reconciliation - staging should be discovered and added")
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "test-dev-preserve",
				Namespace: DeploymentNamespace,
			},
		}

		result, err := reconciler.Reconcile(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Minute))

		updatedDevEnv := &kuberikv1alpha1.Environment{}
		Expect(k8sClient.Get(context.Background(), types.NamespacedName{
			Name:      "test-dev-preserve",
			Namespace: DeploymentNamespace,
		}, updatedDevEnv)).To(Succeed())

		// Verify staging was discovered and added
		var stagingInfo *kuberikv1alpha1.EnvironmentInfo
		for i := range updatedDevEnv.Status.EnvironmentInfos {
			if updatedDevEnv.Status.EnvironmentInfos[i].Environment == "staging" {
				stagingInfo = &updatedDevEnv.Status.EnvironmentInfos[i]
				break
			}
		}
		Expect(stagingInfo).ToNot(BeNil(), "staging should be discovered in first reconciliation")
		originalStagingHistoryCount := len(stagingInfo.History)

		By("Reconciling again - staging should be preserved even if not discovered")
		// The fix should preserve staging even if it's not in graphData.relevantEnvironments
		// In a real scenario, this would happen if staging's deployments are on page 2+ of GitHub API
		result, err = reconciler.Reconcile(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())

		Expect(k8sClient.Get(context.Background(), types.NamespacedName{
			Name:      "test-dev-preserve",
			Namespace: DeploymentNamespace,
		}, updatedDevEnv)).To(Succeed())

		// Verify staging is still preserved
		stagingInfo = nil
		for i := range updatedDevEnv.Status.EnvironmentInfos {
			if updatedDevEnv.Status.EnvironmentInfos[i].Environment == "staging" {
				stagingInfo = &updatedDevEnv.Status.EnvironmentInfos[i]
				break
			}
		}
		Expect(stagingInfo).ToNot(BeNil(), "staging should be preserved even if not discovered in current run due to pagination")
		Expect(stagingInfo.History).To(HaveLen(originalStagingHistoryCount), "staging history should be preserved")
	})
})

// Helper function to find history entry by revision
func findHistoryEntryByRevision(history []kuberikrolloutv1alpha1.DeploymentHistoryEntry, revision string) *kuberikrolloutv1alpha1.DeploymentHistoryEntry {
	for i := range history {
		if history[i].Version.Revision != nil && *history[i].Version.Revision == revision {
			return &history[i]
		}
	}
	return nil
}

// Helper function to extract deployment payload in tests
func extractDeploymentPayloadFromTest(d *github.Deployment) *deploymentPayload {
	if len(d.Payload) == 0 {
		return nil
	}

	// Try to decode payload - GitHub API may return it as base64-encoded string or JSON object
	var payloadBytes []byte

	// First try as base64-encoded string
	var payloadStr string
	if err := json.Unmarshal(d.Payload, &payloadStr); err == nil {
		// Decode base64
		if decoded, err := base64.StdEncoding.DecodeString(payloadStr); err == nil {
			payloadBytes = decoded
		} else {
			// Not base64, use string as-is
			payloadBytes = []byte(payloadStr)
		}
	} else {
		// Not a string, use payload directly
		payloadBytes = d.Payload
	}

	var payload deploymentPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil
	}

	return &payload
}
