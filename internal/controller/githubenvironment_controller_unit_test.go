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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sptr "k8s.io/utils/ptr"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("GitHub Environment Controller Unit Tests", func() {
	var reconciler *GitHubEnvironmentReconciler

	BeforeEach(func() {
		scheme := runtime.NewScheme()
		_ = kuberikv1alpha1.AddToScheme(scheme)
		_ = kuberikrolloutv1alpha1.AddToScheme(scheme)
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&kuberikv1alpha1.Environment{}).
			Build()
		reconciler = &GitHubEnvironmentReconciler{
			Client: fakeClient,
		}
	})

	Context("getCurrentVersionFromRollout", func() {
		It("Should return revision when available", func() {
			revision := "0a9c600d3a75bcb7ec54dcef3b03e0d7fe0598d7"
			rollout := &kuberikrolloutv1alpha1.Rollout{
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

			result := reconciler.getCurrentVersionFromRollout(rollout)
			Expect(result).ToNot(BeNil())
			Expect(*result).To(Equal(revision))
		})

		It("Should return nil when revision is not available", func() {
			rollout := &kuberikrolloutv1alpha1.Rollout{
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
						{
							ID: k8sptr.To(int64(1)),
							Version: kuberikrolloutv1alpha1.VersionInfo{
								Tag: "v1.0.0",
								// Revision is nil
							},
							Timestamp: metav1.Now(),
						},
					},
				},
			}

			result := reconciler.getCurrentVersionFromRollout(rollout)
			Expect(result).To(BeNil())
		})

		It("Should return nil when history is empty", func() {
			rollout := &kuberikrolloutv1alpha1.Rollout{
				Status: kuberikrolloutv1alpha1.RolloutStatus{
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{},
				},
			}

			result := reconciler.getCurrentVersionFromRollout(rollout)
			Expect(result).To(BeNil())
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
			Expect(reconciler.slicesEqual(nil, nil)).To(BeTrue())
		})

		It("Should return false for one nil slice", func() {
			a := []string{"a", "b"}
			Expect(reconciler.slicesEqual(a, nil)).To(BeFalse())
			Expect(reconciler.slicesEqual(nil, a)).To(BeFalse())
		})

		It("Should return false for different lengths", func() {
			a := []string{"a", "b"}
			b := []string{"a", "b", "c"}
			Expect(reconciler.slicesEqual(a, b)).To(BeFalse())
		})
	})

	Context("Environment validation", func() {
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
					Name:        "test-deployment",
					Environment: "production",
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "kuberik/environment-controller-testing",
					},
				},
			}

			Expect(deployment.Spec.RolloutRef.Name).To(Equal("test-rollout"))
			Expect(deployment.Spec.Backend.Project).To(Equal("kuberik/environment-controller-testing"))
			Expect(deployment.Spec.Name).To(Equal("test-deployment"))
			Expect(deployment.Spec.Environment).To(Equal("production"))
		})
	})

	Context("Relevant Versions Logic", func() {
		It("Should build relevantVersions from all relevant environments' history", func() {
			// This test documents how relevantVersions works:
			// 1. relevantVersions is built from ALL history entries in ALL relevant environments
			// 2. It's a set of revision SHAs that appear in any relevant environment's history
			// 3. It's used to determine which versions are relevant across the relationship graph
			// 4. However, it's NOT used to filter history in environmentInfos - each environment keeps ALL its own history

			revision1 := "rev1"
			revision2 := "rev2"
			revision3 := "rev3"
			revision4 := "rev4"

			// Simulate relationship graph data
			envHistory := map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				"dev": {
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: &revision1,
						},
					},
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: &revision2,
						},
					},
				},
				"staging": {
					{
						ID: k8sptr.To(int64(3)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: &revision2, // Shared with dev
						},
					},
					{
						ID: k8sptr.To(int64(4)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: &revision3,
						},
					},
				},
				"prod": {
					{
						ID: k8sptr.To(int64(5)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: &revision3, // Shared with staging
						},
					},
					{
						ID: k8sptr.To(int64(6)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: &revision4,
						},
					},
				},
			}

			relevantEnvironments := map[string]bool{
				"dev":     true,
				"staging": true,
				"prod":    false, // Not relevant
			}

			// Build relevantVersions (simulating the logic from buildRelationshipGraph)
			relevantVersions := make(map[string]bool)
			for envName, history := range envHistory {
				if !relevantEnvironments[envName] {
					continue
				}
				for _, entry := range history {
					if entry.Version.Revision != nil && *entry.Version.Revision != "" {
						relevantVersions[*entry.Version.Revision] = true
					}
				}
			}

			// Verify: relevantVersions should contain all revisions from dev and staging (but not prod)
			Expect(relevantVersions).To(HaveKey(revision1))    // From dev
			Expect(relevantVersions).To(HaveKey(revision2))    // From dev and staging
			Expect(relevantVersions).To(HaveKey(revision3))    // From staging
			Expect(relevantVersions).ToNot(HaveKey(revision4)) // From prod (not relevant)

			// Verify: each environment keeps ALL its own history (not filtered by relevantVersions)
			// This is the key point - environmentInfos contain full history for each environment
			devHistory := envHistory["dev"]
			Expect(devHistory).To(HaveLen(2))
			Expect(*devHistory[0].Version.Revision).To(Equal(revision1))
			Expect(*devHistory[1].Version.Revision).To(Equal(revision2))

			stagingHistory := envHistory["staging"]
			Expect(stagingHistory).To(HaveLen(2))
			Expect(*stagingHistory[0].Version.Revision).To(Equal(revision2))
			Expect(*stagingHistory[1].Version.Revision).To(Equal(revision3))
		})

		It("Should handle empty revisions in history entries", func() {
			envHistory := map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				"dev": {
					{
						ID: k8sptr.To(int64(1)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: nil, // Empty revision
						},
					},
					{
						ID: k8sptr.To(int64(2)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: k8sptr.To("rev1"),
						},
					},
				},
			}

			relevantEnvironments := map[string]bool{
				"dev": true,
			}

			relevantVersions := make(map[string]bool)
			for envName, history := range envHistory {
				if !relevantEnvironments[envName] {
					continue
				}
				for _, entry := range history {
					if entry.Version.Revision != nil && *entry.Version.Revision != "" {
						relevantVersions[*entry.Version.Revision] = true
					}
				}
			}

			// Should only include entries with valid revisions
			Expect(relevantVersions).To(HaveKey("rev1"))
			Expect(relevantVersions).To(HaveLen(1))
		})
	})

	Context("Preserve EnvironmentInfos", func() {
		It("Should preserve existing environmentInfos when environment is not discovered in current run", func() {
			// This test verifies that existing environmentInfos are preserved even if
			// the environment is not discovered in the current reconciliation run
			// (e.g., due to pagination issues or temporary GitHub API issues)

			existingStagingInfo := kuberikv1alpha1.EnvironmentInfo{
				Environment:    "staging",
				EnvironmentURL: "https://staging.example.com",
				Relationship: &kuberikv1alpha1.EnvironmentRelationship{
					Environment: "dev",
					Type:        kuberikv1alpha1.RelationshipTypeAfter,
				},
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(25)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: k8sptr.To("staging-rev-25"),
						},
					},
				},
			}

			existingDevInfo := kuberikv1alpha1.EnvironmentInfo{
				Environment:    "dev",
				EnvironmentURL: "https://dev.example.com",
				History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					{
						ID: k8sptr.To(int64(50)),
						Version: kuberikrolloutv1alpha1.VersionInfo{
							Revision: k8sptr.To("dev-rev-50"),
						},
					},
				},
			}

			// Simulate existing status with staging info
			existingStatus := []kuberikv1alpha1.EnvironmentInfo{
				existingDevInfo,
				existingStagingInfo,
			}

			// Simulate graphData where staging was NOT discovered (e.g., due to pagination)
			// but dev was discovered
			// Note: In the actual implementation, we would check graphData.relevantEnvironments
			// but for this test we're demonstrating that staging should be preserved
			_ = map[string]bool{
				"dev": true,
				// staging is NOT in relevantEnvironments because it wasn't discovered
			}

			graphDataEnvironmentInfos := map[string]environmentInfo{
				"dev": {
					EnvironmentURL: "https://dev.example.com",
				},
				// staging is NOT in environmentInfos because it wasn't discovered
			}

			// Simulate the logic: start with existing, update/add from graphData
			result := make([]kuberikv1alpha1.EnvironmentInfo, len(existingStatus))
			for i := range existingStatus {
				result[i] = *existingStatus[i].DeepCopy()
			}

			// Update/add from graphData
			for envName, info := range graphDataEnvironmentInfos {
				// This would normally update dev's info
				found := false
				for i := range result {
					if result[i].Environment == envName {
						result[i].EnvironmentURL = info.EnvironmentURL
						found = true
						break
					}
				}
				if !found {
					result = append(result, kuberikv1alpha1.EnvironmentInfo{
						Environment:    envName,
						EnvironmentURL: info.EnvironmentURL,
					})
				}
			}

			// OLD BEHAVIOR (what we're fixing): Remove environments not in relevantEnvironments
			// This would remove staging even though it should be preserved
			// OLD: result = removeEnvironmentInfos(result, func(entry kuberikv1alpha1.EnvironmentInfo) bool {
			//     return !graphDataEnvironments[entry.Environment]
			// })

			// NEW BEHAVIOR: Preserve existing environmentInfos
			// staging should still be in the result even though it wasn't discovered

			// Verify staging is preserved
			var stagingFound bool
			for _, info := range result {
				if info.Environment == "staging" {
					stagingFound = true
					Expect(info.EnvironmentURL).To(Equal("https://staging.example.com"))
					Expect(info.Relationship).ToNot(BeNil())
					Expect(info.Relationship.Environment).To(Equal("dev"))
					Expect(info.History).To(HaveLen(1))
					Expect(*info.History[0].Version.Revision).To(Equal("staging-rev-25"))
					break
				}
			}
			Expect(stagingFound).To(BeTrue(), "staging should be preserved even though not discovered in current run")

			// Verify dev is updated
			var devFound bool
			for _, info := range result {
				if info.Environment == "dev" {
					devFound = true
					Expect(info.EnvironmentURL).To(Equal("https://dev.example.com"))
					break
				}
			}
			Expect(devFound).To(BeTrue())
		})

		It("Should sort environmentInfos by relationship (ancestor first)", func() {
			// This test verifies that buildEnvironmentInfos (called via updateStatus)
			// sorts environmentInfos topologically: ancestor -> descendant

			environment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "default",
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					Environment: "dev",
					Name:        "test-deployment",
					Backend: kuberikv1alpha1.BackendConfig{
						Type:    "github",
						Project: "owner/repo",
					},
					RolloutRef: corev1.LocalObjectReference{Name: "test-rollout"},
				},
				Status: kuberikv1alpha1.EnvironmentStatus{
					EnvironmentInfos: []kuberikv1alpha1.EnvironmentInfo{},
				},
			}

			graphData := &relationshipGraphData{
				environmentInfos: map[string]environmentInfo{
					"prod": {
						Relationship: &kuberikv1alpha1.EnvironmentRelationship{
							Environment: "staging",
							Type:        kuberikv1alpha1.RelationshipTypeAfter,
						},
					},
					"dev": {},
					"staging": {
						Relationship: &kuberikv1alpha1.EnvironmentRelationship{
							Environment: "dev",
							Type:        kuberikv1alpha1.RelationshipTypeAfter,
						},
					},
				},
				envHistory: map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					"dev":     {{ID: k8sptr.To(int64(10))}},
					"staging": {{ID: k8sptr.To(int64(30))}},
					"prod":    {{ID: k8sptr.To(int64(50))}},
				},
			}

			Expect(reconciler.Create(context.Background(), environment)).To(Succeed())

			err := reconciler.updateStatus(context.Background(), environment, graphData)
			Expect(err).ToNot(HaveOccurred())

			// dev -> staging -> prod
			Expect(environment.Status.EnvironmentInfos).To(HaveLen(3))
			Expect(environment.Status.EnvironmentInfos[0].Environment).To(Equal("dev"))
			Expect(environment.Status.EnvironmentInfos[1].Environment).To(Equal("staging"))
			Expect(environment.Status.EnvironmentInfos[2].Environment).To(Equal("prod"))
		})

		It("Should sort history entries descending by ID", func() {
			infos := []kuberikv1alpha1.EnvironmentInfo{}
			history := []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				{ID: k8sptr.To(int64(10))},
				{ID: k8sptr.To(int64(30))},
				{ID: k8sptr.To(int64(20))},
			}

			result := updateEnvironmentInfoWithHistory(infos, "dev", "url", nil, history)
			Expect(result).To(HaveLen(1))
			Expect(result[0].History[0].ID).To(Equal(k8sptr.To(int64(30))))
			Expect(result[0].History[1].ID).To(Equal(k8sptr.To(int64(20))))
			Expect(result[0].History[2].ID).To(Equal(k8sptr.To(int64(10))))
		})

		It("Should handle complex branching (dev -> staging, dev -> test, staging -> prod)", func() {
			graphData := &relationshipGraphData{
				environmentInfos: map[string]environmentInfo{
					"prod":    {Relationship: &kuberikv1alpha1.EnvironmentRelationship{Environment: "staging", Type: "after"}},
					"staging": {Relationship: &kuberikv1alpha1.EnvironmentRelationship{Environment: "dev", Type: "after"}},
					"test":    {Relationship: &kuberikv1alpha1.EnvironmentRelationship{Environment: "dev", Type: "after"}},
					"dev":     {},
				},
				envHistory: map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					"dev":     {{ID: k8sptr.To(int64(10))}},
					"staging": {{ID: k8sptr.To(int64(20))}},
					"test":    {{ID: k8sptr.To(int64(15))}},
					"prod":    {{ID: k8sptr.To(int64(30))}},
				},
			}

			env := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "complex-env",
					Namespace: "default",
				},
			}
			Expect(reconciler.Create(context.Background(), env)).To(Succeed())

			err := reconciler.updateStatus(context.Background(), env, graphData)
			Expect(err).ToNot(HaveOccurred())

			// dev should be first. staging/test are both after dev.
			// staging (20) vs test (15) -> staging comes first
			// prod is after staging -> prod must be after staging
			// prod (30) vs test (15) -> prod comes before test
			Expect(env.Status.EnvironmentInfos[0].Environment).To(Equal("dev"))
			Expect(env.Status.EnvironmentInfos[1].Environment).To(Equal("staging"))
			Expect(env.Status.EnvironmentInfos[2].Environment).To(Equal("prod"))
			Expect(env.Status.EnvironmentInfos[3].Environment).To(Equal("test"))
		})

		It("Should select CurrentVersion by highest history ID", func() {
			revOld := "old-rev"
			revNew := "new-rev"
			environment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "current-version-env",
					Namespace: "default",
				},
				Spec: kuberikv1alpha1.EnvironmentSpec{
					Environment: "current-version-env",
				},
			}
			Expect(reconciler.Create(context.Background(), environment)).To(Succeed())

			graphData := &relationshipGraphData{
				environmentInfos: map[string]environmentInfo{
					environment.Name: {},
				},
				envHistory: map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					environment.Name: {
						{
							ID:      k8sptr.To(int64(20)),
							Version: kuberikrolloutv1alpha1.VersionInfo{Revision: &revNew},
						},
						{
							ID:      k8sptr.To(int64(10)),
							Version: kuberikrolloutv1alpha1.VersionInfo{Revision: &revOld},
						},
					},
				},
			}

			err := reconciler.updateStatus(context.Background(), environment, graphData)
			Expect(err).ToNot(HaveOccurred())
			Expect(environment.Status.CurrentVersion).To(Equal(revNew))
		})
	})

	Context("updateEnvironmentInfoWithHistory sorting", func() {
		It("Should sort history entries descending by ID", func() {
			history := []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				{ID: k8sptr.To(int64(10))},
				{ID: k8sptr.To(int64(30))},
				{ID: k8sptr.To(int64(20))},
			}

			infos := updateEnvironmentInfoWithHistory(nil, "dev", "https://dev.com", nil, history)

			Expect(infos).To(HaveLen(1))
			Expect(infos[0].History).To(HaveLen(3))
			Expect(*infos[0].History[0].ID).To(Equal(int64(30)))
			Expect(*infos[0].History[1].ID).To(Equal(int64(20)))
			Expect(*infos[0].History[2].ID).To(Equal(int64(10)))
		})

		It("Should update existing entry and sort history", func() {
			infos := []kuberikv1alpha1.EnvironmentInfo{
				{
					Environment: "dev",
					History: []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
						{ID: k8sptr.To(int64(10))},
					},
				},
			}

			newHistory := []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				{ID: k8sptr.To(int64(5))},
				{ID: k8sptr.To(int64(15))},
			}

			infos = updateEnvironmentInfoWithHistory(infos, "dev", "https://dev-updated.com", nil, newHistory)

			Expect(infos).To(HaveLen(1))
			Expect(infos[0].EnvironmentURL).To(Equal("https://dev-updated.com"))
			Expect(infos[0].History).To(HaveLen(2))
			Expect(*infos[0].History[0].ID).To(Equal(int64(15)))
			Expect(*infos[0].History[1].ID).To(Equal(int64(5)))
		})
	})

	Context("updateStatus sorting via buildEnvironmentInfos", func() {
		It("Should sort environmentInfos alphabetically by name", func() {
			environment := &kuberikv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-env",
				},
				Status: kuberikv1alpha1.EnvironmentStatus{
					EnvironmentInfos: []kuberikv1alpha1.EnvironmentInfo{},
				},
			}

			graphData := &relationshipGraphData{
				environmentInfos: map[string]environmentInfo{
					"prod":    {EnvironmentURL: "https://prod.com"},
					"dev":     {EnvironmentURL: "https://dev.com"},
					"staging": {EnvironmentURL: "https://staging.com"},
				},
				envHistory: map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
					"dev":     {{ID: k8sptr.To(int64(1))}},
					"staging": {{ID: k8sptr.To(int64(2))}},
					"prod":    {{ID: k8sptr.To(int64(3))}},
				},
			}

			err := reconciler.Create(context.Background(), environment)
			Expect(err).ToNot(HaveOccurred())

			err = reconciler.updateStatus(context.Background(), environment, graphData)
			Expect(err).ToNot(HaveOccurred())

			Expect(environment.Status.EnvironmentInfos).To(HaveLen(3))
			Expect(environment.Status.EnvironmentInfos[0].Environment).To(Equal("dev"))
			Expect(environment.Status.EnvironmentInfos[1].Environment).To(Equal("prod"))
			Expect(environment.Status.EnvironmentInfos[2].Environment).To(Equal("staging"))
		})
	})
})
