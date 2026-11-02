package controller

import (
	"fmt"
	"testing"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
	k8sptr "k8s.io/utils/ptr"
)

func TestBuildEnvironmentInfos_LimitsHistoryForIndirectConnections(t *testing.T) {
	// Setup
	r := &GitHubEnvironmentReconciler{}

	// Define environments
	// dev (current)
	// test (next tier) -> Direct
	// staging (two tiers on) -> Indirect

	currentEnvName := "dev"

	envA := &kuberikv1alpha1.Environment{
		Spec: kuberikv1alpha1.EnvironmentSpec{
			Environment: currentEnvName,
		},
		Status: kuberikv1alpha1.EnvironmentStatus{},
	}

	// Create history entries
	createHistory := func(cnt int) []kuberikrolloutv1alpha1.DeploymentHistoryEntry {
		h := make([]kuberikrolloutv1alpha1.DeploymentHistoryEntry, cnt)
		for i := 0; i < cnt; i++ {
			revision := fmt.Sprintf("rev-%d", i)
			tag := fmt.Sprintf("v1.0.%d", i)
			h[i] = kuberikrolloutv1alpha1.DeploymentHistoryEntry{
				ID: k8sptr.To(int64(cnt - i)), // 3, 2, 1
				Version: kuberikrolloutv1alpha1.VersionInfo{
					Revision: &revision,
					Tag:      tag,
				},
			}
		}
		return h
	}

	graphData := &environmentGraphData{
		environmentInfos: map[string]environmentInfo{
			"dev":     {},
			"test":    {},
			"staging": {},
		},
		envHistory: map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry{
			"dev":     createHistory(3),
			"test":    createHistory(3),
			"staging": createHistory(3),
		},
	}

	// Execute
	infos := r.buildEnvironmentInfos(envA, graphData)

	// Assert
	infoMap := make(map[string]kuberikv1alpha1.EnvironmentInfo)
	for _, info := range infos {
		infoMap[info.Environment] = info
	}

	// Helper to assert history length
	assertHistoryLen := func(envName string, expected int) {
		info, ok := infoMap[envName]
		if !ok {
			t.Errorf("Expected environment %s to be present", envName)
			return
		}
		if len(info.History) != expected {
			t.Errorf("Environment %s: expected history length %d, got %d", envName, expected, len(info.History))
		}
	}

	// dev: Current env, should have full history
	assertHistoryLen("dev", 3)

	// test: Directly connected (depends on current), should have full history
	assertHistoryLen("test", 3)

	// staging: Indirectly connected, should have LIMITED history (1)
	assertHistoryLen("staging", 1)
}

func TestUpdateEnvironmentInfoWithHistory_RemovesDuplicates(t *testing.T) {
	// Setup
	history := []kuberikrolloutv1alpha1.DeploymentHistoryEntry{
		{
			ID: k8sptr.To(int64(10)),
			Version: kuberikrolloutv1alpha1.VersionInfo{
				Revision: k8sptr.To("rev-10"),
				Tag:      "v1.0.0",
			},
		},
		{
			ID: k8sptr.To(int64(20)),
			Version: kuberikrolloutv1alpha1.VersionInfo{
				Revision: k8sptr.To("rev-20"),
				Tag:      "v1.0.1",
			},
		},
		{
			ID: k8sptr.To(int64(15)),
			Version: kuberikrolloutv1alpha1.VersionInfo{
				Revision: k8sptr.To("rev-15"),
				Tag:      "v1.0.0", // Duplicate tag
			},
		},
	}

	// Executue with nil relationship/url as they don't matter
	infos := updateEnvironmentInfoWithHistory(nil, "env", "url", history)

	// Assert
	if len(infos) != 1 {
		t.Fatalf("Expected 1 info, got %d", len(infos))
	}

	resultHistory := infos[0].History
	// Should have 2 entries: v1.0.1 (20) and v1.0.0 (15)
	// v1.0.0 (10) should be removed because 15 > 10 and they share tag v1.0.0

	if len(resultHistory) != 2 {
		t.Errorf("Expected 2 history entries (deduplicated), got %d", len(resultHistory))
	}

	// Check IDs
	if *resultHistory[0].ID != 20 {
		t.Errorf("Expected first entry ID 20, got %d", *resultHistory[0].ID)
	}
	if *resultHistory[1].ID != 15 {
		t.Errorf("Expected second entry ID 15, got %d", *resultHistory[1].ID)
	}
}
