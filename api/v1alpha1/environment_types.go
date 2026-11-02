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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RelationshipType defines the type of relationship between environments
type RelationshipType string

const (
	// RelationshipTypeAfter means this environment deploys a version only after the related
	// environment has deployed it successfully (its bake succeeded).
	RelationshipTypeAfter RelationshipType = "After"
	// RelationshipTypeAlongside means this environment follows the related environment and
	// deploys the same version alongside it: a version may deploy here as soon as the related
	// environment has started deploying it, as long as that deployment has not failed or been
	// cancelled. Use it for an environment that should track another environment's version
	// (including its manual approvals) without a gate of its own.
	RelationshipTypeAlongside RelationshipType = "Alongside"
)

// EnvironmentRelationship defines a relationship to another environment
type EnvironmentRelationship struct {
	// Environment is the environment name this environment relates to
	// +required
	Environment string `json:"environment"`

	// Type is the type of relationship:
	// - "After": deploy a version only once it has been successfully deployed to the related environment.
	// - "Alongside": deploy the same version as the related environment as soon as that environment
	//   starts deploying it, unless its deployment there failed or was cancelled.
	// +required
	// +kubebuilder:validation:Enum=After;Alongside
	Type RelationshipType `json:"type"`
}

// BackendConfig contains backend-specific configuration
type BackendConfig struct {
	// Type specifies the backend to use (e.g., "github")
	// +required
	// +kubebuilder:validation:Enum=github
	Type string `json:"type"`

	// Project is the project identifier (backend-specific format, e.g., "owner/repo" for GitHub)
	// +required
	Project string `json:"project"`

	// Secret is the name of the Kubernetes Secret containing the backend authentication token
	// +optional
	Secret string `json:"secret,omitempty"`
}

// EnvironmentSpec defines the desired state of Environment
type EnvironmentSpec struct {

	// RolloutRef is a reference to the Rollout that this Environment manages
	// +required
	RolloutRef corev1.LocalObjectReference `json:"rolloutRef"`

	// Name is the name of the GitHub deployment (the "kuberik" prefix will be automatically added for GitHub backend if not already present)
	// +required
	Name string `json:"name"`

	// Environment is the environment name (e.g., "production", "staging")
	// +optional
	Environment string `json:"environment,omitempty"`

	// Relationship defines how this environment relates to another environment
	// +optional
	Relationship *EnvironmentRelationship `json:"relationship,omitempty"`

	// Backend contains backend-specific configuration
	// +required
	Backend BackendConfig `json:"backend"`

	// Site identifies where this instance of the environment runs (e.g. a region
	// or cluster name such as "eu-west-1"). Members of the same environment that
	// run in different clusters are told apart by site. When empty, the
	// controller's default site is used (key "site" of the optional
	// kuberik-cluster-info ConfigMap in the controller namespace). When neither is
	// set, the Environment is site-less and behaves exactly as before sites existed.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`
	Site string `json:"site,omitempty"`

	// Wave is the rollout wave of this site within its environment ("prod wave 1").
	// Waves of one environment roll out in order: wave N starts a version only
	// after every site of wave N-1 baked it. Defaults to 1 when a site is set.
	// Ignored for site-less Environments.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Wave *int32 `json:"wave,omitempty"`

	// Lockstep, when true, stops the wave-1 leader from selecting a new version
	// until every wave of this environment has baked the currently selected one.
	// The wave-1 leader's value governs; a disagreeing member surfaces a
	// LockstepMismatch condition.
	// +optional
	Lockstep bool `json:"lockstep,omitempty"`

	// RequeueInterval specifies how often the controller should reconcile this Environment
	// If not specified, defaults to 1 minute. Must be a valid duration string (e.g., "1m", "30s", "5m").
	// +optional
	// +kubebuilder:validation:Pattern=^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$
	RequeueInterval string `json:"requeueInterval,omitempty"`
}

// EnvironmentStatusEntry represents a deployment status for a specific version in an environment.
type EnvironmentStatusEntry struct {
	// Environment is the environment name (e.g., "production", "staging")
	// +required
	Environment string `json:"environment"`

	// DeploymentHistoryEntry embeds the entire deployment history entry from the Rollout
	kuberikrolloutv1alpha1.DeploymentHistoryEntry `json:",inline"`
}

// EnvironmentInfo represents information about an environment's deployment.
type EnvironmentInfo struct {
	// Environment is the environment name
	// +required
	Environment string `json:"environment"`

	// EnvironmentURL is the URL of the actual deployed environment (e.g., dashboard URL)
	// +optional
	EnvironmentURL string `json:"environmentUrl,omitempty"`

	// Relationship defines how this environment relates to another environment
	// +optional
	Relationship *EnvironmentRelationship `json:"relationship,omitempty"`

	// History contains deployment history entries for this environment
	// +listType=atomic
	// +optional
	History []kuberikrolloutv1alpha1.DeploymentHistoryEntry `json:"history,omitempty"`
}

// WaveRole is the role a site plays inside its (environment, wave).
type WaveRole string

const (
	// WaveRoleLeader selects the version for its wave.
	WaveRoleLeader WaveRole = "Leader"
	// WaveRoleFollower deploys whatever the leader of its wave selected.
	WaveRoleFollower WaveRole = "Follower"
)

// WaveMemberStatus is one site of a wave as observed in the backend.
type WaveMemberStatus struct {
	// Environment of the member.
	Environment string `json:"environment"`
	// Site of the member.
	Site string `json:"site"`
	// Wave of the member.
	Wave int32 `json:"wave"`
	// Revision of the member's newest deployment.
	// +optional
	Revision string `json:"revision,omitempty"`
	// BakeStatus of the member's newest deployment.
	// +optional
	BakeStatus string `json:"bakeStatus,omitempty"`
	// Deregistered is true when the member was removed and no longer counts.
	// +optional
	Deregistered bool `json:"deregistered,omitempty"`
}

// WaveStatus describes the site/wave view the controller used for the last gate decision.
type WaveStatus struct {
	// Site is the effective site of this Environment (spec or controller default).
	Site string `json:"site"`
	// Wave is the effective wave of this Environment.
	Wave int32 `json:"wave"`
	// Role is Leader or Follower within (environment, wave). It is also the role
	// recorded on the next GitHub deployment this site creates.
	// +optional
	Role WaveRole `json:"role,omitempty"`
	// Leader is the site leading this (environment, wave).
	// +optional
	Leader string `json:"leader,omitempty"`
	// SelectedRevision is the revision the leader of this wave currently selected.
	// +optional
	SelectedRevision string `json:"selectedRevision,omitempty"`
	// Members are the registered sites of this environment and of its upstream environment.
	// +listType=atomic
	// +optional
	Members []WaveMemberStatus `json:"members,omitempty"`
}

// EnvironmentStatus defines the observed state of Environment.
type EnvironmentStatus struct {
	// LastStatusChangeTime is the last time the status was updated (only updated when status changes)
	// +optional
	LastStatusChangeTime *metav1.Time `json:"lastStatusChangeTime,omitempty"`

	// CurrentVersion is the current version being deployed
	// +optional
	CurrentVersion string `json:"currentVersion,omitempty"`

	// RolloutGateRef is a reference to the RolloutGate that was created/updated
	// +optional
	RolloutGateRef *corev1.LocalObjectReference `json:"rolloutGateRef,omitempty"`

	// EnvironmentInfos tracks deployment information for each environment.
	// Each environment has environment URL and relationships (not per version).
	// +listType=atomic
	// +optional
	EnvironmentInfos []EnvironmentInfo `json:"environmentInfos,omitempty"`

	// Wave is set for Environments with a site and describes their wave state.
	// +optional
	Wave *WaveStatus `json:"wave,omitempty"`

	// conditions represent the current state of the Environment resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Environment is the Schema for the environments API
type Environment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of Environment
	// +required
	Spec EnvironmentSpec `json:"spec"`

	// status defines the observed state of Environment
	// +optional
	Status EnvironmentStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// EnvironmentList contains a list of Environment
type EnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Environment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Environment{}, &EnvironmentList{})
}
