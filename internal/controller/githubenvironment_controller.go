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
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	kuberikv1alpha1 "github.com/kuberik/environment-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// deploymentPayload represents the payload stored in GitHub deployments
// Only immutable data should be stored here since GitHub deployments cannot be updated
type deploymentPayload struct {
	ID           string                                   `json:"id"`                     // History entry ID (immutable)
	Relationship *kuberikv1alpha1.EnvironmentRelationship `json:"relationship,omitempty"` // Relationship for this environment
	Version      *kuberikrolloutv1alpha1.VersionInfo      `json:"version,omitempty"`      // Version information (immutable)
}

// deploymentKey represents a unique key for mapping deployments by ID and environment
type deploymentKey struct {
	ID          string
	Environment string
}

// GitHubEnvironmentReconciler reconciles an Environment object for GitHub backend
type GitHubEnvironmentReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	CacheTransport http.RoundTripper
}

// +kubebuilder:rbac:groups=environments.kuberik.com,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=environments.kuberik.com,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=environments.kuberik.com,resources=environments/finalizers,verbs=update
// +kubebuilder:rbac:groups=kuberik.com,resources=rolloutgates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kuberik.com,resources=rollouts,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *GitHubEnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the Environment instance
	deployment := &kuberikv1alpha1.Environment{}
	err := r.Get(ctx, req.NamespacedName, deployment)
	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to get Environment")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate backend
	if deployment.Spec.Backend.Type != "github" {
		return ctrl.Result{}, fmt.Errorf("unsupported backend: %s", deployment.Spec.Backend.Type)
	}

	// Create or update RolloutGate before any GitHub API calls. The gate starts
	// blocking immediately (allowedVersions=[]) when a relationship is configured,
	// so the rollout controller cannot deploy to this environment before the gate
	// has been populated with versions that passed the upstream environment.
	if err := r.createOrUpdateRolloutGate(ctx, deployment); err != nil {
		log.Error(err, "Failed to create or update RolloutGate")
		return ctrl.Result{}, err
	}

	// Get the referenced Rollout to get the current version
	rollout, err := r.getReferencedRollout(ctx, deployment)
	if err != nil {
		log.Error(err, "Failed to get referenced Rollout")
		return ctrl.Result{}, err
	}

	// Get GitHub client
	githubClient, err := r.getGitHubClient(ctx, deployment)
	if err != nil {
		log.Error(err, "Failed to get GitHub client")
		return ctrl.Result{}, err
	}

	// Sync entire rollout history with GitHub deployments and statuses
	if err := r.syncDeploymentHistory(ctx, githubClient, deployment, rollout); err != nil {
		log.Error(err, "Failed to sync deployment history")
		return ctrl.Result{}, err
	}

	// Build relationship graph
	graphData, err := r.buildRelationshipGraph(ctx, deployment, githubClient)
	if err != nil {
		log.Error(err, "Failed to build relationship graph")
		return ctrl.Result{}, err
	}

	// Calculate and update Status
	if err := r.updateStatus(ctx, deployment, graphData); err != nil {
		log.Error(err, "Failed to update Environment status")
		return ctrl.Result{}, err
	}

	// Update allowed versions on RolloutGate based on relationships
	if err := r.updateAllowedVersionsFromRelationships(ctx, deployment); err != nil {
		log.Error(err, "Failed to update allowed versions from relationships")
		return ctrl.Result{}, err
	}

	// Requeue at configured interval (default 1 minute) to keep status updated
	requeueInterval := time.Minute // default
	if deployment.Spec.RequeueInterval != "" {
		parsed, err := time.ParseDuration(deployment.Spec.RequeueInterval)
		if err != nil {
			log.Error(err, "Invalid RequeueInterval, using default", "interval", deployment.Spec.RequeueInterval)
		} else {
			requeueInterval = parsed
		}
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GitHubEnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kuberikv1alpha1.Environment{}).
		Watches(
			&kuberikrolloutv1alpha1.Rollout{},
			handler.EnqueueRequestsFromMapFunc(r.rolloutToEnvironment),
		).
		Named("environment").
		Complete(r)
}

// rolloutToEnvironment maps a Rollout to all Environments that reference it
func (r *GitHubEnvironmentReconciler) rolloutToEnvironment(ctx context.Context, obj client.Object) []reconcile.Request {
	rollout := obj.(*kuberikrolloutv1alpha1.Rollout)
	requests := []reconcile.Request{}

	// List all Environments in the same namespace
	deploymentList := &kuberikv1alpha1.EnvironmentList{}
	if err := r.List(ctx, deploymentList, client.InNamespace(rollout.Namespace)); err != nil {
		return requests
	}

	// Find all Environments that reference this Rollout
	for i := range deploymentList.Items {
		deployment := &deploymentList.Items[i]
		if deployment.Spec.RolloutRef.Name == rollout.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      deployment.Name,
					Namespace: deployment.Namespace,
				},
			})
		}
	}

	return requests
}

// getReferencedRollout gets the Rollout referenced by the Environment
// It uses the regular client which uses cache, but the cache should be kept up to date
// by the controller-runtime watch mechanism. For related environments, we want fresh data
// so we ensure we get the latest status.
func (r *GitHubEnvironmentReconciler) getReferencedRollout(ctx context.Context, deployment *kuberikv1alpha1.Environment) (*kuberikrolloutv1alpha1.Rollout, error) {
	rollout := &kuberikrolloutv1alpha1.Rollout{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      deployment.Spec.RolloutRef.Name,
		Namespace: deployment.Namespace,
	}, rollout)
	if err != nil {
		return nil, fmt.Errorf("failed to get Rollout %s: %w", deployment.Spec.RolloutRef.Name, err)
	}
	return rollout, nil
}

// getGitHubClient creates a GitHub client using the token from the specified secret
// The client uses ghcache for conditional requests with caching to reduce API rate limit consumption
// ghcache automatically partitions the cache by auth header, ensuring proper token isolation
func (r *GitHubEnvironmentReconciler) getGitHubClient(ctx context.Context, deployment *kuberikv1alpha1.Environment) (*github.Client, error) {
	secretName := deployment.Spec.Backend.Secret
	if secretName == "" {
		secretName = "github-token" // Default secret name
	}

	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: deployment.Namespace,
	}, secret)
	if err != nil {
		return nil, fmt.Errorf("failed to get GitHub token secret %s: %w", secretName, err)
	}

	token, exists := secret.Data["token"]
	if !exists {
		return nil, fmt.Errorf("token key not found in secret %s", secretName)
	}

	// Create HTTP client with ghcache transport
	// If CacheTransport is not set, use default transport (for tests)
	transport := r.CacheTransport
	if transport == nil {
		transport = http.DefaultTransport
	}

	httpClient := &http.Client{
		Transport: transport,
	}

	// Create GitHub client with OAuth2 authentication and caching
	return github.NewClient(httpClient).WithAuthToken(string(token)), nil
}

// createDeploymentStatus creates a GitHub deployment status for the given environment
func (r *GitHubEnvironmentReconciler) createDeploymentStatus(ctx context.Context, client *github.Client, environment *kuberikv1alpha1.Environment, deploymentID int64, state string, description string) error {
	owner, repo, err := parseProject(environment.Spec.Backend.Project)
	if err != nil {
		return err
	}

	// Ensure deployment name has kuberik prefix for GitHub
	deploymentName := ensureKuberikPrefix(environment.Spec.Name)
	// Format environment as "deploymentName/environment" for GitHub
	formattedEnv := formatDeploymentEnvironment(deploymentName, environment.Spec.Environment)

	// Get rollout-dashboard URL from ingress/gateway in controller's namespace
	// The URL will include the path /rollouts/<namespace>/<name>
	environmentURL := r.getRolloutDashboardURL(ctx, environment.Namespace, environment.Name)

	// Create deployment status request
	statusRequest := &github.DeploymentStatusRequest{
		State:       github.String(state),
		Description: github.String(description),
		Environment: &formattedEnv,
	}
	if environmentURL != "" {
		statusRequest.EnvironmentURL = github.String(environmentURL)
	}

	// Create the deployment status
	_, _, createErr := client.Repositories.CreateDeploymentStatus(ctx, owner, repo, deploymentID, statusRequest)
	if createErr != nil {
		return fmt.Errorf("failed to create deployment status: %w", createErr)
	}

	return nil
}

// getCurrentVersionFromRollout extracts the current version from the rollout's deployment history
func (r *GitHubEnvironmentReconciler) getCurrentVersionFromRollout(rollout *kuberikrolloutv1alpha1.Rollout) *string {
	// Get the most recent deployment from history
	if len(rollout.Status.History) == 0 {
		return nil
	}

	// The history is ordered with the most recent deployment first
	latestDeployment := rollout.Status.History[0]

	// Always use the Revision field from VersionInfo - if not available, return nil
	if latestDeployment.Version.Revision == nil {
		return nil
	}

	return latestDeployment.Version.Revision
}

// updateStatus updates the Environment status with all gathered information
func (r *GitHubEnvironmentReconciler) updateStatus(ctx context.Context, environment *kuberikv1alpha1.Environment, graphData *relationshipGraphData) error {
	newStatus := environment.Status.DeepCopy()

	// Update current version - get from graphData history for current environment
	currentEnv := environment.Spec.Environment
	if history, ok := graphData.envHistory[currentEnv]; ok && len(history) > 0 {
		// History is already sorted descending
		if history[0].Version.Revision != nil {
			newStatus.CurrentVersion = *history[0].Version.Revision
		}
	}

	// Update environment infos
	environmentInfoList := r.buildEnvironmentInfos(environment, graphData)
	newStatus.EnvironmentInfos = environmentInfoList

	// Check if status changed
	needsUpdate := false

	if newStatus.CurrentVersion != environment.Status.CurrentVersion {
		needsUpdate = true
	}

	if !environmentInfosEqual(newStatus.EnvironmentInfos, environment.Status.EnvironmentInfos) {
		needsUpdate = true
	}

	// Note: RolloutGateRef is updated separately by createOrUpdateRolloutGate if changed,
	// but we should arguably handle it here if we want strictly single status update.
	// For now, let's stick to the plan of handling what we can here.
	// Actually, createOrUpdateRolloutGate assumes it can update status.
	// To strictly follow "update status first", we should probably separate creation from status update.
	// But let's leave RolloutGateRef update to its function for now as it's coupled with creation.

	if needsUpdate {
		// Update last status change time only when status actually changes
		now := metav1.Now()
		newStatus.LastStatusChangeTime = &now
		environment.Status = *newStatus
		return r.Status().Update(ctx, environment)
	}

	return nil
}

// buildEnvironmentInfos builds the list of environment infos from graph data
func (r *GitHubEnvironmentReconciler) buildEnvironmentInfos(environment *kuberikv1alpha1.Environment, graphData *relationshipGraphData) []kuberikv1alpha1.EnvironmentInfo {
	if environment.Status.EnvironmentInfos == nil {
		environment.Status.EnvironmentInfos = []kuberikv1alpha1.EnvironmentInfo{}
	}

	// Make a deep copy of the environment infos
	environmentInfoList := make([]kuberikv1alpha1.EnvironmentInfo, len(environment.Status.EnvironmentInfos))
	for i := range environment.Status.EnvironmentInfos {
		environmentInfoList[i] = *environment.Status.EnvironmentInfos[i].DeepCopy()
	}

	// Update or add environment infos with history from graphData
	for envName, info := range graphData.environmentInfos {
		// Get history for this environment
		history := []kuberikrolloutv1alpha1.DeploymentHistoryEntry{}
		if envHistory, exists := graphData.envHistory[envName]; exists {
			for _, entry := range envHistory {
				if entry.Version.Revision != nil && *entry.Version.Revision != "" {
					history = append(history, entry)
				}
			}
		}

		// Check if environment is directly connected to current environment
		// - It is the current environment
		// - Current environment depends on it
		// - It depends on current environment
		isDirectlyConnected := false
		currentEnv := environment.Spec.Environment

		if envName == currentEnv {
			isDirectlyConnected = true
		} else {
			// Check if current environment depends on this env
			if environment.Spec.Relationship != nil && environment.Spec.Relationship.Environment == envName {
				isDirectlyConnected = true
			}
			// Check if this env depends on current environment
			if info.Relationship != nil && info.Relationship.Environment == currentEnv {
				isDirectlyConnected = true
			}
		}

		// If not directly connected, keep only the latest deployment history
		if !isDirectlyConnected && len(history) > 1 {
			// History is already sorted descending by ID/Revision in buildRelationshipGraph
			history = []kuberikrolloutv1alpha1.DeploymentHistoryEntry{history[0]}
		}

		environmentInfoList = updateEnvironmentInfoWithHistory(environmentInfoList, envName, info.EnvironmentURL, info.Relationship, history)
	}

	// Preserve existing environmentInfos... (same logic as before)
	// Actually we should just keep what we have in environmentInfoList since we copied from Status
	// and updateEnvironmentInfoWithHistory updates in place or appends.

	// Sort (same logic as before)
	// Pre-calculate max IDs and parent map
	maxIDMap := make(map[string]int64)
	parentMap := make(map[string]string)
	for _, info := range environmentInfoList {
		maxIDMap[info.Environment] = getMaxHistoryID(info.History)
		if info.Relationship != nil && info.Relationship.Environment != "" {
			parentMap[info.Environment] = info.Relationship.Environment
		}
	}

	// Recursive helper to check if envA is an ancestor of envB
	var isAncestor func(envA, envB string) bool
	isAncestor = func(envA, envB string) bool {
		parent, exists := parentMap[envB]
		if !exists {
			return false
		}
		if parent == envA {
			return true
		}
		return isAncestor(envA, parent)
	}

	sort.SliceStable(environmentInfoList, func(i, j int) bool {
		envI := environmentInfoList[i].Environment
		envJ := environmentInfoList[j].Environment

		// If envI is an ancestor of envJ, envI comes first
		if isAncestor(envI, envJ) {
			return true
		}
		// If envJ is an ancestor of envI, envJ comes first
		if isAncestor(envJ, envI) {
			return false
		}

		// Tie-breaker: higher max history ID first
		idI := maxIDMap[envI]
		idJ := maxIDMap[envJ]
		if idI != idJ {
			return idI > idJ
		}

		// Last tie-breaker: environment name for stable sort
		return envI < envJ
	})

	return environmentInfoList
}

// formatDeploymentTask formats the deployment name into the task format "deploy:<name>"
func formatDeploymentTask(deploymentName string) string {
	return fmt.Sprintf("deploy:%s", deploymentName)
}

// formatDeploymentEnvironment formats the environment as "deploymentName:environment"
func formatDeploymentEnvironment(deploymentName, environment string) string {
	return fmt.Sprintf("%s/%s", deploymentName, environment)
}

// parseProject parses the project string into owner and repo
func parseProject(project string) (owner, repo string, err error) {
	parts := strings.Split(project, "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid project format: %s", project)
	}
	return parts[0], parts[1], nil
}

// ensureKuberikPrefix ensures the deployment name starts with "kuberik" prefix, adding it if missing
func ensureKuberikPrefix(deploymentName string) string {
	if strings.HasPrefix(deploymentName, "kuberik/") {
		return deploymentName
	}
	return "kuberik/" + deploymentName
}

// getControllerNamespace gets the namespace where the controller is running.
// It reads from the service account namespace file, or falls back to environment variable.
func (r *GitHubEnvironmentReconciler) getControllerNamespace() string {
	// Try reading from service account namespace file (standard in Kubernetes pods)
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns
		}
	}
	// Fallback to environment variable
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	// Last resort: try WATCH_NAMESPACE (used by some operators)
	if ns := os.Getenv("WATCH_NAMESPACE"); ns != "" {
		return ns
	}
	return ""
}

// getRolloutDashboardURL finds the URL for rollout-dashboard service from ingress or gateway
// in the controller's namespace. Returns empty string if not found.
// The URL will have the path /rollouts/<deploymentNamespace>/<deploymentName> appended.
func (r *GitHubEnvironmentReconciler) getRolloutDashboardURL(ctx context.Context, deploymentNamespace, deploymentName string) string {
	// Get the controller's namespace
	controllerNamespace := r.getControllerNamespace()
	if controllerNamespace == "" {
		return ""
	}

	// First, check if rollout-dashboard service exists in controller's namespace
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      "rollout-dashboard",
		Namespace: controllerNamespace,
	}, svc)
	if err != nil {
		// Service doesn't exist, return empty
		return ""
	}

	// List all ingresses in the controller's namespace
	ingressList := &networkingv1.IngressList{}
	if err := r.List(ctx, ingressList, client.InNamespace(controllerNamespace)); err != nil {
		return ""
	}

	// Find ingress that points to rollout-dashboard service
	var baseURL string
	for i := range ingressList.Items {
		ingress := &ingressList.Items[i]

		// Check default backend first
		if ingress.Spec.DefaultBackend != nil && ingress.Spec.DefaultBackend.Service != nil {
			if ingress.Spec.DefaultBackend.Service.Name == "rollout-dashboard" {
				// For default backend, we need at least one rule with a host
				// If no rules, we can't construct a URL
				if len(ingress.Spec.Rules) > 0 && ingress.Spec.Rules[0].Host != "" {
					scheme := "https"
					if len(ingress.Spec.TLS) == 0 {
						scheme = "http"
					}
					baseURL = fmt.Sprintf("%s://%s", scheme, ingress.Spec.Rules[0].Host)
					break
				}
			}
		}

		// Check rules and paths
		for _, rule := range ingress.Spec.Rules {
			if rule.Host == "" {
				continue
			}

			// Check if any path in this rule points to rollout-dashboard service
			for _, path := range rule.HTTP.Paths {
				if path.Backend.Service != nil && path.Backend.Service.Name == "rollout-dashboard" {
					// Construct base URL
					scheme := "https"
					if len(ingress.Spec.TLS) == 0 {
						scheme = "http"
					}
					baseURL = fmt.Sprintf("%s://%s", scheme, rule.Host)
					// Note: We ignore the ingress path and use our own path instead
					break
				}
			}
			if baseURL != "" {
				break
			}
		}
		if baseURL != "" {
			break
		}
	}

	if baseURL == "" {
		// Check Gateway API (HTTPRoute) if Ingress not found
		httpRouteList := &gatewayv1.HTTPRouteList{}
		if err := r.List(ctx, httpRouteList, client.InNamespace(controllerNamespace)); err != nil {
			return ""
		}

		// Find HTTPRoute that points to rollout-dashboard service
		for i := range httpRouteList.Items {
			httpRoute := &httpRouteList.Items[i]

			// Check if any rule references rollout-dashboard service
			for _, rule := range httpRoute.Spec.Rules {
				for _, backendRef := range rule.BackendRefs {
					// Check if this backendRef points to rollout-dashboard service
					// Default to Service kind if not specified
					kind := gatewayv1.Kind("Service")
					if backendRef.Kind != nil {
						kind = *backendRef.Kind
					}
					if kind != gatewayv1.Kind("Service") {
						continue
					}

					// Check namespace - default to HTTPRoute's namespace if not specified
					backendNamespace := controllerNamespace
					if backendRef.Namespace != nil {
						backendNamespace = string(*backendRef.Namespace)
					}

					// Only check if backend is in the same namespace as the service
					if backendNamespace == controllerNamespace && backendRef.Name == gatewayv1.ObjectName("rollout-dashboard") {
						// Found HTTPRoute pointing to rollout-dashboard
						// Get hostname from HTTPRoute spec, not from Gateway (which may have wildcards)
						if len(httpRoute.Spec.Hostnames) == 0 {
							continue
						}
						hostname := string(httpRoute.Spec.Hostnames[0])

						// Get the Gateway to determine protocol (HTTP vs HTTPS)
						if len(httpRoute.Spec.ParentRefs) == 0 {
							continue
						}

						parentRef := httpRoute.Spec.ParentRefs[0]
						gatewayName := string(parentRef.Name)
						gatewayNamespace := controllerNamespace
						if parentRef.Namespace != nil {
							gatewayNamespace = string(*parentRef.Namespace)
						}

						gateway := &gatewayv1.Gateway{}
						if err := r.Get(ctx, types.NamespacedName{
							Name:      gatewayName,
							Namespace: gatewayNamespace,
						}, gateway); err != nil {
							continue
						}

					// Find the listener to determine protocol
					// If parentRef specifies a SectionName, use that specific listener
					// Otherwise, match listener by hostname
					var targetListener *gatewayv1.Listener
					if parentRef.SectionName != nil {
						// Match by listener name
						listenerName := string(*parentRef.SectionName)
						for i := range gateway.Spec.Listeners {
							if string(gateway.Spec.Listeners[i].Name) == listenerName {
								targetListener = &gateway.Spec.Listeners[i]
								break
							}
						}
					} else {
						// Match listener by hostname
						for i := range gateway.Spec.Listeners {
							if gateway.Spec.Listeners[i].Hostname != nil {
								// Check if listener hostname matches or is a wildcard that covers our hostname
								listenerHostname := string(*gateway.Spec.Listeners[i].Hostname)
								if listenerHostname == hostname || listenerHostname == "*" ||
									(strings.HasPrefix(listenerHostname, "*.") && strings.HasSuffix(hostname, "."+listenerHostname[2:])) {
									targetListener = &gateway.Spec.Listeners[i]
									break
								}
							}
						}
						// If no match found, use first listener as fallback
						if targetListener == nil && len(gateway.Spec.Listeners) > 0 {
							targetListener = &gateway.Spec.Listeners[0]
						}
					}

						if targetListener != nil {
							scheme := "https"
							if targetListener.Protocol == gatewayv1.HTTPProtocolType {
								scheme = "http"
							}
							baseURL = fmt.Sprintf("%s://%s", scheme, hostname)
						}

						if baseURL != "" {
							break
						}
					}
				}
				if baseURL != "" {
					break
				}
			}
			if baseURL != "" {
				break
			}
		}
	}

	if baseURL == "" {
		return ""
	}

	// Append the path: /rollouts/<deploymentNamespace>/<deploymentName>
	path := fmt.Sprintf("/rollouts/%s/%s", deploymentNamespace, deploymentName)
	return fmt.Sprintf("%s%s", baseURL, path)
}

// extractDeploymentPayload extracts the deployment payload from a GitHub deployment
func (r *GitHubEnvironmentReconciler) extractDeploymentPayload(dep *github.Deployment) *deploymentPayload {
	if len(dep.Payload) == 0 {
		return nil
	}

	// Try to decode payload - GitHub API may return it as base64-encoded string or JSON object
	var payloadBytes []byte

	// First try as base64-encoded string
	var payloadStr string
	if err := json.Unmarshal(dep.Payload, &payloadStr); err == nil {
		// Decode base64
		if decoded, err := base64.StdEncoding.DecodeString(payloadStr); err == nil {
			payloadBytes = decoded
		} else {
			// Not base64, use string as-is
			payloadBytes = []byte(payloadStr)
		}
	} else {
		// Not a string, use payload directly
		payloadBytes = dep.Payload
	}

	// Skip empty objects
	if string(payloadBytes) == "{}" || string(payloadBytes) == "null" {
		return nil
	}

	var payload deploymentPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil
	}
	// Ensure payload has required fields
	if payload.ID == "" || payload.Version == nil {
		return nil
	}

	return &payload
}

// extractDeploymentKey extracts the deployment key from a GitHub deployment's payload and environment
func (r *GitHubEnvironmentReconciler) extractDeploymentKey(dep *github.Deployment) *deploymentKey {
	if dep.Environment == nil {
		return nil
	}

	payload := r.extractDeploymentPayload(dep)
	if payload == nil {
		return nil
	}

	// Get ID from payload
	return &deploymentKey{
		ID:          payload.ID,
		Environment: *dep.Environment,
	}
}

// syncDeploymentHistory ensures a GitHub deployment exists for each rollout history entry
// and posts a DeploymentStatus matching each entry's bake status. It returns a map of version -> deployment info
// for versions currently in history.
func (r *GitHubEnvironmentReconciler) syncDeploymentHistory(ctx context.Context, gh *github.Client, environment *kuberikv1alpha1.Environment, rollout *kuberikrolloutv1alpha1.Rollout) error {
	owner, repo, err := parseProject(environment.Spec.Backend.Project)
	if err != nil {
		return err
	}

	// Ensure deployment name has kuberik prefix for GitHub
	deploymentName := ensureKuberikPrefix(environment.Spec.Name)
	formattedEnv := formatDeploymentEnvironment(deploymentName, environment.Spec.Environment)
	task := formatDeploymentTask(deploymentName)

	// Map deployments by ID + environment from payload
	// We'll query deployments per history entry using ref + environment for more targeted queries
	keyToDeployment := make(map[deploymentKey]*github.Deployment)

	// Iterate history from oldest to newest so states evolve in order
	for i := len(rollout.Status.History) - 1; i >= 0; i-- {
		h := rollout.Status.History[i]
		if h.Version.Revision == nil || *h.Version.Revision == "" {
			continue
		}
		ref := *h.Version.Revision

		// Skip entries without an ID as we need it for unique mapping
		if h.ID == nil {
			continue
		}
		historyID := fmt.Sprintf("%d", *h.ID)

		// Create key for this history entry using ID + formatted environment
		key := deploymentKey{
			ID:          historyID,
			Environment: formattedEnv,
		}

		// Check if we already found this deployment in a previous iteration
		dep := keyToDeployment[key]

		// If not found, query deployments for this specific ref + environment + task
		// Using task (deployment name) helps differentiate different service deployments
		if dep == nil {
			deployments, _, err := gh.Repositories.ListDeployments(ctx, owner, repo, &github.DeploymentsListOptions{
				Ref:         ref,
				Task:        task,
				Environment: formattedEnv,
			})
			if err != nil {
				return fmt.Errorf("failed to list deployments for ref %s: %w", ref, err)
			}

			// Check all deployments to find one with our ID
			for _, d := range deployments {
				if d.ID == nil {
					continue
				}

				// Try to extract key from list response first (faster)
				existingKey := r.extractDeploymentKey(d)
				if existingKey != nil && existingKey.ID == historyID {
					dep = d
					keyToDeployment[key] = dep
					break
				}

				// If payload extraction failed, fetch individually to get full payload
				fullDeployment, _, err := gh.Repositories.GetDeployment(ctx, owner, repo, *d.ID)
				if err != nil {
					continue
				}
				existingKey = r.extractDeploymentKey(fullDeployment)
				if existingKey != nil && existingKey.ID == historyID {
					dep = fullDeployment
					keyToDeployment[key] = dep
					break
				}
			}
		}

		if dep == nil {
			// Create missing deployment for this history entry
			// Only store immutable data in payload: ID, relationship, and version
			// Status and other mutable fields come from GitHub deployment statuses
			payload := deploymentPayload{
				ID:           historyID,
				Relationship: environment.Spec.Relationship,
				Version:      h.Version.DeepCopy(),
			}
			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed to marshal deployment payload: %w", err)
			}

			req := &github.DeploymentRequest{
				Ref:                   &ref,
				Task:                  &task,
				Environment:           &formattedEnv,
				Description:           h.Message,
				ProductionEnvironment: github.Bool(environment.Spec.Environment == "production"),
				AutoMerge:             github.Bool(false),
				Payload:               payloadJSON,
				RequiredContexts:      k8sptr.To([]string{}),
			}
			created, _, err := gh.Repositories.CreateDeployment(ctx, owner, repo, req)
			if err != nil {
				return fmt.Errorf("failed to create GitHub deployment: %w", err)
			}
			dep = created
			keyToDeployment[key] = dep
		}

		// Determine desired GH status for this history entry
		ghState, statusPrefix := mapBakeToGitHubState(h.BakeStatus)
		// Format description with status prefix: "Status: message"
		// Use the rollout's BakeStatusMessage if available, otherwise use just the status prefix
		var message string
		if h.BakeStatusMessage != nil && *h.BakeStatusMessage != "" {
			message = fmt.Sprintf("%s: %s", statusPrefix, *h.BakeStatusMessage)
		} else {
			message = statusPrefix
		}
		// Truncate to 140 chars if needed (GitHub limit)
		ghDesc := message
		if len(ghDesc) > 140 {
			ghDesc = ghDesc[:140]
		}

		// Check all statuses to see if we've already created this exact status
		// This prevents duplicate statuses like: pending -> success -> pending
		statuses, _, err := gh.Repositories.ListDeploymentStatuses(ctx, owner, repo, dep.GetID(), &github.ListOptions{})
		statusExists := false
		if err == nil && len(statuses) > 0 {
			// Statuses are ordered newest first, so the first one is the latest
			for _, status := range statuses {
				if status.State != nil && *status.State == ghState && status.Description != nil && *status.Description == ghDesc {
					// This exact status already exists, skip creating it again
					statusExists = true
					break
				}
			}
		}

		if !statusExists {
			if err := r.createDeploymentStatus(ctx, gh, environment, dep.GetID(), ghState, ghDesc); err != nil {
				return err
			}
		}
	}

	return nil
}

// mapBakeToGitHubState maps bake status/message to GitHub DeploymentStatus state and description prefix
// Returns the GitHub state and a user-friendly status prefix for the description
func mapBakeToGitHubState(bakeStatus *string) (string, string) {
	var status string
	if bakeStatus != nil {
		status = *bakeStatus
	}

	var ghState string
	var statusPrefix string
	switch status {
	case kuberikrolloutv1alpha1.BakeStatusSucceeded:
		ghState = "success"
		statusPrefix = "Bake succeeded"
	case kuberikrolloutv1alpha1.BakeStatusFailed:
		ghState = "failure"
		statusPrefix = "Bake failed"
	case kuberikrolloutv1alpha1.BakeStatusInProgress:
		ghState = "in_progress"
		statusPrefix = "Baking in progress"
	case kuberikrolloutv1alpha1.BakeStatusDeploying:
		ghState = "in_progress"
		statusPrefix = "Deploying"
	case kuberikrolloutv1alpha1.BakeStatusCancelled:
		ghState = "inactive"
		statusPrefix = "Bake cancelled"
	default:
		ghState = "pending"
		statusPrefix = "Deploying"
	}

	return ghState, statusPrefix
}

// extractBakeStatusFromDescription extracts bake status from GitHub status description
// The description format is: "Status: message" or just "Status"
// It parses the status prefix to reliably determine the bake status
func extractBakeStatusFromDescription(description string) (*string, *string) {
	if description == "" {
		return nil, nil
	}

	// Parse the description format: "Status: message" or just "Status"
	// Split by ":" to separate status prefix from message
	parts := strings.SplitN(description, ":", 2)
	statusPrefix := strings.TrimSpace(parts[0])

	var bakeStatus *string
	var message *string

	// Determine bake status from the status prefix
	switch statusPrefix {
	case "Bake succeeded":
		status := kuberikrolloutv1alpha1.BakeStatusSucceeded
		bakeStatus = &status
	case "Bake failed":
		status := kuberikrolloutv1alpha1.BakeStatusFailed
		bakeStatus = &status
	case "Baking in progress":
		status := kuberikrolloutv1alpha1.BakeStatusInProgress
		bakeStatus = &status
	case "Bake cancelled":
		status := kuberikrolloutv1alpha1.BakeStatusCancelled
		bakeStatus = &status
	case "Deploying":
		status := kuberikrolloutv1alpha1.BakeStatusDeploying
		bakeStatus = &status
	default:
		// If we don't recognize the prefix, return nil for status
		// This handles cases where GitHub sets automatic statuses
		bakeStatus = nil
	}

	// Extract message if present (part after ":")
	if len(parts) >= 2 {
		msg := strings.TrimSpace(parts[1])
		if msg != "" {
			// Truncate to 140 chars if needed
			if len(msg) > 140 {
				msg = msg[:140]
			}
			message = &msg
		}
	}

	return bakeStatus, message
}

// applyGitHubStatusToEntry applies GitHub deployment status to a DeploymentHistoryEntry
// It extracts bake status by inferring from the GitHub state and description
// It skips "inactive" statuses that don't appear to be from our controller (no meaningful description)
// since inactive statuses are often set by GitHub automatically
func applyGitHubStatusToEntry(entry *kuberikrolloutv1alpha1.DeploymentHistoryEntry, statuses []*github.DeploymentStatus) {
	if len(statuses) == 0 {
		return
	}
	// Statuses are ordered newest first
	// Look for the most recent status that appears to be from our controller
	// Skip "inactive" statuses that don't have meaningful descriptions, as they're often set automatically by GitHub
	for _, status := range statuses {
		if status.Description == nil || *status.Description == "" {
			continue
		}
		// Skip inactive statuses that don't appear to be from our controller
		// These are often set automatically by GitHub and don't contain our bake status information
		if status.State != nil && *status.State == "inactive" {
			// Only consider inactive statuses that have our status prefix
			if !strings.HasPrefix(*status.Description, "Bake cancelled") {
				continue
			}
		}

		// Extract bake status and message from description
		bakeStatus, message := extractBakeStatusFromDescription(*status.Description)
		if bakeStatus != nil {
			entry.BakeStatus = bakeStatus
			if message != nil {
				entry.BakeStatusMessage = message
			}
			return // Found a valid status, stop looking
		}
	}
}

// buildHistoryFromDeployments builds history entries from GitHub deployments
// If rolloutHistoryOrder is provided, process deployments in that order to ensure all entries get their statuses
func (r *GitHubEnvironmentReconciler) buildHistoryFromDeployments(ctx context.Context, ghClient *github.Client, owner, repo string, deployments []*github.Deployment, rolloutHistoryIDs map[int64]bool, rolloutHistoryOrder []int64, envDeploymentStatuses map[string]map[string]string, envName string) []kuberikrolloutv1alpha1.DeploymentHistoryEntry {
	historyEntries := make([]kuberikrolloutv1alpha1.DeploymentHistoryEntry, 0)
	seenIDs := make(map[int64]bool) // Avoid duplicates

	// Build a map of deployment ID -> deployment for quick lookup
	deploymentMap := make(map[int64]*github.Deployment)
	for _, d := range deployments {
		payload := r.extractDeploymentPayload(d)
		if payload == nil || payload.ID == "" || payload.Version == nil || payload.Version.Revision == nil || *payload.Version.Revision == "" {
			continue
		}
		entryID, err := strconv.ParseInt(payload.ID, 10, 64)
		if err != nil {
			continue
		}
		if len(rolloutHistoryIDs) > 0 && !rolloutHistoryIDs[entryID] {
			continue
		}
		deploymentMap[entryID] = d
	}

	// Process deployments in rollout history order if provided, otherwise process all from map
	processOrder := rolloutHistoryOrder
	if len(processOrder) == 0 {
		// If no order specified, process all deployments from the map
		processOrder = make([]int64, 0, len(deploymentMap))
		for entryID := range deploymentMap {
			processOrder = append(processOrder, entryID)
		}
	}

	for _, entryID := range processOrder {
		d, exists := deploymentMap[entryID]
		if !exists || seenIDs[entryID] {
			continue
		}
		// Process this deployment
		if entry := r.processDeploymentForHistory(ctx, ghClient, owner, repo, d, entryID, envDeploymentStatuses, envName); entry != nil {
			historyEntries = append(historyEntries, *entry)
			seenIDs[entryID] = true
		}
	}

	return historyEntries
}

// processDeploymentForHistory processes a single deployment and returns a DeploymentHistoryEntry
// It fetches the deployment status from GitHub and applies it to the entry
func (r *GitHubEnvironmentReconciler) processDeploymentForHistory(ctx context.Context, ghClient *github.Client, owner, repo string, d *github.Deployment, entryID int64, envDeploymentStatuses map[string]map[string]string, envName string) *kuberikrolloutv1alpha1.DeploymentHistoryEntry {
	payload := r.extractDeploymentPayload(d)
	if payload == nil || payload.Version == nil || payload.Version.Revision == nil || *payload.Version.Revision == "" {
		return nil
	}

	// Build DeploymentHistoryEntry from payload (immutable data) + GitHub status (mutable data)
	entry := kuberikrolloutv1alpha1.DeploymentHistoryEntry{
		ID:        &entryID,
		Version:   *payload.Version.DeepCopy(),
		Timestamp: metav1.Now(), // Default to now, could be improved with deployment.created_at
	}

	// Get the latest deployment status from GitHub to get bake status
	// This is the source of truth for deployment status
	var statuses []*github.DeploymentStatus
	if d.ID != nil {
		var err error
		statuses, err = r.fetchAllDeploymentStatuses(ctx, ghClient, owner, repo, d.GetID())
		if err != nil {
			// Log error but continue - we'll add entry without status if fetch fails
			// This ensures we don't skip deployments due to transient errors
			log.FromContext(ctx).Error(err, "Failed to list deployment statuses for deployment", "deploymentID", *d.ID)
		}
	}

	// Always apply statuses if we have them
	if len(statuses) > 0 {
		applyGitHubStatusToEntry(&entry, statuses)
		// Also track GitHub deployment status for this version (for environments without rollouts)
		if envDeploymentStatuses != nil && d.Ref != nil && *d.Ref != "" && statuses[0].State != nil {
			if envDeploymentStatuses[envName] == nil {
				envDeploymentStatuses[envName] = make(map[string]string)
			}
			envDeploymentStatuses[envName][*d.Ref] = *statuses[0].State
		}
	}
	// Note: Even if status fetch fails or returns empty, we still add the entry (without bakeStatus)

	return &entry
}

// fetchAllDeploymentStatuses fetches all deployment statuses with pagination
// entryID and envName are optional parameters used for logging errors
func (r *GitHubEnvironmentReconciler) fetchAllDeploymentStatuses(ctx context.Context, ghClient *github.Client, owner, repo string, deploymentID int64) ([]*github.DeploymentStatus, error) {
	allStatuses := make([]*github.DeploymentStatus, 0)
	statusOpts := &github.ListOptions{PerPage: 100}
	for {
		pageStatuses, resp, err := ghClient.Repositories.ListDeploymentStatuses(ctx, owner, repo, deploymentID, statusOpts)
		if err != nil {
			return allStatuses, err
		}
		allStatuses = append(allStatuses, pageStatuses...)
		if resp.NextPage == 0 {
			break
		}
		statusOpts.Page = resp.NextPage
	}
	return allStatuses, nil
}

// applyRolloutGateDesiredState applies the desired state to a RolloutGate based on the Deployment spec.
// This function sets all fields that should be managed by this controller, whether creating or updating.
func (r *GitHubEnvironmentReconciler) applyRolloutGateDesiredState(rolloutGate *kuberikrolloutv1alpha1.RolloutGate, deployment *kuberikv1alpha1.Environment) error {
	// Set annotations
	prettyName := "Relationship not ready yet"
	var description string
	if deployment.Spec.Relationship != nil {
		relType := "deployed"
		switch deployment.Spec.Relationship.Type {
		case kuberikv1alpha1.RelationshipTypeAfter:
			relType = "deployed after"
		case kuberikv1alpha1.RelationshipTypeParallel:
			relType = "deployed in parallel with"
		}
		description = fmt.Sprintf("This gate is passing only for those versions that have been successfully %s the %s environment.", relType, deployment.Spec.Relationship.Environment)
	} else {
		description = "This gate is passing only for those versions that have been successfully deployed to the related environment."
	}

	if rolloutGate.Annotations == nil {
		rolloutGate.Annotations = make(map[string]string)
	}
	rolloutGate.Annotations["gate.kuberik.com/pretty-name"] = prettyName
	rolloutGate.Annotations["gate.kuberik.com/description"] = description

	// Set spec
	rolloutGate.Spec.RolloutRef = &deployment.Spec.RolloutRef

	// When a relationship is configured, initialize AllowedVersions to an empty
	// slice (not nil) so the gate blocks all versions by default until
	// updateAllowedVersionsFromRelationships populates it. A nil AllowedVersions
	// is treated by the rollout controller as "no restriction", which would allow
	// deployments to race through before the gate is fully set up.
	if deployment.Spec.Relationship != nil && rolloutGate.Spec.AllowedVersions == nil {
		empty := []string{}
		rolloutGate.Spec.AllowedVersions = &empty
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(deployment, rolloutGate, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference: %w", err)
	}

	return nil
}

// createOrUpdateRolloutGate creates or updates the RolloutGate based on the Environment spec
func (r *GitHubEnvironmentReconciler) createOrUpdateRolloutGate(ctx context.Context, environment *kuberikv1alpha1.Environment) error {
	// List all RolloutGates in the namespace to find one owned by this Environment
	rolloutGateList := &kuberikrolloutv1alpha1.RolloutGateList{}
	if err := r.List(ctx, rolloutGateList, client.InNamespace(environment.Namespace)); err != nil {
		return fmt.Errorf("failed to list RolloutGates: %w", err)
	}

	// Find existing RolloutGate owned by this Environment
	var existingRolloutGate *kuberikrolloutv1alpha1.RolloutGate
	for i := range rolloutGateList.Items {
		gate := &rolloutGateList.Items[i]
		for _, ownerRef := range gate.OwnerReferences {
			if ownerRef.Kind == "Environment" &&
				ownerRef.APIVersion == kuberikv1alpha1.GroupVersion.String() &&
				ownerRef.Name == environment.Name {
				// If UID is set, it must match; otherwise match by name and kind only
				if ownerRef.UID == "" || ownerRef.UID == environment.UID {
					existingRolloutGate = gate
					break
				}
			}
		}
		if existingRolloutGate != nil {
			break
		}
	}

	if existingRolloutGate == nil {
		// RolloutGate doesn't exist, create it
		rolloutGate := &kuberikrolloutv1alpha1.RolloutGate{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "ghd-",
				Namespace:    environment.Namespace,
			},
		}

		// Apply desired state
		if err := r.applyRolloutGateDesiredState(rolloutGate, environment); err != nil {
			return err
		}

		if err := r.Create(ctx, rolloutGate); err != nil {
			return fmt.Errorf("failed to create RolloutGate: %w", err)
		}
		existingRolloutGate = rolloutGate
	} else {
		// RolloutGate exists, apply desired state and update if needed
		// Store original state for comparison
		originalAnnotations := make(map[string]string)
		if existingRolloutGate.Annotations != nil {
			for k, v := range existingRolloutGate.Annotations {
				originalAnnotations[k] = v
			}
		}
		var originalRolloutRef *corev1.LocalObjectReference
		if existingRolloutGate.Spec.RolloutRef != nil {
			originalRolloutRef = &corev1.LocalObjectReference{
				Name: existingRolloutGate.Spec.RolloutRef.Name,
			}
		}
		originalOwnerRefs := make([]metav1.OwnerReference, len(existingRolloutGate.OwnerReferences))
		copy(originalOwnerRefs, existingRolloutGate.OwnerReferences)

		// Apply desired state
		if err := r.applyRolloutGateDesiredState(existingRolloutGate, environment); err != nil {
			return err
		}

		// Check if update is needed by comparing current state with original
		needsUpdate := false

		// Check annotations
		if len(originalAnnotations) != len(existingRolloutGate.Annotations) {
			needsUpdate = true
		} else {
			for k, v := range existingRolloutGate.Annotations {
				if originalAnnotations[k] != v {
					needsUpdate = true
					break
				}
			}
		}

		// Check RolloutRef
		if originalRolloutRef == nil || existingRolloutGate.Spec.RolloutRef == nil {
			if originalRolloutRef != existingRolloutGate.Spec.RolloutRef {
				needsUpdate = true
			}
		} else if originalRolloutRef.Name != existingRolloutGate.Spec.RolloutRef.Name {
			needsUpdate = true
		}

		// Check owner references (SetControllerReference may have modified them)
		if len(originalOwnerRefs) != len(existingRolloutGate.OwnerReferences) {
			needsUpdate = true
		} else {
			for i, ref := range existingRolloutGate.OwnerReferences {
				if i >= len(originalOwnerRefs) || ref.UID != originalOwnerRefs[i].UID {
					needsUpdate = true
					break
				}
			}
		}

		if needsUpdate {
			if err := r.Update(ctx, existingRolloutGate); err != nil {
				return fmt.Errorf("failed to update RolloutGate: %w", err)
			}
		}
	}

	// Update the RolloutGateRef in Environment status
	rolloutGateRef := &corev1.LocalObjectReference{Name: existingRolloutGate.Name}
	if environment.Status.RolloutGateRef == nil || environment.Status.RolloutGateRef.Name != rolloutGateRef.Name {
		environment.Status.RolloutGateRef = rolloutGateRef
		return r.Status().Update(ctx, environment)
	}

	return nil
}

// environmentInfo holds information about an environment's deployment
type environmentInfo struct {
	EnvironmentURL string
	Relationship   *kuberikv1alpha1.EnvironmentRelationship
}

// relationshipGraphData holds the data needed for relationship-based operations
type relationshipGraphData struct {
	relevantEnvironments map[string]bool
	relevantVersions     map[string]bool
	envDeployments       map[string][]*github.Deployment
	// envHistory maps environment name -> history entries from that environment's rollout
	envHistory map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry
	// envDeploymentStatuses maps environment name -> version -> GitHub deployment status ("success", "failure", etc.)
	envDeploymentStatuses map[string]map[string]string
	environmentInfos      map[string]environmentInfo
}

// buildRelationshipGraph discovers all environments and builds the relationship graph
func (r *GitHubEnvironmentReconciler) buildRelationshipGraph(ctx context.Context, environment *kuberikv1alpha1.Environment, ghClient *github.Client) (*relationshipGraphData, error) {
	owner, repo, err := parseProject(environment.Spec.Backend.Project)
	if err != nil {
		return nil, err
	}

	// Ensure deployment name has kuberik prefix for GitHub
	deploymentName := ensureKuberikPrefix(environment.Spec.Name)

	// Build relationship graph to determine relevant environments
	// Relevant environments are: current environment + all environments related to it (directly or transitively)
	relevantEnvironments := make(map[string]bool)
	currentEnv := environment.Spec.Environment
	relevantEnvironments[currentEnv] = true

	// Build a map of environment -> relationships from all discovered environments
	envRelationships := make(map[string]*kuberikv1alpha1.EnvironmentRelationship)

	// Query all deployments with the same task to discover all environments
	task := formatDeploymentTask(deploymentName)
	allDeployments, _, err := ghClient.Repositories.ListDeployments(ctx, owner, repo, &github.DeploymentsListOptions{
		Task: task,
		ListOptions: github.ListOptions{
			PerPage: 100,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list all deployments: %w", err)
	}

	// Group deployments by environment and extract environment names
	envDeployments := make(map[string][]*github.Deployment) // environment -> deployments
	for _, d := range allDeployments {
		if d.Environment == nil {
			continue
		}
		// Extract environment name from formatted environment (e.g., "kuberik-deployment/environment" -> "environment")
		env := *d.Environment
		// The environment format is "deploymentName/environment", so split and take the last part
		envParts := strings.Split(env, "/")
		if len(envParts) > 0 {
			envName := envParts[len(envParts)-1]
			envDeployments[envName] = append(envDeployments[envName], d)

			// Extract relationship from payload
			payload := r.extractDeploymentPayload(d)
			if payload != nil && payload.Relationship != nil {
				envRelationships[envName] = payload.Relationship
			}
		}
	}

	// Also discover environments from Environment resources (not just GitHub deployments)
	// This ensures we find related environments even if they don't have GitHub deployments yet
	environmentList := &kuberikv1alpha1.EnvironmentList{}
	if err := r.List(ctx, environmentList, client.InNamespace(environment.Namespace)); err == nil {
		for i := range environmentList.Items {
			env := &environmentList.Items[i]
			// Match by deployment name
			if env.Spec.Name != environment.Spec.Name {
				continue
			}
			if env.Spec.Environment == "" {
				continue
			}
			envName := env.Spec.Environment

			// Add to discovered environments
			if envDeployments[envName] == nil {
				envDeployments[envName] = []*github.Deployment{}
			}

			// Extract relationship from Environment spec
			if env.Spec.Relationship != nil {
				envRelationships[envName] = env.Spec.Relationship
			}
		}
	}

	// Build set of relevant environments based on relationships
	// Start with current environment and traverse relationships
	visited := make(map[string]bool)
	var traverseRelationships func(env string)
	traverseRelationships = func(env string) {
		if visited[env] {
			return
		}
		visited[env] = true
		relevantEnvironments[env] = true

		// Find all environments that have a relationship pointing to this environment
		for otherEnv, rel := range envRelationships {
			if rel != nil && rel.Environment == env {
				traverseRelationships(otherEnv)
			}
		}

		// Also traverse environments that this environment relates to
		if rel, exists := envRelationships[env]; exists && rel != nil {
			traverseRelationships(rel.Environment)
		}
	}
	traverseRelationships(currentEnv)

	// Track environment info (environment URL and relationship) for all environments
	environmentInfos := make(map[string]environmentInfo)

	// First, update environment infos from GitHub deployments
	for envName, deployments := range envDeployments {
		if len(deployments) == 0 {
			continue
		}

		// Track the latest environment URL and relationship (first deployment in list is latest)
		latestDeployment := deployments[0]
		envInfo := environmentInfo{}

		// Extract relationship from the latest deployment's payload
		payload := r.extractDeploymentPayload(latestDeployment)
		if payload != nil && payload.Relationship != nil {
			envInfo.Relationship = payload.Relationship
		}

		// Extract environment URL from the latest deployment's statuses
		statuses, err := r.fetchAllDeploymentStatuses(ctx, ghClient, owner, repo, latestDeployment.GetID())
		if err == nil && len(statuses) > 0 {
			// Statuses are ordered newest first, so the first one has the latest environment URL
			if statuses[0].EnvironmentURL != nil && *statuses[0].EnvironmentURL != "" {
				envInfo.EnvironmentURL = *statuses[0].EnvironmentURL
			}
		}

		environmentInfos[envName] = envInfo
	}

	// Also add environment infos for environments discovered from Environment resources (without GitHub deployments)
	// Reuse environmentList from above
	for i := range environmentList.Items {
		env := &environmentList.Items[i]
		if env.Spec.Name != environment.Spec.Name || env.Spec.Environment == "" {
			continue
		}
		envName := env.Spec.Environment

		// Only add if not already set from GitHub deployments and if it's a relevant environment
		existingInfo, exists := environmentInfos[envName]
		if !exists && relevantEnvironments[envName] {
			envInfo := environmentInfo{}
			if env.Spec.Relationship != nil {
				envInfo.Relationship = env.Spec.Relationship
			}
			environmentInfos[envName] = envInfo
		} else if exists && existingInfo.Relationship == nil && env.Spec.Relationship != nil {
			// Update relationship from Environment spec if not set from GitHub
			existingInfo.Relationship = env.Spec.Relationship
			environmentInfos[envName] = existingInfo
		}
	}

	// Now fetch history from GitHub deployments for all relevant environments (current and related)
	// We use GitHub deployments as the source of truth. If an environment doesn't have
	// deployments in GitHub, we won't show it in status unless it's the current environment with rollout history

	envHistory := make(map[string][]kuberikrolloutv1alpha1.DeploymentHistoryEntry)

	for envName := range environmentInfos {
		var history []kuberikrolloutv1alpha1.DeploymentHistoryEntry

		// Rely on ListDeployments results which are refreshed above
		if deployments, ok := envDeployments[envName]; ok {
			// We need to resolve rollout history IDs for other environments too if possible,
			// but here we just rely on what's in the deployment payload
			history = r.buildHistoryFromDeployments(ctx, ghClient, owner, repo, deployments, nil, nil, nil, envName)
		}

		if len(history) > 0 {
			sortHistoryEntriesDescending(history)
			envHistory[envName] = history
		}
	}
	environmentList = &kuberikv1alpha1.EnvironmentList{}
	if err := r.List(ctx, environmentList, client.InNamespace(environment.Namespace)); err == nil {
		for i := range environmentList.Items {
			env := &environmentList.Items[i]
			// Check if this environment matches one of our discovered environments
			if env.Spec.Environment == "" || env.Spec.Name != environment.Spec.Name {
				continue
			}
			envName := env.Spec.Environment
			if !relevantEnvironments[envName] {
				continue
			}

			// Always sync this environment's history to ensure GitHub deployments and statuses
			// are up to date with the latest rollout status. This ensures we get the latest bake status.
			// This applies to both current and related environments - all are synced the same way.
			// Get the rollout for this environment
			rollout, err := r.getReferencedRollout(ctx, env)
			if err != nil {
				continue
			}

			// Sync deployment history to ensure GitHub deployments and statuses match rollout
			// This updates GitHub deployment statuses when rollout bake status changes
			err = r.syncDeploymentHistory(ctx, ghClient, env, rollout)
			if err != nil {
				continue
			}

			// Re-fetch deployments after syncing to get the latest status
			// Fetch all pages to ensure we get all deployments (not just the first page)
			formattedEnv := formatDeploymentEnvironment(deploymentName, envName)
			task := formatDeploymentTask(deploymentName)
			allDeploymentsForEnv := make([]*github.Deployment, 0)
			opts := &github.DeploymentsListOptions{
				Task:        task,
				Environment: formattedEnv,
				ListOptions: github.ListOptions{PerPage: 100}, // Fetch up to 100 per page
			}
			for {
				deployments, resp, err := ghClient.Repositories.ListDeployments(ctx, owner, repo, opts)
				if err != nil {
					break
				}
				allDeploymentsForEnv = append(allDeploymentsForEnv, deployments...)
				if resp.NextPage == 0 {
					break
				}
				opts.Page = resp.NextPage
			}
			envDeployments[envName] = allDeploymentsForEnv

			if len(allDeploymentsForEnv) == 0 {
				// No deployments after sync - skip this environment
				continue
			}

			// Extract history from GitHub deployment payloads and get latest status
			// For environments with Environment resources, only include entries that are in the rollout's current history
			// Get rollout history IDs to filter (only for environments with Environment resources)
			// This ensures we only include history entries that are in the rollout's current history
			rolloutHistoryIDs := make(map[int64]bool)
			rolloutHistoryOrder := make([]int64, 0) // Track order of history IDs
			if rollout != nil {
				for _, h := range rollout.Status.History {
					if h.ID != nil {
						rolloutHistoryIDs[*h.ID] = true
						rolloutHistoryOrder = append(rolloutHistoryOrder, *h.ID)
					}
				}
			}

			historyEntries := r.buildHistoryFromDeployments(ctx, ghClient, owner, repo, allDeploymentsForEnv, rolloutHistoryIDs, rolloutHistoryOrder, nil, envName)
			if len(historyEntries) > 0 {
				envHistory[envName] = historyEntries
			}
		}
	}

	// Track deployment statuses from GitHub for environments without Environment resources
	envDeploymentStatuses := make(map[string]map[string]string) // environment -> version -> status

	// For environments without Environment resources, extract history from GitHub deployment payloads
	// Also process environments that have GitHub deployments but weren't processed above
	for envName, deployments := range envDeployments {
		// Skip if we already have history from Environment resource
		if _, exists := envHistory[envName]; exists {
			continue
		}
		// Only process relevant environments
		if !relevantEnvironments[envName] {
			continue
		}

		// Skip if no deployments
		if len(deployments) == 0 {
			continue
		}

		// Collect history entries from deployment payloads + GitHub statuses
		// For environments without Environment resources, don't filter by rollout history IDs
		historyEntries := r.buildHistoryFromDeployments(ctx, ghClient, owner, repo, deployments, nil, nil, envDeploymentStatuses, envName)
		if len(historyEntries) > 0 {
			envHistory[envName] = historyEntries
		}
	}

	// Build set of relevant versions: versions that appear in any relevant environment's history
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

	return &relationshipGraphData{
		relevantEnvironments:  relevantEnvironments,
		relevantVersions:      relevantVersions,
		envDeployments:        envDeployments,
		envHistory:            envHistory,
		envDeploymentStatuses: envDeploymentStatuses,
		environmentInfos:      environmentInfos,
	}, nil
}

// updateAllowedVersionsFromRelationships checks environment relationships and updates allowed versions on RolloutGate
// Version relevance is determined by relationships: we track versions that are relevant to the current environment
// based on "After" and "Parallel" relationships
func (r *GitHubEnvironmentReconciler) updateAllowedVersionsFromRelationships(ctx context.Context, environment *kuberikv1alpha1.Environment) error {
	// Get the referenced Rollout to access releaseCandidates
	rollout, err := r.getReferencedRollout(ctx, environment)
	if err != nil {
		return fmt.Errorf("failed to get referenced Rollout: %w", err)
	}

	// Build a map of revision -> tag from releaseCandidates
	revisionToTag := make(map[string]string)
	for _, candidate := range rollout.Status.ReleaseCandidates {
		if candidate.Revision != nil && *candidate.Revision != "" && candidate.Tag != "" {
			revisionToTag[*candidate.Revision] = candidate.Tag
		}
	}

	// Initialize as empty slice (not nil) when relationship is set
	// This ensures allowedVersions is set to empty array instead of nil
	allowedTags := []string{}
	seenTags := make(map[string]bool) // Avoid duplicates

	// Find allowed versions from related environment history
	if environment.Spec.Relationship != nil {
		relatedEnvName := environment.Spec.Relationship.Environment

		// Find related environment info in Status
		var relatedEnvInfo *kuberikv1alpha1.EnvironmentInfo
		if environment.Status.EnvironmentInfos != nil {
			for i := range environment.Status.EnvironmentInfos {
				// Use 'info' to access the element without copying if possible, but range returns copy
				if environment.Status.EnvironmentInfos[i].Environment == relatedEnvName {
					relatedEnvInfo = &environment.Status.EnvironmentInfos[i]
					break
				}
			}
		}

		// Check if related environment has history
		if relatedEnvInfo != nil {
			for _, entry := range relatedEnvInfo.History {
				if entry.Version.Revision == nil || *entry.Version.Revision == "" {
					continue
				}
				revision := *entry.Version.Revision

				// Check if bake status is succeeded
				// Since we now have consolidated history in Status with latest bake status (from GitHub),
				// checking the entry's BakeStatus is sufficient.
				bakeSucceeded := entry.BakeStatus != nil && *entry.BakeStatus == kuberikrolloutv1alpha1.BakeStatusSucceeded

				if bakeSucceeded {
					// Match revision to tag in releaseCandidates
					if tag, found := revisionToTag[revision]; found {
						// Only add if not already seen
						if !seenTags[tag] {
							allowedTags = append(allowedTags, tag)
							seenTags[tag] = true
						}
					}
				}
			}
		}
	}

	// Update allowed versions on RolloutGate (only if relationship is set)
	if environment.Spec.Relationship == nil {
		// No relationship, skip RolloutGate update
		return nil
	} else if environment.Status.RolloutGateRef == nil {
		return nil
	} else {
		rolloutGate := &kuberikrolloutv1alpha1.RolloutGate{}
		err = r.Get(ctx, types.NamespacedName{
			Name:      environment.Status.RolloutGateRef.Name,
			Namespace: environment.Namespace,
		}, rolloutGate)
		if err != nil {
			return fmt.Errorf("failed to get RolloutGate: %w", err)
		}

		// Check if allowedVersions need to be updated
		// When dependencies are set, we must ensure allowedVersions is set to empty array instead of nil
		needsUpdate := false
		if rolloutGate.Spec.AllowedVersions == nil {
			// If currently nil and dependencies are set, we need to set it to empty array
			needsUpdate = true
		} else {
			// Compare current values with new values
			currentAllowedVersions := *rolloutGate.Spec.AllowedVersions
			if !r.slicesEqual(currentAllowedVersions, allowedTags) {
				needsUpdate = true
			}
		}

		if needsUpdate {
			rolloutGate.Spec.AllowedVersions = &allowedTags
			if err := r.Update(ctx, rolloutGate); err != nil {
				return fmt.Errorf("failed to update RolloutGate allowedVersions: %w", err)
			}
		}
	}

	return nil
}

// updateEnvironmentInfoWithHistory updates or adds an EnvironmentInfo entry to the list
// It ensures that history entries are sorted descending by ID
func updateEnvironmentInfoWithHistory(infos []kuberikv1alpha1.EnvironmentInfo, environment, environmentURL string, relationship *kuberikv1alpha1.EnvironmentRelationship, history []kuberikrolloutv1alpha1.DeploymentHistoryEntry) []kuberikv1alpha1.EnvironmentInfo {
	var historyCopy []kuberikrolloutv1alpha1.DeploymentHistoryEntry
	if history != nil {
		// Deep copy
		copiedHistory := make([]kuberikrolloutv1alpha1.DeploymentHistoryEntry, len(history))
		for i := range history {
			copiedHistory[i] = *history[i].DeepCopy()
		}

		// Sort descending by ID
		sortHistoryEntriesDescending(copiedHistory)

		// De-duplicate: keep only the first occurrence of each tag/revision
		// Since it's sorted descending by ID, the first occurrence is the latest one
		seen := make(map[string]bool)
		for _, entry := range copiedHistory {
			key := ""
			if entry.Version.Tag != "" {
				key = entry.Version.Tag
			} else if entry.Version.Revision != nil {
				key = *entry.Version.Revision
			}

			if key != "" {
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			historyCopy = append(historyCopy, entry)
		}
	}

	// Find existing entry
	for i := range infos {
		if infos[i].Environment == environment {
			infos[i].EnvironmentURL = environmentURL
			infos[i].Relationship = relationship
			if history != nil {
				infos[i].History = historyCopy
			}
			return infos
		}
	}

	// If not found, add new entry
	infos = append(infos, kuberikv1alpha1.EnvironmentInfo{
		Environment:    environment,
		EnvironmentURL: environmentURL,
		Relationship:   relationship,
		History:        historyCopy,
	})
	return infos
}

// historyEntriesEqual compares two DeploymentHistoryEntry slices for equality
// It compares entries by ID (which uniquely identifies a history entry) and all relevant fields
func historyEntriesEqual(a, b []kuberikrolloutv1alpha1.DeploymentHistoryEntry) bool {
	if len(a) != len(b) {
		return false
	}

	// Create maps for easier comparison: key is ID (or revision if ID is not available)
	aMap := make(map[string]kuberikrolloutv1alpha1.DeploymentHistoryEntry)
	bMap := make(map[string]kuberikrolloutv1alpha1.DeploymentHistoryEntry)

	for _, entry := range a {
		key := ""
		if entry.ID != nil {
			key = fmt.Sprintf("id:%d", *entry.ID)
		} else if entry.Version.Revision != nil {
			key = fmt.Sprintf("rev:%s", *entry.Version.Revision)
		}
		if key != "" {
			aMap[key] = entry
		}
	}

	for _, entry := range b {
		key := ""
		if entry.ID != nil {
			key = fmt.Sprintf("id:%d", *entry.ID)
		} else if entry.Version.Revision != nil {
			key = fmt.Sprintf("rev:%s", *entry.Version.Revision)
		}
		if key != "" {
			bMap[key] = entry
		}
	}

	if len(aMap) != len(bMap) {
		return false
	}

	// Compare entries by key (ID or revision)
	for key, aEntry := range aMap {
		bEntry, exists := bMap[key]
		if !exists {
			return false
		}
		// Compare all relevant fields that can change
		// ID comparison
		if (aEntry.ID == nil) != (bEntry.ID == nil) {
			return false
		}
		if aEntry.ID != nil && bEntry.ID != nil && *aEntry.ID != *bEntry.ID {
			return false
		}
		// Bake status comparison
		if (aEntry.BakeStatus == nil) != (bEntry.BakeStatus == nil) {
			return false
		}
		if aEntry.BakeStatus != nil && bEntry.BakeStatus != nil && *aEntry.BakeStatus != *bEntry.BakeStatus {
			return false
		}
		// Bake status message comparison
		if (aEntry.BakeStatusMessage == nil) != (bEntry.BakeStatusMessage == nil) {
			return false
		}
		if aEntry.BakeStatusMessage != nil && bEntry.BakeStatusMessage != nil && *aEntry.BakeStatusMessage != *bEntry.BakeStatusMessage {
			return false
		}
		// Bake start time comparison
		if (aEntry.BakeStartTime == nil) != (bEntry.BakeStartTime == nil) {
			return false
		}
		if aEntry.BakeStartTime != nil && bEntry.BakeStartTime != nil && !aEntry.BakeStartTime.Equal(bEntry.BakeStartTime) {
			return false
		}
		// Bake end time comparison
		if (aEntry.BakeEndTime == nil) != (bEntry.BakeEndTime == nil) {
			return false
		}
		if aEntry.BakeEndTime != nil && bEntry.BakeEndTime != nil && !aEntry.BakeEndTime.Equal(bEntry.BakeEndTime) {
			return false
		}
	}

	return true
}

// environmentInfosEqual compares two environment info lists for equality
func environmentInfosEqual(a, b []kuberikv1alpha1.EnvironmentInfo) bool {
	if len(a) != len(b) {
		return false
	}
	// Create maps for easier comparison
	aMap := make(map[string]kuberikv1alpha1.EnvironmentInfo)
	bMap := make(map[string]kuberikv1alpha1.EnvironmentInfo)
	for _, entry := range a {
		aMap[entry.Environment] = entry
	}
	for _, entry := range b {
		bMap[entry.Environment] = entry
	}
	if len(aMap) != len(bMap) {
		return false
	}
	for k, aEntry := range aMap {
		bEntry, exists := bMap[k]
		if !exists {
			return false
		}
		if aEntry.EnvironmentURL != bEntry.EnvironmentURL {
			return false
		}
		// Compare relationships
		if (aEntry.Relationship == nil) != (bEntry.Relationship == nil) {
			return false
		}
		if aEntry.Relationship != nil && bEntry.Relationship != nil {
			if aEntry.Relationship.Environment != bEntry.Relationship.Environment {
				return false
			}
			if aEntry.Relationship.Type != bEntry.Relationship.Type {
				return false
			}
		}
		// Compare history
		if !historyEntriesEqual(aEntry.History, bEntry.History) {
			return false
		}
	}
	return true
}

// getMaxHistoryID returns the maximum ID from history entries
func getMaxHistoryID(entries []kuberikrolloutv1alpha1.DeploymentHistoryEntry) int64 {
	var maxID int64 = -1
	for _, entry := range entries {
		if entry.ID != nil && *entry.ID > maxID {
			maxID = *entry.ID
		}
	}
	return maxID
}

// sortHistoryEntriesDescending sorts history entries in descending order by ID
func sortHistoryEntriesDescending(entries []kuberikrolloutv1alpha1.DeploymentHistoryEntry) {
	sort.Slice(entries, func(i, j int) bool {
		idI := getHistoryID(&entries[i])
		idJ := getHistoryID(&entries[j])
		if idI != idJ {
			return idI > idJ
		}
		// Fallback to revision for stable sort
		return getRevision(&entries[i]) > getRevision(&entries[j])
	})
}

// getRevision returns the revision from a history entry or empty string if missing
func getRevision(entry *kuberikrolloutv1alpha1.DeploymentHistoryEntry) string {
	if entry != nil && entry.Version.Revision != nil {
		return *entry.Version.Revision
	}
	return ""
}

// getHistoryID returns the ID from a history entry or -1 if missing
func getHistoryID(entry *kuberikrolloutv1alpha1.DeploymentHistoryEntry) int64 {
	if entry != nil && entry.ID != nil {
		return *entry.ID
	}
	return -1
}

// slicesEqual compares two string slices for equality
func (r *GitHubEnvironmentReconciler) slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
