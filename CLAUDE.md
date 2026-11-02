# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Rules

- **Always use kubebuilder CLI to scaffold controllers.** Never manually create controller files. Use:
  ```bash
  # For controllers with new CRDs:
  kubebuilder create api --group <group> --version <version> --kind <Kind>

  # For controllers watching external CRDs:
  kubebuilder create api \
    --group <group> \
    --version <version> \
    --kind <Kind> \
    --controller=true \
    --resource=false \
    --external-api-domain <domain> \
    --external-api-path <import-path>
  ```
  Then implement your logic in the scaffolded `*_controller.go` file.

## What is Environment Controller?

A Kubernetes controller for managing environments and their relationships across different backends. Currently implements GitHub backend integration that reports deployment status to GitHub's Deployments API and manages environment relationships.

## Key Features

- **Multi-Backend Support**: Generic Environment API that can support multiple backends (currently GitHub)
- **GitHub Deployment Integration**: Creates and manages GitHub deployments for Environment resources
- **Relationship Management**: Manages environment relationships (After, Alongside)
- **Status Reporting**: Reports deployment status back to GitHub Deployments API
- **Automatic RolloutGate Creation**: Automatically creates and manages RolloutGate resources

## Common Development Commands

```bash
# Generate code after modifying CRD types
make manifests              # Generate CRD YAML from Go types
make generate               # Generate DeepCopy methods

# Code quality
make fmt                    # go fmt
make vet                    # go vet
make lint                   # gofmt + govet

# Testing
make test                   # Run unit tests (Ginkgo/Gomega)
make test-e2e              # Run e2e tests on Kind cluster

# Building
make build                  # Build binary
make docker-build           # Build container image
make docker-push            # Push to registry

# Deployment
make install                # Install CRDs to cluster
make deploy                 # Deploy controller to cluster
make uninstall              # Remove CRDs and controller
make run                    # Run locally (without cluster deployment)
```

## Development Workflow

1. Modify CRD type definitions in `api/v1alpha1/*_types.go`
2. Run `make manifests generate` to update CRDs and DeepCopy methods
3. Implement reconciliation logic in `internal/controller/*_controller.go`
4. Write tests in `internal/controller/*_controller_test.go`
5. Run `make test` to verify
6. Deploy with `make docker-build docker-push deploy IMG=registry/image:tag`

## Environment Spec Example

```yaml
apiVersion: environments.kuberik.com/v1alpha1
kind: Environment
metadata:
  name: myapp-production
  namespace: default
spec:
  # Reference to the Rollout that this Environment manages
  rolloutRef:
    name: myapp-rollout

  # Environment configuration
  name: "kuberik-myapp-production"
  environment: "production"
  ref: "main"

  # Relationship configuration
  relationship:
    type: "After"
    environment: "staging"

  # Backend-specific configuration
  backend:
    type: "github"
    project: "myorg/myapp"
    secret: "github-token"

  # Optional sites and waves (see README "Sites and waves"). Omit for site-less.
  # site: eu-west-1   # defaults to the controller's --default-site / $SITE
  # wave: 1           # defaults to 1 when a site is set
  # lockstep: false
```

## GitHub Backend Integration

### GitHub Token Secret

Create a secret with the GitHub token:

```bash
kubectl create secret generic github-token --from-literal=token=ghp_xxx
```

Or using YAML:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: github-token
  namespace: default
type: Opaque
data:
  token: <base64-encoded-github-token>
```

### How It Works

1. **Environment Detection**: Watches for Environment resources with configured backend
2. **Backend Validation**: Validates that the backend is supported (currently only "github")
3. **Rollout Reference**: Fetches the referenced Rollout to get current deployment version
4. **Version Resolution**: Gets current version from Rollout's deployment history using `Revision` field
5. **GitHub Deployment Sync**: Syncs entire rollout history with GitHub deployments and statuses
6. **Status Reporting**: Reports deployment status to GitHub's Deployments API
7. **RolloutGate Management**: Automatically creates and manages RolloutGate resources
8. **Relationship Resolution**: Checks deployment statuses across environments to determine allowed versions

## Environment Relationships

**Relationship Types:**
- `After`: Sequential deployment (a version passes once the other environment's deployment of it succeeded)
- `Alongside`: Follow the other environment (a version passes as soon as the other environment starts deploying it, unless that deployment failed or was cancelled)

When relationships are specified, the controller automatically creates RolloutGate resources to enforce the deployment order.

## API Reference

### Environment Spec

```go
type EnvironmentSpec struct {
    RolloutRef       corev1.LocalObjectReference `json:"rolloutRef"`
    Name             string                      `json:"name"`
    Environment      string                      `json:"environment,omitempty"`
    Ref              string                      `json:"ref,omitempty"`
    Relationship     *EnvironmentRelationship    `json:"relationship,omitempty"`
    Backend          BackendConfig               `json:"backend"`
    RequeueInterval  string                      `json:"requeueInterval,omitempty"`
}

type BackendConfig struct {
    Type             string   `json:"type"`
    Project          string   `json:"project"`
    Secret           string   `json:"secret,omitempty"`
}

type EnvironmentRelationship struct {
    Environment string           `json:"environment"`
    Type        RelationshipType `json:"type"` // "After" or "Alongside"
}
```

### Environment Status

```go
type EnvironmentStatus struct {
    DeploymentID      *int64                    `json:"deploymentId,omitempty"`
    DeploymentURL     string                    `json:"deploymentUrl,omitempty"`
    DeploymentStatuses []EnvironmentStatusEntry  `json:"deploymentStatuses,omitempty"`
    EnvironmentInfos  []EnvironmentInfo         `json:"environmentInfos,omitempty"`
}
```

## Adding New Backends

To add a new backend (e.g., Slack, PagerDuty):

1. Add backend type to `BackendConfig` enum in `api/v1alpha1/environment_types.go`
2. Implement backend interface in `internal/controller/`
3. Add backend-specific logic for status reporting/notifications
4. Create example resources in `config/samples/`
5. Update documentation

## Common Patterns

### Controller Reconciliation

```go
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    environment := &environmentsv1alpha1.Environment{}
    if err := r.Get(ctx, req.NamespacedName, environment); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    // Validate backend
    // Fetch rollout
    // Sync with backend
    // Update status

    return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}
```

### Testing with Ginkgo/Gomega

```go
var _ = Describe("EnvironmentController", func() {
    Context("when environment is created", func() {
        It("should create GitHub deployment", func() {
            env := &environmentsv1alpha1.Environment{...}
            Expect(k8sClient.Create(ctx, env)).To(Succeed())

            Eventually(func() *int64 {
                err := k8sClient.Get(ctx, key, env)
                if err != nil {
                    return nil
                }
                return env.Status.DeploymentID
            }).ShouldNot(BeNil())
        })
    })
})
```

## Dependencies

The controller depends on:
- `github.com/kuberik/rollout-controller` - For Rollout CRD (local path replacement)
- `sigs.k8s.io/controller-runtime` - Kubebuilder framework
- GitHub API client libraries

## Local Path Dependencies

```go
replace github.com/kuberik/rollout-controller => ../rollout-controller
```

Run `go mod tidy` after making changes to the rollout-controller API.

## Debugging

```bash
# Controller logs
kubectl logs -n kuberik-system deployment/environment-controller -f

# Events
kubectl get events --sort-by='.lastTimestamp'

# Describe resources
kubectl describe environment <name>
kubectl describe rolloutgate <name>
```
