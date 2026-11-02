# Environment Controller

A Kubernetes controller for managing environments and their relationships across different backends. Currently implements GitHub backend integration that reports deployment status to GitHub's Deployments API and manages environment relationships.

## Features

- **Multi-Backend Support**: Generic Environment API that can support multiple backends (currently GitHub)
- **GitHub Deployment Integration**: Creates and manages GitHub deployments for Environment resources
- **Relationship Management**: Manages environment relationships between environments (After, Alongside)
- **Status Reporting**: Reports deployment status back to GitHub Deployments API
- **Automatic RolloutGate Creation**: Automatically creates and manages RolloutGate resources

## Architecture

The controller manages Environment resources and integrates with backend-specific implementations:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Environment   │    │  Environment    │    │  GitHub Deploy  │
│   (CRD)          │───▶│  Controller     │───▶│  API            │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  RolloutGate    │
                       │  (auto-created) │
                       └─────────────────┘
```

## Configuration

The controller uses the Environment CRD for configuration:

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
```

## Environment Spec

### Required Fields

- `rolloutRef`: Reference to the Rollout resource
- `name`: Name of the deployment (the "kuberik" prefix will be automatically added for GitHub backend)
- `backend`: Backend-specific configuration
  - `type`: Backend type (currently only "github" is supported)
  - `project`: Project identifier (for GitHub: "owner/repo")
  - `secret`: Name of the secret containing backend token (optional, default: "github-token" for GitHub)

### Optional Fields

- `environment`: Environment name (e.g., "production", "staging")
- `ref`: Git reference (branch, tag, or SHA) - defaults to the revision from Rollout history
- `relationship`: Defines relationship to other environments
  - `type`: "After" or "Alongside" (see [Environment Relationships](#environment-relationships))
  - `environment`: Environment name this deployment relates to
- `requeueInterval`: Interval for reconciliation (default: "1m")

## Sites and waves

One logical environment (`prod`) can run in several clusters. Each cluster's instance is a **site**; sites of one environment roll out in ordered **waves**. All coordination happens through GitHub deployments, so clusters never talk to each other.

### Fields

| Field | Meaning |
| --- | --- |
| `spec.site` | Name of this cluster's instance (`eu-west-1`). Max 63 chars, `[A-Za-z0-9._-]`. |
| `spec.wave` | 1-based wave of this site inside its environment. Defaults to 1 when a site is set. |
| `spec.lockstep` | If true on the wave-1 leader, no new version is selected until every wave baked the current one. |

If `spec.site` is empty the controller uses its default site: flag `--default-site`, which defaults to `$SITE`. The manager Deployment fills `SITE` from the optional ConfigMap `kuberik-cluster-info`, key `site` (the same ConfigMap rollout-dashboard reads). With neither set an Environment is **site-less** and behaves exactly as before: same GitHub names, same payloads, no finalizer, no new conditions or status.

### Leader rule

Within `(environment, wave)` one site is the **leader** and writes the *selection record*: a GitHub deployment whose payload has `selection: true`. The leader is the lexicographically lowest registered site, but it is sticky while a selection is in flight: a lower-sorting site registering mid-rollout does not take over. Followers deploy exactly the newest selection record of their wave and do not wait for the leader's bake.

The `selection` flag on a deployment records the role from the *previous* gate decision (`status.wave.role`), because the deployment is written after the Rollout already acted on the gate. A leader's very first deployment therefore carries no flag yet; it acts as an implicit selection until the next reconcile records `Leader`, after which new deployments are flagged.

### Allowed versions

- Wave-1 leader: versions that completed the upstream environment (`relationship`): baked successfully by every registered member of the upstream's last wave. No upstream: unrestricted (the gate's `allowedVersions` is unset).
- Wave-N leader (N>1): versions baked successfully by every registered member of wave N-1.
- Follower: exactly the current selection of its wave.
- A site-less Environment with a sited upstream uses the upstream's last wave as above.
- Revisions are mapped to tags via the Rollout's `releaseCandidates`; a revision without a tag is dropped.
- The waves of an environment must be exactly `1..max`; a gap blocks everything.

### Lockstep

When the wave-1 leader sets `lockstep: true`, its allowed set is pinned to the current selection until every registered member of every wave baked it successfully. Members that disagree with the leader's value surface `LockstepConsistent=False`; the leader's value governs.

### Status and conditions

`status.wave` records site, wave, role (`Leader`/`Follower`), leader, selected revision and the observed members. It is written before the RolloutGate is updated. Only sited Environments get these conditions:

| Condition | Status / Reason | Meaning |
| --- | --- | --- |
| `WaveReady` | `True` / `Ready` | The site may deploy the allowed versions. |
| `WaveReady` | `False` / `WaitingFor` | Waiting for a named member to bake (message names it). |
| `WaveReady` | `False` / `WaitingForCandidate` | The wanted revision has no release-candidate tag yet. |
| `WaveReady` | `False` / `Pinned` | The Rollout pins `spec.wantedVersion`. |
| `WaveReady` | `False` / `InvalidTopology` | Waves have a gap (this or the upstream environment). |
| `WaveReady` | `False` / `LockstepWaiting` | Lockstep holds the leader on the current selection. |
| `LockstepConsistent` | `False` / `LockstepMismatch` | A member disagrees with the leader's `lockstep`. |

### GitHub encoding

The GitHub environment stays `kuberik/<name>/<environment>` and the task `deploy:kuberik/<name>` for every site. The deployment payload gains `site`, `wave`, `lockstep` and `selection` (all omitted when empty). Deployments are matched by (history ID, environment, site), because Rollout history IDs are per-cluster sequences. Statuses of sited deployments are posted with `auto_inactive: false` so one site's success does not deactivate another's. Per reconcile the controller lists at most 10 pages of 100 deployments per environment (self and upstream), keeps the newest 20 per site and fetches each of their statuses once (see "GitHub call budget").

### Deletion

Sited Environments carry the finalizer `environments.kuberik.com/deregister`. On delete the controller posts an `inactive` status `Deregistered: site <site> removed` (always with `auto_inactive: false`) on the site's newest deployment and removes the finalizer. A deregistered site no longer counts for membership, leadership or completion; its past selection records still do. GitHub's automatic `inactive` statuses (no description) appended after the marker do not undo it; any other newer status (the site deploys again) re-registers it.

The identity written to GitHub (deployment name, environment, site) is recorded in `status.wave` before the first GitHub write. Changing `spec.site`, `spec.environment` or `spec.name` deregisters the old identity first, then records the new one. Removing the site altogether deregisters, releases the finalizer and clears `status.wave` and the wave conditions. Re-registering the same site simply posts fresh statuses.

Wave and lockstep changes reach the other sites only with this site's next deployment, because both are read from the newest deployment's payload.

Bypass when GitHub is unreachable or the cluster is dying:

```bash
kubectl annotate environment X environments.kuberik.com/skip-deregister=true
```

The controller also gives up (event plus finalizer removal) when the namespace is terminating and the token Secret is gone. If the controller itself is down:

```bash
kubectl patch environment X --type=merge -p '{"metadata":{"finalizers":null}}'
```

Both bypasses, and a cluster that simply died, leave the site registered in GitHub. A registered site that never bakes keeps blocking the next wave and lockstep until someone posts the deregistration marker for it. The CLI for that is a follow-up; until then post the `inactive` status by hand on that site's newest deployment.

### GitHub call budget

Counted from the code, per reconcile (H = entries in the Rollout history, N = other Environments of the same name in the namespace that are part of the relationship graph):

| | Calls |
| --- | --- |
| History sync of this Environment | per history entry: 1 list deployments (by ref) + 1 list statuses; plus 1 create deployment and 1 create status per missing or changed entry |
| Relationship graph | 1 list deployments of the task, then per relevant environment the paginated deployments list and one status list per displayed deployment; each of the N other Environments repeats the history sync |
| Wave reading, sited | for self and the upstream: up to 10 list pages (100 each), plus one status list per deployment in the newest 20 of every site; status lists are shared with the graph through a per-reconcile cache, so no deployment's statuses are fetched twice |
| Wave reading, site-less with an upstream | 1 list page of the upstream environment (more only when it turns out to be sited) |
| Site-less without relationship | none beyond the history sync and graph |
| Site change or deletion | up to 10 list pages, 1 status list, 1 create status |

Large fleets multiply the status lists by the number of sites. The budget relies on the `ghcache` conditional requests (ETag): a `304 Not Modified` answer does not count against the GitHub rate limit, so an idle fleet is cheap. For large fleets raise `spec.requeueInterval` (for example `5m`) so the steady-state polling stays well under the limit.

### Follow-ups

- CLI to deregister dead clusters.
- Helm chart wiring of `SITE` / the `kuberik-cluster-info` ConfigMap.
- Dashboard UI for sites, waves and the conditions above.

See `config/samples/environment-sites-and-waves.yaml` for a three-site example.

## GitHub Token Secret

The controller requires a GitHub token to authenticate with the GitHub API. Create a secret with the token:

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

## Requirements

- The referenced `Rollout` must have deployment history with a `Revision` field in the `VersionInfo` structure
- For GitHub backend, the "kuberik" prefix will be automatically added to deployment names if not already present
- If the revision is not available, the controller will requeue and wait

## How It Works

1. **Environment Detection**: The controller watches for Environment resources with the configured backend.

2. **Backend Validation**: The controller validates that the backend is supported (currently only "github").

3. **Rollout Reference**: The controller fetches the referenced Rollout to get the current deployment version.

4. **Version Resolution**: The controller gets the current version from the Rollout's deployment history, using the `Revision` field from `VersionInfo`.

5. **GitHub Deployment Sync**: For GitHub backend, the controller syncs the entire rollout history with GitHub deployments and statuses.

6. **Status Reporting**: The deployment status is reported back to GitHub's Deployments API.

7. **RolloutGate Management**: The controller automatically creates and manages RolloutGate resources for the Environment.

8. **Relationship Resolution**: If relationships are specified, the controller checks deployment statuses across environments to determine allowed versions.

## Environment Relationships

An Environment with a `relationship` gets a RolloutGate whose `allowedVersions` list comes from the related environment's deployment history, as reported through GitHub deployment statuses. The relationship type decides which of those versions pass:

| Type | A version passes the gate once the related environment's deployment of it is | Use it for |
|------|------------------------------------------------------------------------------|------------|
| `After` | `Succeeded` | Promotion: deploy only what has been deployed and baked successfully before, e.g. `production` after `staging`. |
| `Alongside` | `Deploying`, `InProgress` or `Succeeded` (not `Failed` or `Cancelled`) | Following: deploy the same version as another environment at the same time, e.g. a second production region that should inherit `production`'s manual approvals instead of needing its own. |

```yaml
# prod-eu-1 deploys whatever prod deploys, as soon as prod starts deploying it
spec:
  environment: "prod-eu-1"
  relationship:
    type: "Alongside"
    environment: "prod"
```

The gate is an allow list that the Rollout only consults when it picks its next version. For `Alongside` this means:

- **Failures on the related environment**: when the related environment's deployment of a version fails or is cancelled, the version drops out of the allow list. If this environment has already started deploying it, that deployment keeps going and is judged by this environment's own bake and health checks.
- **Rollbacks don't propagate**: a Rollout only moves to release candidates newer than the version it runs, so when the related environment rolls back, this environment stays where it is.

### Mixed controller versions

Every controller that reports the same deployment `name` to the same GitHub repository reads the other environments' relationships from the GitHub deployment payloads and copies them into its own Environment's status:

- A relationship type the controller doesn't know, for example `Parallel` written by an older controller, is treated as `After`. It is never written to status as-is.
- Older controllers (v0.1.x) don't know `Alongside`, and their CRD rejects it in status. Upgrade the controller and CRD in every cluster that reports to the repository before you set any Environment to `Alongside`. Until then, those older controllers fail to update their status and RolloutGates.

### Upgrading from `Parallel`

`Parallel` has been removed. It never gated differently from `After`: both only passed versions whose deployment on the related environment succeeded. With the new CRD, an Environment that still has `type: Parallel` fails validation on its next update. Until it is updated, the controller treats a stored `Parallel` as `After`.

To upgrade without disruption:

1. Change every `type: Parallel` to `type: After`. Both the old and the new CRD accept it, and it gates exactly as before.
2. Upgrade the controller and CRD in every cluster that reports to the same GitHub repository.
3. Set `type: Alongside` on the environments that should follow another one.

## Installation

### Prerequisites

- Kubernetes cluster
- kubectl configured
- GitHub token with appropriate permissions (for GitHub backend)

### Install the Controller

1. **Install CRDs**:
   ```bash
   kubectl apply -f config/crd/bases/
   ```

2. **Install the controller**:
   ```bash
   kubectl apply -k config/default/
   ```

3. **Create GitHub token secret**:
   ```bash
   kubectl apply -f config/samples/github-token-secret.yaml
   ```

4. **Create Environment resources**:
   ```bash
   kubectl apply -k config/samples/
   ```

## Development

### Building

```bash
make build
```

### Testing

```bash
make test
```

### Running Locally

```bash
make run
```

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
```

### EnvironmentRelationship

```go
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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache 2.0
