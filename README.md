# Environment Controller

A Kubernetes controller that orders environments by fixed tiers (dev, test, staging, prod) and rolls each environment out across sites and waves, across different backends. Currently implements GitHub backend integration that reports deployment status to GitHub's Deployments API and derives rollout gates from it.

## Features

- **Multi-Backend Support**: Generic Environment API that can support multiple backends (currently GitHub)
- **GitHub Deployment Integration**: Creates and manages GitHub deployments for Environment resources
- **Tier Ordering**: Environments are gated by a fixed tier order (dev, test, staging, prod); no per-object configuration
- **Sites and Waves**: One environment can run in several clusters (sites) that roll out in ordered waves, one owner per site
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
  environment: "prod"
  ref: "main"

  # Optional: which cluster instance this is, and its rollout wave
  site: "eu-west-1"   # defaults to the controller's default site, then "default"
  wave: 1             # defaults to 1

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
- `environment`: The tier of this environment, one of `dev`, `development`, `test`, `qa`, `staging`, `stage`, `preprod`, `prod`, `production` (see [Tier ordering](#tier-ordering)). Other values are rejected by validation.
- `backend`: Backend-specific configuration
  - `type`: Backend type (currently only "github" is supported)
  - `project`: Project identifier (for GitHub: "owner/repo")
  - `secret`: Name of the secret containing backend token (optional, default: "github-token" for GitHub)

### Optional Fields

- `ref`: Git reference (branch, tag, or SHA) - defaults to the revision from Rollout history
- `site`: Name of this cluster's instance of the environment (see [Sites and waves](#sites-and-waves)). Defaults to the controller's default site, then `default`.
- `wave`: 1-based wave of this site inside its environment. Defaults to 1.
- `lockstep`: See [Lockstep](#lockstep).
- `requeueInterval`: Interval for reconciliation (default: "1m")
- `relationship`: **Deprecated and ignored.** Environment order is fixed. Still accepted so existing manifests keep applying; the Environment reports `Deprecated=True` with reason `RelationshipIgnored`. It will be removed in a later release.

## Tier ordering

Environments are ordered by fixed tiers, earliest first: `dev` < `test` < `staging` < `prod`. `spec.environment` accepts these names, where the aliases are the same environment as their canonical tier everywhere (wave grouping, upstream lookup, GitHub matching):

| Tier (canonical) | Accepted names |
| --- | --- |
| 1 | `dev`, `development` |
| 2 | `test`, `qa` |
| 3 | `staging`, `stage`, `preprod` |
| 4 | `prod`, `production` |

An Environment is gated on its **upstream**: the last wave of the nearest earlier tier that is present in GitHub for the same deployment name. Tiers that are not present are skipped, so `prod` is gated on `staging` when there is no `test`, and `dev` has no upstream. Presence is read from GitHub, not from the cluster, so the upstream environment may live in another cluster.

## Sites and waves

One logical environment (`prod`) can run in several clusters. Each cluster's instance is a **site**; sites of one environment roll out in ordered **waves**. All coordination happens through GitHub deployments, so clusters never talk to each other.

### Fields

| Field | Meaning |
| --- | --- |
| `spec.site` | Name of this cluster's instance (`eu-west-1`). Max 63 chars, `[A-Za-z0-9._-]`. |
| `spec.wave` | 1-based wave of this site inside its environment. Defaults to 1. |
| `spec.lockstep` | If true on the wave-1 leader, no new version is selected until every wave baked the current one. |

The effective site is `spec.site`, else the controller's default site (flag `--default-site`, which defaults to `$SITE`; the manager Deployment fills `SITE` from the optional ConfigMap `kuberik-cluster-info`, key `site`, the same ConfigMap rollout-dashboard reads), else the literal `default`. The effective wave is `spec.wave`, else 1. Every Environment is therefore sited: it always carries the deregister finalizer, `status.wave` and the wave conditions.

Deployments in GitHub written before sites existed have no site in their payload. They count as site `default`, wave 1, so a single-cluster setup continues its history. A cluster whose default site is not `default` (for example `SITE=eu-west-1`) does not match those legacy deployments and starts a fresh history under its own site.

### Leader rule

Within `(environment, wave)` one site is the **leader** and writes the *selection record*: a GitHub deployment whose payload has `selection: true`. The leader is the lexicographically lowest registered site, but it is sticky while a selection is in flight: a lower-sorting site registering mid-rollout does not take over. Followers deploy exactly the newest selection record of their wave and do not wait for the leader's bake.

The `selection` flag on a deployment records the role from the *previous* gate decision (`status.wave.role`), because the deployment is written after the Rollout already acted on the gate. A leader's very first deployment therefore carries no flag yet; it acts as an implicit selection until the next reconcile records `Leader`, after which new deployments are flagged.

### Allowed versions

- Wave-1 leader: versions that completed the upstream environment (see [Tier ordering](#tier-ordering)): baked successfully by every registered member of the upstream's last wave. No upstream: unrestricted (the gate's `allowedVersions` is unset).
- Wave-N leader (N>1): versions baked successfully by every registered member of wave N-1.
- Follower: exactly the current selection of its wave.
- Revisions are mapped to tags via the Rollout's `releaseCandidates`; a revision without a tag is dropped.
- The waves of an environment must be exactly `1..max`; a gap blocks everything.

### Lockstep

When the wave-1 leader sets `lockstep: true`, its allowed set is pinned to the current selection until every registered member of every wave baked it successfully. Members that disagree with the leader's value surface `LockstepConsistent=False`; the leader's value governs.

### Status and conditions

`status.wave` records site, wave, role (`Leader`/`Follower`), leader, selected revision and the observed members. It is written before the RolloutGate is updated. Every Environment gets these conditions (`Deprecated` only while `spec.relationship` is set):

| Condition | Status / Reason | Meaning |
| --- | --- | --- |
| `WaveReady` | `True` / `Ready` | The site may deploy the allowed versions. |
| `WaveReady` | `False` / `WaitingFor` | Waiting for a named member to bake (message names it). Also set on a follower when the leader's bake of the selected revision is Failed or Cancelled (`prod/us-east-1 bake failed on 1a2b3c4`): the version drops out of the follower's gate, and a follower that has not started it gets an empty gate. A follower that already started it keeps going. |
| `WaveReady` | `False` / `DuplicateSite` | Another Environment owns this deployment name + tier + site (message names the owner). See [One owner per site](#one-owner-per-site). |
| `WaveReady` | `False` / `WaitingForCandidate` | The wanted revision has no release-candidate tag yet. |
| `WaveReady` | `False` / `Pinned` | The Rollout pins `spec.wantedVersion`. |
| `WaveReady` | `False` / `InvalidTopology` | Waves have a gap (this or the upstream environment). |
| `WaveReady` | `False` / `LockstepWaiting` | Lockstep holds the leader on the current selection. |
| `LockstepConsistent` | `False` / `LockstepMismatch` | A member disagrees with the leader's `lockstep`. |
| `Deprecated` | `True` / `RelationshipIgnored` | `spec.relationship` is set; it is ignored. Removed when the field is removed. |

### GitHub encoding

The GitHub environment stays `kuberik/<name>/<environment>` and the task `deploy:kuberik/<name>` for every site. The deployment payload carries `site`, `wave`, `lockstep`, `selection` and `owner` (the last three omitted when empty). Deployments are matched by (history ID, environment, site, owner), because Rollout history IDs are per-cluster sequences. Statuses are posted with `auto_inactive: false` so one site's success does not deactivate another's. Per reconcile the controller lists at most 10 pages of 100 deployments per environment (self and upstream), keeps the newest 20 per site and fetches each of their statuses once (see "GitHub call budget").

### One owner per site

Exactly one Environment may own a deployment name + tier + site across all clusters. The owner is identified by `{cluster, namespace, name}`, where `cluster` is the UID of the `kube-system` namespace (the controller needs `get` on namespaces). Every new deployment carries it in the payload as `owner`.

- The first registration wins. If two new Environments race, the one whose earliest deployment has the lower ID owns the site.
- A second Environment for the same deployment name, tier and site is a **duplicate**: it posts nothing to GitHub (no deployments, statuses or deregistration markers), its gate is closed (`allowedVersions: []`, so the Rollout cannot deploy), `status.wave` carries site and wave but no role, a Warning event is emitted, and `WaveReady` is `False` with reason `DuplicateSite`, naming the owner: `site <site> of <tier> for <name> is owned by cluster <uid> namespace <ns> name <name>`.
- Deleting a duplicate has no effect on the owner; it just drops its finalizer.
- The same object (same cluster, namespace and name) re-reconciling, or deleted and recreated, is still the owner.
- Deregistration frees the slot: after the owner deregisters (deleted, or site changed), another Environment may register the site.
- Deployments without an `owner` (legacy, or written by earlier builds) belong to nobody. They never cause a conflict and are adopted by whichever Environment reconciles them.

### Deletion

Every Environment carries the finalizer `environments.kuberik.com/deregister`. On delete the controller posts an `inactive` status `Deregistered: site <site> removed` (always with `auto_inactive: false`) on the site's newest deployment and removes the finalizer. A deregistered site no longer counts for membership, leadership or completion; its past selection records still do. GitHub's automatic `inactive` statuses (no description) appended after the marker do not undo it; any other newer status (the site deploys again) re-registers it.

The identity written to GitHub (deployment name, environment, site) is recorded in `status.wave` before the first GitHub write. Changing `spec.site`, `spec.environment` or `spec.name` deregisters the old identity first, then records the new one. Removing `spec.site` moves the Environment to the default site (the controller's, else `default`): the old site is deregistered and the new one registered. Re-registering the same site simply posts fresh statuses. A deregistration frees the site for another owner (see below).

Deleting an Environment removes the site from its wave. Its RolloutGate is owned by the Environment and is garbage-collected with it, which leaves the Rollout **ungated**: it deploys freely and may pick up any new release candidate. Pin the Rollout (`spec.wantedVersion`) or delete the Rollout first if that is not what you want.

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

Counted from the code, per reconcile (H = entries in the Rollout history, N = other Environments of the same name in the namespace):

| | Calls |
| --- | --- |
| History sync of this Environment | per history entry: 1 list deployments (by ref) + 1 list statuses; plus 1 create deployment and 1 create status per missing or changed entry |
| Environment graph (`status.environmentInfos`) | 1 list deployments of the task, then per relevant environment the paginated deployments list and one status list per displayed deployment; each of the N other Environments repeats the history sync |
| Wave reading | for self and the upstream: up to 10 list pages (100 each), plus one status list per deployment in the newest 20 of every site; status lists are shared with the graph through a per-reconcile cache, so no deployment's statuses are fetched twice |
| Site change or deletion | up to 10 list pages, 1 status list, 1 create status; plus the ownership check (shares the cached lists) |

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

8. **Tier and Wave Resolution**: The controller reads deployments and statuses from GitHub to determine the upstream tier, the site's wave and the allowed versions.

## Upgrading to sites and waves

`spec.relationship` (`After`, `Alongside`, `Parallel`) is gone as a feature: environment order is fixed by tier, and a site always has a wave. To upgrade:

1. Apply the new CRD, then the new controller, in every cluster that reports to the same GitHub repository.
2. Rename non-standard `spec.environment` values to one of the nine accepted names (`dev`, `development`, `test`, `qa`, `staging`, `stage`, `preprod`, `prod`, `production`). Objects outside that set fail validation on their next update; they keep running until then.
3. Optionally set `spec.site` and `spec.wave` (or provide the site once per cluster through the `site` key of the `kuberik-cluster-info` ConfigMap).
4. Remove `spec.relationship` at your leisure. It is accepted and ignored (condition `Deprecated=True`, reason `RelationshipIgnored`) and will be removed in a later release.
5. GitHub history continues: deployments without a site count as site `default`, wave 1. This holds as long as the Environment's effective site is `default`; if you set another site or the cluster's default site differs, a fresh history starts under that site.

What changes after the upgrade: every Environment gets the `environments.kuberik.com/deregister` finalizer and `status.wave`, and new deployment payloads carry `site`, `wave` and `owner`.

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
    Environment      string                      `json:"environment"` // dev|development|test|qa|staging|stage|preprod|prod|production
    Ref              string                      `json:"ref,omitempty"`
    Relationship     *EnvironmentRelationship    `json:"relationship,omitempty"` // deprecated, ignored
    Site             string                      `json:"site,omitempty"`
    Wave             *int32                      `json:"wave,omitempty"`
    Lockstep         bool                        `json:"lockstep,omitempty"`
    Backend          BackendConfig               `json:"backend"`
    RequeueInterval  string                      `json:"requeueInterval,omitempty"`
}

type BackendConfig struct {
    Type             string   `json:"type"`
    Project          string   `json:"project"`
    Secret           string   `json:"secret,omitempty"`
}
```

### EnvironmentRelationship (deprecated)

```go
// Accepted and ignored; removed in a later release.
type EnvironmentRelationship struct {
    Environment string           `json:"environment"`
    Type        RelationshipType `json:"type"` // "After", "Alongside" or "Parallel"
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
