# Maintainers Guide

This guide documents the architecture, control flows, invariants, and development workflows of the `gke-labs/workload-class` controller and admission webhooks.

## Architecture Overview

The manager (`cmd/main.go`) runs four controllers and three admission webhooks in a single binary:

| Component | Package | Responsibility |
| --- | --- | --- |
| `WorkloadClassReconciler` | `internal/controller/workloadclass_controller.go` | Validates `WorkloadClass` specs against `WorkloadClassGuardrail`s, computes `status.maintenanceReadiness`, and reconciles Per-Pod `PodDisruptionBudget`s (including async bypass leases). |
| `WorkloadClassGuardrailReconciler` | `internal/controller/workloadclassguardrail_controller.go` | Validates cluster-scoped guardrails (days of week, plugin constraint JSON schemas) and updates guardrail status. |
| `GKESpotPlacementPolicyReconciler` | `internal/controller/gkespotplacementpolicy_controller.go` | Validates cluster-scoped `GKESpotPlacementPolicy` resources against guardrails and sets `PlacementPluginAttached` on referencing `WorkloadClass` objects. |
| `GKESpotPlacementReconciler` | `internal/controller/gkespotplacement_controller.go` | Tracks Spot capacity (`kube-system/cluster-autoscaler-status` + Spot nodes), maintains `InFallback`, and runs active Spot reversion evictions. |
| `DisruptionWebhook` | `internal/webhook/disruption_webhook.go` | Read-only validating webhook (`/validate-disruption`) on `pods/eviction` (`CREATE`). Enforces disruption windows, `minInitialRunDurationDays`, and authorized bypass identities. |
| `PodPlacementDefaulter` | `internal/webhook/v1/pod_webhook.go` | Fail-open mutating webhook (`/mutate-pod-placement`) on `pods` (`CREATE`). Injects Spot/On-Demand selectors, affinities, tolerations, and Custom Compute Class selectors. |
| `WorkloadClassGuardrailCustomValidator` | `internal/webhook/v1/workloadclassguardrail_webhook.go` | Validating webhook on `WorkloadClassGuardrail` create/update. Never blocks guardrail changes; returns non-blocking warnings for existing non-compliant `WorkloadClass` objects. |

## Key Flows & Design Invariants

### 1. WorkloadClass Resolution Precedence

`utils.FindBestMatchWorkloadClass` resolves the governing `WorkloadClass` for a Pod in strict order:

1. **Namespace-enforced class**: If the Pod's namespace carries `workloads.gke.io/enforced-class: <name>`, that `WorkloadClass` governs every Pod in the namespace, overriding `podSelector`.
2. **Most specific `podSelector`**: Otherwise, the `WorkloadClass` in the same namespace with the most matching label selector requirements wins.
3. **Oldest creation timestamp (then lexicographical name)**: Deterministic tie-breaker for equal specificity.

### 2. Per-Pod PDBs and Async Leasing

- Every governed active Pod not already covered by a user-created PDB gets a dedicated Per-Pod PDB named `workload-<pod.Name>` (truncated with a deterministic SHA-256 suffix if longer than 253 chars) selecting strictly `workloads.gke.io/pod-uid: <pod.UID>`.
- **Read-only admission webhook**: `DisruptionWebhook` never writes to PDBs directly. When an identity in `allowedDisruptionsOutsideOfWindow` requests eviction of a Pod with a closed Per-Pod PDB, the webhook enqueues a request into the shared in-memory `PendingLeaseTracker`. The first eviction is rejected by kube-apiserver (`429 TooManyRequests`); `WorkloadClassReconciler` wakes on the channel, temporarily opens the PDB with `workloads.gke.io/bypass-owner` and `workloads.gke.io/bypass-expiration` annotations, and the caller's retry succeeds.
- **Same-process requirement**: Because `PendingLeaseTracker` is in-memory, the disruption webhook and `WorkloadClassReconciler` must run in the same active leader replica (run with `replicas: 1`).
- **Fail-open on invalid policies**: If a `WorkloadClass` fails guardrail validation, its Per-Pod PDBs are opened (`maxUnavailable: 100%`) rather than deleted, and `DisruptionWebhook` skips policy enforcement. Overlapping `podSelector`s (`ReasonSelectorOverlap`) still keep protection active because each Pod resolves to a single best match.

### 3. Placement Plugin Handshake

1. `WorkloadClassReconciler` detects `spec.infrastructureProfileRef` and initializes `PlacementPluginAttached=False (PluginPending)` for the current `metadata.generation`.
2. `GKESpotPlacementPolicyReconciler` validates the referenced `GKESpotPlacementPolicy` and updates `PlacementPluginAttached=True (ProfileResolved)` or `False (PluginResolutionFailed)` with `ObservedGeneration == wc.Generation`.
3. `PodPlacementDefaulter` and `GKESpotPlacementReconciler` only act once validation and plugin attachment match the current generation.

### 4. GKE Subject Identity Note

On managed GKE clusters, the control-plane Cluster Autoscaler runs off-cluster and authenticates using client cert / RBAC identity `User: system:cluster-autoscaler` (verify on your cluster via `kubectl get clusterrolebinding -o yaml | grep cluster-autoscaler` or eviction webhook audit logs), whereas self-hosted Cluster Autoscaler deployments authenticate as `ServiceAccount: kube-system/cluster-autoscaler`. Include both entries (or the one matching your cluster) in `allowedDisruptionsOutsideOfWindow`.

## Adding New CRD Fields Checklist

When adding or modifying API types under `api/v1/`:

1. Add struct fields, doc comments, and `+kubebuilder:validation:...` / CEL markers in `api/v1/*_types.go`.
2. Regenerate manifests, deepcopy, and installer YAML:
   ```sh
   make manifests generate build-installer
   ```
   Verify `config/manager/kustomization.yaml` has no unintended image diff after `make build-installer`.
3. Mirror any CRD schema changes from `config/crd/bases/` into the corresponding Helm chart template under `dist/chart/templates/crd/`.
4. Run full tests, linter, and Helm chart lint:
   ```sh
   make fmt vet test
   make lint
   helm lint ./dist/chart
   ```

## Live Eviction Testing

To verify `DisruptionWebhook` and Per-Pod PDB behavior against a live cluster:

```sh
kubectl apply -k config/samples/
kubectl proxy --port=8001 &
PROXY_PID=$!

POD=$(kubectl get pods -n sample -l role=batch-processor -o jsonpath='{.items[0].metadata.name}')
curl -s -X POST "http://127.0.0.1:8001/api/v1/namespaces/sample/pods/${POD}/eviction" \
  -H "Content-Type: application/json" \
  -d "{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"${POD}\",\"namespace\":\"sample\"}}"

kill "${PROXY_PID}"
```

To test an authorized bypass subject via impersonation:

```sh
kubectl create --raw "/api/v1/namespaces/sample/pods/${POD}/eviction" \
  --as="system:cluster-autoscaler" \
  -f - <<EOF
{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"${POD}","namespace":"sample"}}
EOF
```
