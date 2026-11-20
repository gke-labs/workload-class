# GKE Workload Class — Runtime Safety Invariants (`I1`–`I5`)

This document defines the authoritative runtime safety invariants (`I1`–`I5`) for `gke-labs/workload-class` and its Two-Lane CODEOWNERS Invariant Governance (`verification-suite/verify_invariant_governance.sh`).

Each invariant rule in `internal/invariants/` evaluates an in-memory `Snapshot` of `WorkloadClass`, `WorkloadClassGuardrail`, `GKESpotPlacementPolicy`, `Pod`, and `PodDisruptionBudget` (`PDB`) objects to ensure that controller reconcilers and admission webhooks never wedge workloads, bypass organizational guardrails, or leak disruption locks.

---

## Core Runtime Safety Invariants (`I1`–`I5`)

### `I1` — Guardrail Gating & Fail-Open Safety (`GuardrailValidationGating`)
Organizational `WorkloadClassGuardrail` constraints bound the disruption windows, maximum non-disruption duration, and Spot placement behavior that workload owners may declare. When a `WorkloadClass` fails guardrail validation (`Validated=False` with `Reason=ValidationFailed` or `Reason=PluginResolutionFailed`), it must **fail open** for disruption protection rather than enforcing an unauthorized policy:
- `I1` verifies that no `WorkloadClass` with `Validated=False` maintains a closed Per-Pod `PodDisruptionBudget` (`spec.maxUnavailable == 0`) on any governed Pod (unless an active authorized bypass lease is irrelevant because invalid classes must set `maxUnavailable: "100%"`), nor blocks Pod evictions in `DisruptionWebhook`.

### `I2` — Per-Pod PDB Fidelity & User-PDB Precedence (`PerPodPDBFidelityAndPrecedence`)
The `WorkloadClass` controller manages a dedicated Per-Pod `PodDisruptionBudget` (`workload-<pod.Name>`) for each governed Pod when at least one `WorkloadClassGuardrail` exists in the cluster:
- **Single-Pod UID Scoping:** Every controller-managed PDB (`workload-*` carrying `workloads.gke.io/pod-uid`) must select strictly that single Pod's `metadata.uid` (`spec.selector.matchLabels["workloads.gke.io/pod-uid"] == pod.Labels["workloads.gke.io/pod-uid"] == string(pod.UID)`).
- **User-PDB Precedence:** If a non-controller-managed (user-created) PDB in the namespace selects a Pod, the controller must step aside and must not maintain a competing `workload-<pod.Name>` PDB for that Pod.
- **No Orphaned Controller PDBs:** No `workload-<pod.Name>` PDB may outlive its target Pod or remain when the Pod is no longer governed by any `WorkloadClass`.
- **Readiness-Synchronized Budget:** When no active authorized bypass lease (`I3`) is present on `workload-<pod.Name>`, `spec.maxUnavailable` must be `0` if and only if the governing `WorkloadClass` is validated and `status.maintenanceReadiness == NotReady`, and `"100%"` when `status.maintenanceReadiness` is `Ready` or `Overdue` (or when `Validated=False`).

### `I3` — Bounded Authorized-Subject PDB Leases (`BoundedAuthorizedLease`)
When a caller listed in `spec.disruptionPolicy.allowedDisruptionsOutsideOfWindow` evicts a Pod while `status.maintenanceReadiness == NotReady`, `DisruptionWebhook` queues an asynchronous Per-Pod PDB lease via `PendingLeaseTracker` and `WorkloadClassReconciler` opens `workload-<pod.Name>` (`spec.maxUnavailable = "100%"`) stamped with `workloads.gke.io/bypass-owner` and `workloads.gke.io/bypass-expiration`:
- `I3` verifies that any controller-managed PDB carrying `workloads.gke.io/bypass-owner` also carries a valid, parseable `workloads.gke.io/bypass-expiration` timestamp, that the bypass owner corresponds to an identity authorized in the governing `WorkloadClass.spec.disruptionPolicy.allowedDisruptionsOutsideOfWindow`, and that no expired lease (`now > bypass-expiration`) leaves `spec.maxUnavailable == "100%"` while `status.maintenanceReadiness == NotReady`.

### `I4` — Deterministic WorkloadClass Resolution Precedence (`DeterministicClassPrecedence`)
Pod-to-`WorkloadClass` resolution must agree across `WorkloadClassReconciler`, `DisruptionWebhook`, and `PodPlacementDefaulter`:
1. **Namespace Default / Enforced Class (`workloads.gke.io/default-class`):** If the Pod's `Namespace` carries `workloads.gke.io/default-class: <name>`, that `WorkloadClass` takes precedence over all `podSelector` matches in the namespace.
2. **Highest Selector Specificity:** Otherwise, among matching `WorkloadClass` resources in the namespace, the class with the highest selector specificity (`len(matchLabels) + len(matchExpressions)`) wins.
3. **Deterministic Tie-Breaking:** Ties in specificity are broken by oldest `metadata.creationTimestamp` (and then lexical `metadata.name`).
- `I4` verifies that every controller-managed Per-Pod PDB (`workload-<pod.Name>`) is reconciled against the unique highest-precedence `WorkloadClass` for that Pod.

### `I5` — Spot Placement Guardrail & Reversion Safety (`SpotPlacementAndReversionSafety`)
When `WorkloadClass.spec.capacityStrategy` and `GKESpotPlacementPolicy` govern Pod placement:
- **Forbidden Mode Compliance:** If a cluster `WorkloadClassGuardrail` configures `gke-spot-placement` with `enforcementMode: Forbidden`, no validated `WorkloadClass` may target `Spot` capacity (`spec.capacityStrategy.type == "Spot"` with a positive `spotRatio`).
- **Active Reversion Safety:** Proactive On-Demand -> Spot eviction (`reversion.action == "Active"`) must never evict a fallback Pod while the referenced `GKESpotPlacementPolicy` reports `SpotCapacityAvailable=False` (`Reason=SpotStockout`) or before the Pod has run on fallback capacity for at least the effective `minDurationOnFallback`.

---

## Two-Lane CODEOWNERS Invariant Governance (`T0`)

Changes to `internal/invariants/**`, `docs/invariants.md`, `.github/CODEOWNERS`, and `verification-suite/verify_invariant_governance.sh` are enforced by `./verification-suite/verify_invariant_governance.sh`:

- **Lane 1 (`feature/*`, `fix/*`, `docs/*`, `chore/*`):** Must **not** modify `internal/invariants/**`, `docs/invariants.md`, `.github/CODEOWNERS`, or `verification-suite/verify_invariant_governance.sh`. Never weaken an invariant rule to make a unit test pass. Rare atomic reconciler + invariant co-changes require an `Invariant-Cochange: <rationale>` commit trailer (or the `allow-invariant-cochange` PR label).
- **Lane 2 (`invariant/*`):** Dedicated invariant PRs must touch **only** `internal/invariants/**` and `docs/invariants.md`, and must include both positive (`GREEN`) and negative fault-injection (`RED`) unit tests in `internal/invariants/invariants_test.go`.
