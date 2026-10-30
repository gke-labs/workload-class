---
name: review-adversarial
description: Run an adversarial red-team code review in a fresh context against the current branch diff before opening or updating a pull request in gke-labs/workload-class.
---

# Adversarial Pre-PR Review (`review-adversarial`)

Run this skill in a fresh agent context (a subagent or clean session that did not author the changes) before opening or updating a PR.

## Workflow

1. **Inspect the Full Branch Diff Against the Base Branch:**
   ```bash
   git fetch origin --prune && git diff origin/main...HEAD
   ```
   *(Use `upstream/main...HEAD` on a fork, or `<parent-branch>...HEAD` when reviewing a stacked PR.)*

2. **Adversarial Checklist (Hunt for Failures, Not Style):**
   - **Guardrail Gating & Fail-Open Safety (`I1`):** Can an unvalidated `WorkloadClass` (`Validated=False`) still block Pod evictions in `DisruptionWebhook`, keep a closed Per-Pod `PodDisruptionBudget` (`maxUnavailable: 0`), or mutate Pods for Spot placement?
   - **Per-Pod PDB Lifecycle & User-PDB Precedence (`I2`):** Can a reconciler error, Pod deletion, or selector change leave an orphaned `workload-<pod.Name>` PDB or a permanently closed PDB (`maxUnavailable: 0`) when `MaintenanceReadiness` is `Ready` or `Overdue`? Does the controller reliably step aside when a user-created PDB covers the Pod?
   - **Authorized-Subject PDB Lease Races (`I3`):** Can `DisruptionWebhook` (`handleAuthorizedSubject`) and `PendingLeaseTracker` drop or leak leases across reconcile retries, grant a lease to an unauthorized caller, or leave stale `workloads.gke.io/bypass-owner` / `workloads.gke.io/bypass-expiration` annotations after lease expiration?
   - **WorkloadClass Resolution Precedence (`I4`):** Do `WorkloadClassReconciler`, `DisruptionWebhook`, and `PodPlacementDefaulter` agree on Pod-to-`WorkloadClass` resolution (namespace default label -> highest selector specificity -> oldest `creationTimestamp`)?
   - **Spot Placement, Fallback Budgets & Active Reversion (`I5`):** Can concurrent Pod admissions or stale informer reads exceed `MaxFallbackRatio`, violate `EnforcementMode` (`Required` / `Forbidden`), or trigger eviction thrashing during `Active` reversion while Spot nodes are in stockout or before `minDurationOnFallback` has elapsed?
   - **Webhook & RBAC Safety:** Do admission webhooks fail open/closed appropriately? Are Kubebuilder RBAC markers minimal?
   - **Invariant Governance & Negative Tests:** Does the diff modify or weaken any safety invariant (`I1`–`I5`) in `internal/invariants/` or governance files (`.github/CODEOWNERS`, `verify_invariant_governance.sh`) on a Lane 1 branch? Are negative unit tests included for the failure mode being fixed?

3. **Output Format (`## Self-Review` Block):**
   Summarize every concrete risk checked and any code/test fixes applied under `## Self-Review` -> `Adversarial pass (review-adversarial)` in the PR body before requesting maintainer review.
