# Recommendation: scope WorkloadClassGuardrails to namespaces

Status: draft recommendation from a GKE demo cluster. Author: vhamburger. Date: 2026-11-17.

## Problem

A `WorkloadClassGuardrail` has no selector (`api/v1/workloadclassguardrail_types.go`: the spec has only
`constraints` and `pluginConstraints`). `validateAgainstGuardrails` in `internal/controller/workloadclass_controller.go`
merges every guardrail in the cluster (intersection of `daysOfWeek`, minimum `maxAllowedWindows` and
`maxNonDisruptionDurationDays`) and validates every WorkloadClass against the result. Capacity limits are merged the
same way. A platform team can make the whole cluster stricter, but can't make one tier stricter.

Observed on the demo cluster on 2026-11-17: the demo has a second guardrail, `performance-tier`, intended for the
inference workload. Setting its `daysOfWeek` to `[Monday, Tuesday, Wednesday, Thursday]` (a Friday freeze) marked all
three demo classes `Validated=False`: `checkout-critical`, `web-spot-60` and `inference-perf-50`, because each has a
Monday to Friday window. This is more than a status flag:

- The PDBs of `checkout-critical` changed from `maxUnavailable: 0` to `100%`, so its pods lost PDB protection outside
  their window.
- The Spot placement webhook stops mutating pods of non-validated classes, so new `web-frontend` and `inference-api`
  pods would be placed without Spot or compute-class rules.

Lifting the freeze restored `Validated=True` and closed the PDBs again. The deployed `performance-tier` also caps
`maxFallbackRatio: "50%"`, which by the "most restrictive wins" rule applies to `web-spot-60` as well (merge semantics
from the README; the plugin code that enforces the cap was not read).

## Proposal

Add an optional `spec.selector.namespaceSelector` (a standard `metav1.LabelSelector`):

```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: performance-tier
spec:
  selector:
    namespaceSelector:
      matchLabels: {tier: performance}
  constraints:
    disruption:
      daysOfWeek: [Monday, Tuesday, Wednesday, Thursday]
```

| Rule | Behavior |
|---|---|
| `selector` unset (nil) | applies to all classes, as today |
| `namespaceSelector: {}` | matches all namespaces (Kubernetes label-selector convention) |
| `namespaceSelector` with labels | applies only to classes in matching namespaces |
| Scope | the selector scopes both `constraints` and `pluginConstraints` |
| Several guardrails match a class | strictest value per field, computed per class from the matching guardrails only |
| Relaxing | a scoped guardrail can't relax an unscoped one; limits only combine to the stricter value |

Only namespace labels for v1: they are usually controlled by the platform team. A selector on WorkloadClass labels
would let a workload owner leave a stricter tier by removing a label.

`GKESpotPlacementPolicy` is cluster-scoped and can be shared across namespaces, so "which guardrails apply to a
policy" has no single answer. Policy validation moves to each (class, policy) pair, using the guardrails that match the
class's namespace.

## Status messages

Today a violation message gives the effective limit, for example "must be between 1 and 30, inclusive (guardrail
limit)", but not which guardrail set it. Upstream omits guardrail names on purpose: guardrails are cluster-scoped and
users who can read a WorkloadClass may not be allowed to read them (comment in `validateAgainstGuardrails`). Keep that,
and add an optional `spec.contact` string (for example a team alias) that the message includes instead of the name.

## Compatibility and operations

- Relabeling a namespace must re-reconcile its WorkloadClasses. The controller already watches Namespaces, but its
  predicate only passes changes to `workloads.gke.io/enforced-class`; extend it to any label change matched by a guardrail
  `namespaceSelector`, and do the same for the gke-spot-placement reconciler.
- The guardrail admission webhook warns only for classes the guardrail matches.
- The "disjoint `daysOfWeek`" message becomes per class.
- Version skew: an older controller ignores `selector`, so a scoped guardrail would apply cluster-wide and could open
  PDBs elsewhere. The controller sets a `SelectorSupported` condition on guardrails it can scope, and the docs tell
  platform teams to create a scoped guardrail without restrictive constraints first, check `SelectorSupported`, and then
  tighten it.

## Alternatives considered

- Separate clusters per tier: works, but multiplies clusters for a policy difference.
- Tier limits in per-tier placement policies: covers placement only, not disruption windows.
- A namespaced guardrail kind: possible, but would duplicate the cluster-scoped kind and its webhook.

## Open questions

- Should `spec.contact` be required for scoped guardrails?
- Is a `workloadClassSelector` needed later, if namespaces turn out too coarse, and who should own those labels?
