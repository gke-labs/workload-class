# Proposal: compute-class capacity strategy for WorkloadClass

Status: draft recommendation, based on an experiment on GKE 1.36.4-gke.2377000 with workload-class v0.0.3.
Author: vhamburger. Date: 2026-11-17. "Patch 1" (compute-class toleration) and "patch 2" (opt-in
`count-compute-class-as-target` annotation) refer to the companion pull requests.

## Summary

Add `capacityStrategy.type: ComputeClass` with a `computeClass.preferredRatio`, so a WorkloadClass can say
"50% of these pods on the preferred compute class, the rest on the default one". A new GKE profile,
`GKEComputeClassPlacementPolicy`, names the preferred class, and a new guardrail entry caps the preferred share.
Fallback and migration back stay inside GKE compute classes (priority list, `activeMigration`), because GKE allows
only one compute class per pod.

## Problem

The API can only split Spot versus On-Demand. The demo builds a compute-class split by pointing a
`GKESpotPlacementPolicy` at an on-demand compute class, which needs two local patches, and the YAML doesn't say what
it does:

| Intent | What the YAML says today |
|---|---|
| 50% of pods on `performance-od` | `capacityStrategy: {type: Spot, spotRatio: "50%"}` |
| Fall back from C3 to N2 | set in the ComputeClass priorities; the WorkloadClass still says `action: FallbackToOnDemand` |
| Other half on a second compute class | not expressible: the plugin adds required `gke-spot DoesNotExist`, which GKE rejects next to a compute class. In the demo the other half lands on the default node pool (e2). |

## Constraints observed on GKE (server-side dry runs on the demo cluster)

1. **One compute class per pod.** GKE Warden (`ccc-node-affinity-selector-limitation`) denies required
   `compute-class In [performance-od, standard-od]`: "Custom Compute Classes allow only one value". A pod can't
   target "preferred or default"; cross-class fallback per pod is not possible.
2. **No hard Spot or machine-family terms next to a compute class.** Required `cloud.google.com/gke-spot` or
   `cloud.google.com/machine-family` terms, and a `gke-spot` nodeSelector, are denied. Tolerations and preferred
   affinity are allowed.
3. **Taint toleration.** Compute-class nodes carry `cloud.google.com/compute-class=<name>:NoSchedule`. GKE's mutating
   webhook adds the toleration only if the pod already selects the class when it runs; it runs before the
   workload-class webhook and isn't reinvoked. The plugin must add the toleration itself (patch 1).
4. **Count by compute class.** Counting by the node's Spot label treats pods on an on-demand preferred class as
   fallback; adding pods one at a time put 10 of 10 on the preferred class. Patch 2 (opt-in annotation) counts the
   policy's class instead and held 5/5 over gradual scale-up and three pod deletions.

## Proposed API

### WorkloadClass

```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: inference-perf-50
  namespace: demo-ccc
spec:
  podSelector:
    matchLabels: {app: inference-api}
  capacityStrategy:
    type: ComputeClass            # new enum value; Spot and OnDemand unchanged
    computeClass:                 # required when type is ComputeClass, forbidden otherwise
      preferredRatio: "50%"
  infrastructureProfileRef:
    kind: GKEComputeClassPlacementPolicy
    name: performance-split
```

Validation: CEL requires `computeClass` exactly when `type: ComputeClass` (the defaulted `spotRatio` can't signal
intent) and rejects non-default `spotRatio`/`action` with it:
`self.type == 'ComputeClass' ? (has(self.computeClass) && self.spotRatio == '100%' && self.action == 'Fail') : !has(self.computeClass)`.
`type: ComputeClass` requires a `GKEComputeClassPlacementPolicy` ref; any other combination is rejected.

### GKEComputeClassPlacementPolicy

```yaml
apiVersion: workloads.gke.io/v1
kind: GKEComputeClassPlacementPolicy
metadata:
  name: performance-split
spec:
  preferredComputeClass: performance-od   # e.g. priorities c3 -> n2, activeMigration on
  defaultComputeClass: standard-od        # optional; unset = "default" means any node outside the preferred class
```

Fallback lives in the compute classes: list the acceptable fallback families in each class's `priorities`, and set
`activeMigration.optimizeRulePriority: true` to move pods back when the better family returns.

### Guardrail entry

```yaml
pluginConstraints:
  placement:
    - name: gke-computeclass-placement
      parameters:
        allowedPreferredComputeClasses: [performance-od]   # unset = any
        allowedDefaultComputeClasses: [standard-od]        # unset = any
        maxPreferredRatio: "50%"                            # percent only
```

A compute class can provision Spot (`spot: true` priorities). The plugin reads the referenced classes: if any priority
is Spot, the policy is also subject to `gke-spot-placement.enforcementMode`, so `Forbidden` can't be bypassed through a
Spot compute class.

## Controller behaviour

| Area | Behaviour |
|---|---|
| Routing | Only the new plugin handles `type: ComputeClass`; the Spot plugin ignores it. |
| Mutation, preferred side | nodeSelector `cloud.google.com/compute-class: <preferred>` and the matching toleration. No Spot or machine-family terms (constraint 2). |
| Mutation, default side | nodeSelector and toleration for `<default>` if set; otherwise unchanged. |
| Namespace or cluster default class | GKE stamps it on every pod before the plugin runs. The plugin overwrites a selector that equals that default and adds the toleration for its own class (GKE's toleration for the default class stays; this shape was admitted in a dry run). Limitation: a user who explicitly selects the default class can't be told apart and is overwritten too. How the plugin discovers the cluster-level default class is an open question. |
| Pod selects another class | A selector that is neither the default nor set by the plugin is treated as user intent: left unchanged, counted on neither side, condition `ForeignComputeClassPods` set. |
| Accounting | A pod is preferred if its node's `cloud.google.com/compute-class` is the preferred class, or it is Pending with that selector. Known limits carried over: burst admissions can overshoot by about one pod (cache lag), and rolling updates count old pods; replacing pods one at a time lands on target. |
| Fallback status | Plugin fallback can't happen (constraint 1); `InFallback` is not set for this type. New informational condition `PreferredClassDegraded` when preferred pods run on a node whose machine family is not the class's top priority (N2 instead of C3, or a general-purpose machine if the class uses `whenUnsatisfiable: ScaleUpAnyway`). Not observed in the demo yet. |
| Preferred-class stockout | The preferred side behaves like `Fail`: if no priority can be provisioned and the class uses `DoNotScaleUp`, preferred pods stay Pending and still count as preferred. The class's `whenUnsatisfiable` decides; this trade-off is accepted in exchange for GKE-native fallback. |
| Reversion | Delegated to compute-class `activeMigration`; the plugin keeps no reversion settings. Assumption (from internal ComputeClass material, not observed): activeMigration respects PDBs. WorkloadClass PDBs are closed outside disruption windows and open to 100% inside them, so migration should happen only inside windows, possibly for all preferred pods at once. |
| Status | Neutral reason `TargetMet`; printer column `PREFERRED` showing `preferredRatio`. |

## Guardrail precedence

| Guardrail setting | Effect on `type: ComputeClass` |
|---|---|
| `gke-spot-placement.enforcementMode: Required` | rejects it unless the preferred class has Spot priorities |
| `gke-spot-placement.enforcementMode: Forbidden` | rejects it if either referenced class has a Spot priority; no effect otherwise |
| `gke-spot-placement.minSpotRatio` | no effect (`preferredRatio` is not a Spot ratio) |
| several `gke-computeclass-placement` entries | minimum `maxPreferredRatio`, intersection of `allowedPreferredComputeClasses` |
| `maxPreferredRatio` above 100% | rejected by the guardrail webhook |

The core controller validates `preferredRatio` against `maxPreferredRatio` (portable limit, as with `minSpotRatio`);
the plugin validates `allowedPreferredComputeClasses` against the policy.

Accepted trade-off: the Spot plugin's fallback governance (`maxFallbackRatio`, `maxFallbackDuration`,
`requiredReversionAction`) has no equivalent here; platform teams govern fallback through the compute classes they
publish and through `allowedPreferredComputeClasses`.

## Alternatives considered

1. **Keep the Spot plugin plus patch 2's annotation.** Works today, but the YAML says `type: Spot` and the default side
   can't use a compute class.
2. **Generic tiers** (`capacityStrategy.tiers: [{name: preferred, share: 50%}, {name: default}]`). Portable and would
   generalise `spotRatio`, but it is a larger API change; worth it if more than two tiers are needed.
3. **Two Deployments, one per class.** No controller work, but the split is fixed and outside WorkloadClass and
   guardrails.

## Rollout

1. Land patch 1 (toleration); it is needed for any compute class used through the Spot plugin.
2. Offer patch 2's annotation as experimental.
3. Implement this API. Test gate: Warden dry-run matrix for every mutation shape, and split tests for gradual
   scale-up, burst and pod deletion.
4. Migration for annotation users: `type: Spot` plus annotation becomes `type: ComputeClass` with the same ratio.
   `action: FallbackToOnDemand` and the policy's `reversion` move into the class's priorities and `activeMigration`;
   the migration tool fails on non-default values instead of dropping them. If step 3 is planned soon, patch 2 can
   stay local rather than ship upstream.

## Open questions

- Naming: `type: ComputeClass` and the `computeClass:` sub-struct use GKE vocabulary in the portable layer. A neutral
  `type: Preferred` with `preferred: {ratio: "50%"}` would match the API's portability goal; either way the Go enum
  `SpotPlacementType` becomes misnamed.
- Should `defaultComputeClass` be required, so default-side pods never depend on a namespace default?
