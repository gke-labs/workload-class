---
name: review-docs-drift
description: Verify that code changes, CLI flags, labels/annotations, CRD fields, manifests, and invariants stay synchronized with canonical repository documentation in gke-labs/workload-class.
---

# Documentation Drift Pre-PR Review (`review-docs-drift`)

Run this skill in a fresh agent context before opening or updating a PR.

## Workflow

1. **Identify Changed Surfaces:**
   ```bash
   git diff --name-only origin/main...HEAD
   ```
   *(Use `upstream/main...HEAD` on a fork, or `<parent-branch>...HEAD` when reviewing a stacked PR.)*
   Check if the diff adds, renames, or alters controller CLI flags, CRD fields, status conditions, events, labels/annotations, RBAC/webhook markers, or safety invariants.

2. **Cross-Check Canonical Docs & Generated Artifacts:**

   | Code Modification Area | Key Source Files | Canonical Docs & Artifacts to Verify |
   | :--- | :--- | :--- |
   | **CLI Flags & Manager Wiring** | `cmd/main.go` | `README.md`, `AGENTS.md` |
   | **CRD Schemas & Markers** | `api/v1/*_types.go` | `README.md`, `config/samples/`, `config/crd/bases/` (`make manifests generate`), `dist/install.yaml` (`make build-installer`), `dist/chart/` |
   | **Labels, Annotations & Conditions** | `api/v1/`, `internal/{controller,webhook,utils}/` | `README.md` (precedence, Per-Pod PDBs, Spot placement sections), `docs/invariants.md` |
   | **Safety Invariants (`I1`–`I5`)** | `internal/invariants/` | `docs/invariants.md` |

3. **Record Result:**
   Document what was cross-checked and any documentation or manifest updates applied under `## Self-Review` -> `Docs-drift pass (review-docs-drift)` in the PR body.
