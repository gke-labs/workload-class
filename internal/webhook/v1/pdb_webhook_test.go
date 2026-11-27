/*
Copyright 2026.

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

package v1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func newPDBWebhookTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(clientgo) failed: %v", err)
	}
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(workloads) failed: %v", err)
	}
	return scheme
}

func makeUserPDBGuardrail(name string, p *workloadsv1.UserPDBPolicy) *workloadsv1.WorkloadClassGuardrail {
	return &workloadsv1.WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: workloadsv1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1.Constraints{
				Disruption: workloadsv1.Disruption{MaxNonDisruptionDurationDays: 7, UserPDBPolicy: p},
			},
		},
	}
}

func TestPodDisruptionBudgetValidator_Create(t *testing.T) {
	scheme := newPDBWebhookTestScheme(t)
	zero, one := intstr.FromInt32(0), intstr.FromInt32(1)
	isController, yes, no := true, true, false

	webPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "default", Labels: map[string]string{"app": "web"}}}
	payPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pay-0", Namespace: "default", Labels: map[string]string{"team": "payments"}}}
	existingPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-pdb", Namespace: "default"},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
	}

	tests := []struct {
		name         string
		objects      []client.Object
		pdb          *policyv1.PodDisruptionBudget
		wantErr      bool
		wantWarnings int
		wantSubstr   string
	}{
		{
			name:    "allow_mode_permits_zero_disruption",
			objects: []client.Object{makeUserPDBGuardrail("g-allow", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeAllow})},
			pdb:     &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "strict", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero}},
		},
		{
			name:    "warn_mode_admits_with_warnings",
			objects: []client.Object{makeUserPDBGuardrail("g-warn", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeWarn}), existingPDB, webPod},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "strict-overlap", Namespace: "default"},
				Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
			},
			wantWarnings: 2, wantSubstr: "existing-pdb",
		},
		{
			name:    "enforce_mode_rejects_zero_disruption",
			objects: []client.Object{makeUserPDBGuardrail("g-enf", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce})},
			pdb:     &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "strict", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero}},
			wantErr: true, wantSubstr: "zero voluntary evictions",
		},
		{
			name:    "enforce_mode_rejects_co_selected_pod_overlap",
			objects: []client.Object{makeUserPDBGuardrail("g-enf", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce}), existingPDB, webPod},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "overlap", Namespace: "default"},
				Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
			},
			wantErr: true, wantSubstr: "'existing-pdb'",
		},
		{
			name:    "enforce_mode_allows_disjoint_keys_on_separate_pods",
			objects: []client.Object{makeUserPDBGuardrail("g-enf", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce}), existingPDB, webPod, payPod},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "pay-pdb", Namespace: "default"},
				Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "payments"}}},
			},
		},
		{
			name: "mixed_guardrails_warn_zero_without_enforcing_it",
			objects: []client.Object{
				makeUserPDBGuardrail("g-warn", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeWarn, BlockZeroDisruption: &yes, BlockOverlapping: &no}),
				makeUserPDBGuardrail("g-enf", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce, BlockZeroDisruption: &no, BlockOverlapping: &yes}),
			},
			pdb:          &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "strict", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero}},
			wantWarnings: 1, wantSubstr: "zero voluntary evictions",
		},
		{
			name:    "enforce_skips_workloadclass_managed_pdb",
			objects: []client.Object{makeUserPDBGuardrail("g-enf", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce})},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name: "workload-pod-1", Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "workloads.gke.io/v1", Kind: "WorkloadClass", Name: "wc", Controller: &isController}},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.objects...).Build()
			v := &PodDisruptionBudgetCustomValidator{Client: c}
			warns, err := v.ValidateCreate(context.Background(), tt.pdb)
			if (err != nil) != tt.wantErr || len(warns) != tt.wantWarnings {
				t.Fatalf("ValidateCreate() = (warns=%v, err=%v), wantErr=%v wantWarnings=%d", warns, err, tt.wantErr, tt.wantWarnings)
			}
			if tt.wantSubstr != "" {
				combined := strings.Join(warns, "; ")
				if err != nil {
					combined += err.Error()
				}
				if !strings.Contains(combined, tt.wantSubstr) {
					t.Errorf("output %q missing %q", combined, tt.wantSubstr)
				}
			}
			if dw, de := v.ValidateDelete(context.Background(), tt.pdb); de != nil || len(dw) != 0 {
				t.Errorf("ValidateDelete() = (%v, %v), want nil", dw, de)
			}
		})
	}
}

func TestPodDisruptionBudgetValidator_Update(t *testing.T) {
	scheme := newPDBWebhookTestScheme(t)
	zero, one := intstr.FromInt32(0), intstr.FromInt32(1)
	now := metav1.Now()

	enforce := makeUserPDBGuardrail("g-enf", &workloadsv1.UserPDBPolicy{Mode: workloadsv1.UserPDBModeEnforce})
	webPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "default", Labels: map[string]string{"app": "web", "role": "api"}}}
	existingWeb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-web", Namespace: "default"},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
	}
	existingRole := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-role", Namespace: "default"},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "api"}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(enforce, webPod, existingWeb, existingRole).Build()
	v := &PodDisruptionBudgetCustomValidator{Client: c}

	preExistingZero := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "default", Finalizers: []string{"test/finalizer"}},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
	}

	// (a) Metadata-only update of a pre-existing violating PDB is allowed with warnings.
	metaOnly := preExistingZero.DeepCopy()
	metaOnly.Annotations = map[string]string{"gitops/sync": "1"}
	if w, err := v.ValidateUpdate(context.Background(), preExistingZero, metaOnly); err != nil || len(w) != 2 {
		t.Fatalf("metadata-only update of legacy PDB = (warns=%v, err=%v), want 2 warns and nil err", w, err)
	}

	// (b) Finalizer removal while DeletionTimestamp is set is skipped completely.
	terminating := preExistingZero.DeepCopy()
	terminating.DeletionTimestamp = &now
	terminating.Finalizers = nil
	if w, err := v.ValidateUpdate(context.Background(), preExistingZero, terminating); err != nil || len(w) != 0 {
		t.Fatalf("terminating PDB update = (warns=%v, err=%v), want no warns and nil err", w, err)
	}

	// (c) Updating spec from compliant maxUnavailable:1 to 0 introduces a new violation and is rejected.
	oldGood := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "good", Namespace: "default"},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}},
	}
	newZero := oldGood.DeepCopy()
	newZero.Spec.MaxUnavailable = &zero
	if _, err := v.ValidateUpdate(context.Background(), oldGood, newZero); err == nil || !strings.Contains(err.Error(), "zero voluntary evictions") {
		t.Fatalf("introducing maxUnavailable:0 should be rejected, got err=%v", err)
	}

	// (d) Spec update widening selector to co-select a Pod with existing PDBs is rejected.
	newOverlap := oldGood.DeepCopy()
	newOverlap.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}
	if _, err := v.ValidateUpdate(context.Background(), oldGood, newOverlap); err == nil || !strings.Contains(err.Error(), "'existing-web'") {
		t.Fatalf("introducing new overlap should be rejected, got err=%v", err)
	}

	// (e) Spec change that keeps pre-existing maxUnavailable:0 and removes overlap warns without rejecting.
	preZeroKeep := preExistingZero.DeepCopy()
	preZeroKeep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}
	if w, err := v.ValidateUpdate(context.Background(), preExistingZero, preZeroKeep); err != nil || len(w) != 1 {
		t.Fatalf("narrowing selector while keeping legacy maxUnavailable:0 = (warns=%v, err=%v), want 1 warn and nil err", w, err)
	}
}
