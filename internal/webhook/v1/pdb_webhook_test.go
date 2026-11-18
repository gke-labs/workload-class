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

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestPodDisruptionBudgetValidator(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(clientgo) failed: %v", err)
	}
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(workloads) failed: %v", err)
	}

	zero := intstr.FromInt32(0)
	one := intstr.FromInt32(1)
	isController := true

	guardrail := func(mode workloadsv1.UserPDBEnforcementMode) *workloadsv1.WorkloadClassGuardrail {
		return &workloadsv1.WorkloadClassGuardrail{
			ObjectMeta: metav1.ObjectMeta{Name: "g-" + strings.ToLower(string(mode))},
			Spec: workloadsv1.WorkloadClassGuardrailSpec{
				Constraints: workloadsv1.Constraints{
					Disruption: workloadsv1.Disruption{
						MaxNonDisruptionDurationDays: 7,
						UserPDBPolicy:                &workloadsv1.UserPDBPolicy{Mode: mode},
					},
				},
			},
		}
	}

	existingPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-pdb", Namespace: "default"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &one,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
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
			objects: []client.Object{guardrail(workloadsv1.UserPDBModeAllow)},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "strict", Namespace: "default"},
				Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero},
			},
			wantErr: false,
		},
		{
			name:    "warn_mode_admits_with_warnings",
			objects: []client.Object{guardrail(workloadsv1.UserPDBModeWarn), existingPDB},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "strict-overlap", Namespace: "default"},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: &zero,
					Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				},
			},
			wantErr:      false,
			wantWarnings: 2,
			wantSubstr:   "existing-pdb",
		},
		{
			name:    "enforce_mode_rejects_zero_disruption",
			objects: []client.Object{guardrail(workloadsv1.UserPDBModeEnforce)},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "strict", Namespace: "default"},
				Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero},
			},
			wantErr:    true,
			wantSubstr: "zero voluntary evictions",
		},
		{
			name:    "enforce_mode_rejects_overlapping_user_pdb",
			objects: []client.Object{guardrail(workloadsv1.UserPDBModeEnforce), existingPDB},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "overlap", Namespace: "default"},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: &one,
					Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				},
			},
			wantErr:    true,
			wantSubstr: "'existing-pdb'",
		},
		{
			name:    "enforce_mode_admits_compliant_non_overlapping_pdb",
			objects: []client.Object{guardrail(workloadsv1.UserPDBModeEnforce), existingPDB},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "other-app", Namespace: "default"},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: &one,
					Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
				},
			},
			wantErr: false,
		},
		{
			name:    "enforce_mode_always_skips_workloadclass_managed_pdb",
			objects: []client.Object{guardrail(workloadsv1.UserPDBModeEnforce)},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-pod-1",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{APIVersion: "workloads.gke.io/v1", Kind: "WorkloadClass", Name: "wc", Controller: &isController},
					},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &zero},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.objects...).Build()
			v := &PodDisruptionBudgetCustomValidator{Client: c}

			warnings, err := v.ValidateCreate(context.Background(), tt.pdb)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateCreate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(warnings) != tt.wantWarnings {
				t.Fatalf("ValidateCreate() warnings = %v (len %d), want len %d", warnings, len(warnings), tt.wantWarnings)
			}
			if tt.wantSubstr != "" {
				combined := strings.Join(warnings, "; ")
				if err != nil {
					combined += err.Error()
				}
				if !strings.Contains(combined, tt.wantSubstr) {
					t.Errorf("ValidateCreate() output %q does not contain %q", combined, tt.wantSubstr)
				}
			}

			// ValidateUpdate should produce the same outcome and ignore self-overlap.
			updateWarnings, updateErr := v.ValidateUpdate(context.Background(), tt.pdb, tt.pdb)
			if (updateErr != nil) != tt.wantErr || len(updateWarnings) != tt.wantWarnings {
				t.Errorf("ValidateUpdate() = (%v, %v), wantErr %v, wantWarnings %d", updateWarnings, updateErr, tt.wantErr, tt.wantWarnings)
			}

			if delWarnings, delErr := v.ValidateDelete(context.Background(), tt.pdb); delErr != nil || len(delWarnings) != 0 {
				t.Errorf("ValidateDelete() = (%v, %v), want (nil, nil)", delWarnings, delErr)
			}
		})
	}
}
