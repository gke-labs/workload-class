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

package utils

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestSyncPDBWithWorkloadClass(t *testing.T) {
	tests := []struct {
		name               string
		wc                 *workloadsv1.WorkloadClass
		pdb                *policyv1.PodDisruptionBudget
		wantErr            bool
		wantMaxUnavailable *intstr.IntOrString
		wantUnhealthyEvict *policyv1.UnhealthyPodEvictionPolicyType
	}{
		{
			name:    "workload_class_is_nil",
			wc:      nil,
			pdb:     &policyv1.PodDisruptionBudget{},
			wantErr: true,
		},
		{
			name: "maintenance_readiness_ready",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				},
				Status: workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: workloadsv1.ReadinessReady,
				},
			},
			pdb:                &policyv1.PodDisruptionBudget{},
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"},
			wantUnhealthyEvict: func() *policyv1.UnhealthyPodEvictionPolicyType {
				policy := policyv1.IfHealthyBudget
				return &policy
			}(),
		},
		{
			name: "maintenance_readiness_not_ready",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				},
				Status: workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: workloadsv1.ReadinessNotReady,
				},
			},
			pdb:                &policyv1.PodDisruptionBudget{},
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 0},
			wantUnhealthyEvict: func() *policyv1.UnhealthyPodEvictionPolicyType {
				policy := policyv1.IfHealthyBudget
				return &policy
			}(),
		},
		{
			name: "maintenance_readiness_overdue",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				},
				Status: workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: workloadsv1.ReadinessOverdue,
				},
			},
			pdb:                &policyv1.PodDisruptionBudget{},
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"},
			wantUnhealthyEvict: func() *policyv1.UnhealthyPodEvictionPolicyType {
				policy := policyv1.IfHealthyBudget
				return &policy
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SyncPDBWithWorkloadClass(tt.wc, tt.pdb)
			if (err != nil) != tt.wantErr {
				t.Errorf("SyncPDBWithWorkloadClass() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(tt.pdb.Spec.MaxUnavailable, tt.wantMaxUnavailable) {
					t.Errorf("SyncPDBWithWorkloadClass() MaxUnavailable = %v, want %v", tt.pdb.Spec.MaxUnavailable, tt.wantMaxUnavailable)
				}
				if !reflect.DeepEqual(tt.pdb.Spec.UnhealthyPodEvictionPolicy, tt.wantUnhealthyEvict) {
					t.Errorf("SyncPDBWithWorkloadClass() UnhealthyPodEvictionPolicy = %v, want %v", tt.pdb.Spec.UnhealthyPodEvictionPolicy, tt.wantUnhealthyEvict)
				}
				if !reflect.DeepEqual(tt.pdb.Spec.Selector, tt.wc.Spec.PodSelector) {
					t.Errorf("SyncPDBWithWorkloadClass() Selector = %v, want %v", tt.pdb.Spec.Selector, tt.wc.Spec.PodSelector)
				}
			}
		})
	}
}

func TestAllowLease(t *testing.T) {
	tests := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want bool
	}{
		{
			name: "pdb_is_nil",
			pdb:  nil,
			want: true,
		},
		{
			name: "no_annotations",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{},
			},
			want: true,
		},
		{
			name: "missing_bypass_annotation",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BypassExpiration: time.Now().Add(time.Hour).Format(ExpirationFormat),
					},
				},
			},
			want: true,
		},
		{
			name: "missing_expiration_annotation",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BypassPod: "test-pod",
					},
				},
			},
			want: true,
		},
		{
			name: "invalid_expiration_format",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BypassPod:        "test-pod",
						BypassExpiration: "not-a-valid-time",
					},
				},
			},
			want: true,
		},
		{
			name: "expired_lease",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BypassPod:        "test-pod",
						BypassExpiration: time.Now().Add(-1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			want: true,
		},
		{
			name: "valid_active_lease",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BypassPod:        "test-pod",
						BypassExpiration: time.Now().Add(1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AllowLease(tt.pdb); got != tt.want {
				t.Errorf("AllowLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPDBWithLease(t *testing.T) {
	tests := []struct {
		name    string
		wc      *workloadsv1.WorkloadClass
		pdb     *policyv1.PodDisruptionBudget
		pod     *corev1.Pod
		wantErr bool
	}{
		{
			name:    "workload_class_is_nil",
			wc:      nil,
			pdb:     &policyv1.PodDisruptionBudget{},
			pod:     &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}},
			wantErr: true,
		},
		{
			name: "successfully_configures_pdb",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				},
			},
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"existing": "annotation"},
				},
			},
			pod:     &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var c client.Client // nil client is fine since it's unused in the func body

			s := workloadsv1.Subject{Kind: "ServiceAccount", Name: "sam", Namespace: "system"}
			err := PDBWithLease(ctx, c, tt.pdb, tt.wc, tt.pod, s)
			if (err != nil) != tt.wantErr {
				t.Errorf("PDBWithLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// 1. Selector should match wc
				if !reflect.DeepEqual(tt.pdb.Spec.Selector, tt.wc.Spec.PodSelector) {
					t.Errorf("PDBWithLease() Selector = %v, want %v", tt.pdb.Spec.Selector, tt.wc.Spec.PodSelector)
				}
				// 2. UnhealthyPodEvictionPolicy should be IfHealthyBudget
				wantPolicy := policyv1.IfHealthyBudget
				if tt.pdb.Spec.UnhealthyPodEvictionPolicy == nil || *tt.pdb.Spec.UnhealthyPodEvictionPolicy != wantPolicy {
					t.Errorf("PDBWithLease() UnhealthyPodEvictionPolicy = %v, want %v", tt.pdb.Spec.UnhealthyPodEvictionPolicy, wantPolicy)
				}
				// 3. MaxUnavailable should be open (100%)
				wantMaxUnavailable := &intstr.IntOrString{Type: intstr.String, StrVal: "100%"}
				if !reflect.DeepEqual(tt.pdb.Spec.MaxUnavailable, wantMaxUnavailable) {
					t.Errorf("PDBWithLease() MaxUnavailable = %v, want %v", tt.pdb.Spec.MaxUnavailable, wantMaxUnavailable)
				}
				// 4. Annotations should include BypassPod and BypassExpiration
				if tt.pdb.Annotations[BypassOwner] != BypassOwnerValue(s) {
					t.Errorf("PDBWithLease() BypassPod annotation = %v, want %v", tt.pdb.Annotations[BypassOwner], BypassOwnerValue(s))
				}

				if tt.pdb.Annotations[BypassPod] != tt.pod.Name {
					t.Errorf("PDBWithLease() BypassPod annotation = %v, want %v", tt.pdb.Annotations[BypassPod], tt.pod.Name)
				}

				expirationStr, ok := tt.pdb.Annotations[BypassExpiration]
				if !ok {
					t.Errorf("PDBWithLease() BypassExpiration annotation is missing")
				}
				if _, err := time.Parse(ExpirationFormat, expirationStr); err != nil {
					t.Errorf("PDBWithLease() BypassExpiration annotation is not properly formatted: %v", err)
				}

				// 5. Existing annotations shouldn't be wiped out
				if tt.pdb.Annotations["existing"] != "annotation" {
					t.Errorf("PDBWithLease() failed to preserve existing annotations")
				}
			}
		})
	}
}
