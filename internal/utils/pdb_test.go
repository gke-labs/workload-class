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
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestPDBNameHelpers(t *testing.T) {
	if got := PDBName("my-pod"); got != "workload-my-pod" {
		t.Errorf("PDBName(\"my-pod\") = %q, want \"workload-my-pod\"", got)
	}
	if got := PodNameFromPDBName("workload-my-pod"); got != "my-pod" {
		t.Errorf("PodNameFromPDBName(\"workload-my-pod\") = %q, want \"my-pod\"", got)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "team-a",
			UID:       types.UID("uid-123"),
		},
	}
	base := PDBBase(pod)
	if base.Name != "workload-pod-1" || base.Namespace != "team-a" || base.Labels[PodUIDLabel] != "uid-123" {
		t.Errorf("PDBBase(pod) = %+v, want Name=workload-pod-1, Namespace=team-a, Labels[%s]=uid-123", base, PodUIDLabel)
	}

	// Pod names up to 244 characters keep the exact "workload-<podName>" name (253 chars).
	exactMaxPod := strings.Repeat("a", 244)
	if got := PDBName(exactMaxPod); len(got) != MaxPDBNameLength || got != PDBNamePrefix+exactMaxPod {
		t.Errorf("PDBName(244-char pod) len = %d, want %d", len(got), MaxPDBNameLength)
	}

	// Pod names > 244 characters are truncated with a deterministic hash suffix to stay <= 253 characters.
	longPodA := strings.Repeat("a", 250) + "-1"
	longPodB := strings.Repeat("a", 250) + "-2"
	nameA := PDBName(longPodA)
	nameB := PDBName(longPodB)
	if len(nameA) > MaxPDBNameLength {
		t.Errorf("PDBName(longPodA) len = %d, want <= %d", len(nameA), MaxPDBNameLength)
	}
	if nameA == nameB {
		t.Errorf("PDBName(longPodA) == PDBName(longPodB) (%q), want distinct names", nameA)
	}
	if PDBName(longPodA) != nameA {
		t.Errorf("PDBName(longPodA) is not deterministic")
	}
}

func TestSyncPDBWithWorkloadClass(t *testing.T) {
	testPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       types.UID("pod-uid-1"),
		},
	}
	wantSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			PodUIDLabel: "pod-uid-1",
		},
	}
	ifHealthy := policyv1.IfHealthyBudget

	tests := []struct {
		name               string
		wc                 *workloadsv1.WorkloadClass
		pod                *corev1.Pod
		pdb                *policyv1.PodDisruptionBudget
		isValid            bool
		wantErr            bool
		wantMaxUnavailable *intstr.IntOrString
		wantUnhealthyEvict *policyv1.UnhealthyPodEvictionPolicyType
	}{
		{
			name:    "workload_class_is_nil",
			wc:      nil,
			pod:     testPod,
			pdb:     &policyv1.PodDisruptionBudget{},
			isValid: true,
			wantErr: true,
		},
		{
			name: "pod_is_nil",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
			},
			pod:     nil,
			pdb:     &policyv1.PodDisruptionBudget{},
			isValid: true,
			wantErr: true,
		},
		{
			name: "pdb_is_nil",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
			},
			pod:     testPod,
			pdb:     nil,
			isValid: true,
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
			pod:                testPod,
			pdb:                &policyv1.PodDisruptionBudget{},
			isValid:            true,
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"},
			wantUnhealthyEvict: &ifHealthy,
		},
		{
			name: "maintenance_readiness_not_ready_clears_lease_annotations",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				},
				Status: workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: workloadsv1.ReadinessNotReady,
				},
			},
			pod: testPod,
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BypassOwner:      "user-a",
						BypassExpiration: time.Now().Add(time.Minute).Format(ExpirationFormat),
						"custom":         "keep",
					},
				},
			},
			isValid:            true,
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 0},
			wantUnhealthyEvict: &ifHealthy,
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
			pod:                testPod,
			pdb:                &policyv1.PodDisruptionBudget{},
			isValid:            true,
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"},
			wantUnhealthyEvict: &ifHealthy,
		},
		{
			name: "invalid_workload_class_opens_pdb_to_100_percent",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wc", Namespace: "default"},
				Status: workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: workloadsv1.ReadinessNotReady,
				},
			},
			pod:                testPod,
			pdb:                &policyv1.PodDisruptionBudget{},
			isValid:            false,
			wantErr:            false,
			wantMaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"},
			wantUnhealthyEvict: &ifHealthy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SyncPDBWithWorkloadClass(tt.wc, tt.pod, tt.pdb, tt.isValid)
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
				if !reflect.DeepEqual(tt.pdb.Spec.Selector, wantSelector) {
					t.Errorf("SyncPDBWithWorkloadClass() Selector = %v, want %v", tt.pdb.Spec.Selector, wantSelector)
				}
				if tt.pdb.Labels[PodUIDLabel] != string(tt.pod.UID) {
					t.Errorf("SyncPDBWithWorkloadClass() Labels[%s] = %q, want %q", PodUIDLabel, tt.pdb.Labels[PodUIDLabel], tt.pod.UID)
				}
				if _, ok := tt.pdb.Annotations[BypassOwner]; ok {
					t.Errorf("SyncPDBWithWorkloadClass() did not clear %s annotation", BypassOwner)
				}
				if _, ok := tt.pdb.Annotations[BypassExpiration]; ok {
					t.Errorf("SyncPDBWithWorkloadClass() did not clear %s annotation", BypassExpiration)
				}
			}
		})
	}
}

func TestAllowLeaseAndSubjectAlreadyLeasing(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       types.UID("pod-uid-1"),
		},
	}
	isController := true
	wcOwnerRefs := []metav1.OwnerReference{
		{
			APIVersion: "workloads.gke.io/v1",
			Kind:       "WorkloadClass",
			Name:       "test-wc",
			UID:        types.UID("wc-uid-1"),
			Controller: &isController,
		},
	}
	subjectA := workloadsv1.Subject{Kind: "User", Name: "user-a"}
	subjectB := workloadsv1.Subject{Kind: "ServiceAccount", Name: "vpa-recommender", Namespace: "kube-system"}

	tests := []struct {
		name               string
		pdb                *policyv1.PodDisruptionBudget
		pod                *corev1.Pod
		wantAllowLease     bool
		wantSubjectALeased bool
		wantSubjectBLeased bool
	}{
		{
			name:           "pdb_is_nil",
			pdb:            nil,
			pod:            pod,
			wantAllowLease: false,
		},
		{
			name: "pod_is_nil",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
				},
			},
			pod:            nil,
			wantAllowLease: false,
		},
		{
			name: "pdb_not_owned_by_workloadclass_disallows_lease",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-test-pod",
					Namespace: "default",
				},
			},
			pod:            pod,
			wantAllowLease: false,
		},
		{
			name: "pdb_owned_by_non_workloadclass_kind_disallows_lease",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload-test-pod",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "Deployment",
							Name:       "my-deploy",
							Controller: &isController,
						},
					},
				},
			},
			pod:            pod,
			wantAllowLease: false,
		},
		{
			name: "no_annotations_on_wc_owned_pdb",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
				},
			},
			pod:            pod,
			wantAllowLease: true,
		},
		{
			name: "missing_bypass_owner_annotation",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassExpiration: time.Now().Add(time.Hour).Format(ExpirationFormat),
					},
				},
			},
			pod:            pod,
			wantAllowLease: true,
		},
		{
			name: "missing_expiration_annotation",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassOwner: "user-a",
					},
				},
			},
			pod:            pod,
			wantAllowLease: true,
		},
		{
			name: "invalid_expiration_format",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassOwner:      "user-a",
						BypassExpiration: "not-a-valid-time",
					},
				},
			},
			pod:            pod,
			wantAllowLease: true,
		},
		{
			name: "expired_lease",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassOwner:      "user-a",
						BypassExpiration: time.Now().Add(-1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			pod:            pod,
			wantAllowLease: true,
		},
		{
			name: "stale_pod_uid_allows_new_lease",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "old-pod-uid"},
					Annotations: map[string]string{
						BypassOwner:      "user-a",
						BypassExpiration: time.Now().Add(1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			pod:            pod,
			wantAllowLease: true,
		},
		{
			name: "valid_active_lease_by_subject_a",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassOwner:      "user:user-a",
						BypassExpiration: time.Now().Add(1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			pod:                pod,
			wantAllowLease:     false,
			wantSubjectALeased: true,
			wantSubjectBLeased: false,
		},
		{
			name: "group_with_same_name_as_user_does_not_match_user_subject",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassOwner:      BypassOwnerValue(workloadsv1.Subject{Kind: "Group", Name: "user-a"}),
						BypassExpiration: time.Now().Add(1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			pod:                pod,
			wantAllowLease:     false,
			wantSubjectALeased: false,
			wantSubjectBLeased: false,
		},
		{
			name: "valid_active_lease_by_service_account_subject_b",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: wcOwnerRefs,
					Labels:          map[string]string{PodUIDLabel: "pod-uid-1"},
					Annotations: map[string]string{
						BypassOwner:      "system:serviceaccount:kube-system:vpa-recommender",
						BypassExpiration: time.Now().Add(1 * time.Hour).Format(ExpirationFormat),
					},
				},
			},
			pod:                pod,
			wantAllowLease:     false,
			wantSubjectALeased: false,
			wantSubjectBLeased: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AllowLease(tt.pdb, tt.pod); got != tt.wantAllowLease {
				t.Errorf("AllowLease() = %v, want %v", got, tt.wantAllowLease)
			}
			if got := SubjectAlreadyLeasing(tt.pdb, tt.pod, subjectA); got != tt.wantSubjectALeased {
				t.Errorf("SubjectAlreadyLeasing(subjectA) = %v, want %v", got, tt.wantSubjectALeased)
			}
			if got := SubjectAlreadyLeasing(tt.pdb, tt.pod, subjectB); got != tt.wantSubjectBLeased {
				t.Errorf("SubjectAlreadyLeasing(subjectB) = %v, want %v", got, tt.wantSubjectBLeased)
			}
		})
	}
}

func TestPDBWithLease(t *testing.T) {
	customGrace := int64(90)
	subject := workloadsv1.Subject{
		Kind:      "ServiceAccount",
		Name:      "cluster-autoscaler",
		Namespace: "kube-system",
	}

	tests := []struct {
		name              string
		pdb               *policyv1.PodDisruptionBudget
		pod               *corev1.Pod
		wantErr           bool
		wantLeaseDuration time.Duration
	}{
		{
			name:    "pdb_is_nil",
			pdb:     nil,
			pod:     &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", UID: "uid-1"}},
			wantErr: true,
		},
		{
			name:    "pod_is_nil",
			pdb:     &policyv1.PodDisruptionBudget{},
			pod:     nil,
			wantErr: true,
		},
		{
			name: "default_30s_lease_duration_when_grace_period_unset",
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"existing": "annotation"},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					UID:       types.UID("uid-1"),
				},
			},
			wantErr:           false,
			wantLeaseDuration: DefaultLeaseDuration,
		},
		{
			name: "uses_pod_termination_grace_period_seconds",
			pdb:  &policyv1.PodDisruptionBudget{},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-grace",
					Namespace: "default",
					UID:       types.UID("uid-2"),
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &customGrace,
				},
			},
			wantErr:           false,
			wantLeaseDuration: 90 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := time.Now()
			err := PDBWithLease(tt.pdb, tt.pod, subject)
			if (err != nil) != tt.wantErr {
				t.Errorf("PDBWithLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				wantSelector := &metav1.LabelSelector{
					MatchLabels: map[string]string{
						PodUIDLabel: string(tt.pod.UID),
					},
				}
				if !reflect.DeepEqual(tt.pdb.Spec.Selector, wantSelector) {
					t.Errorf("PDBWithLease() Selector = %v, want %v", tt.pdb.Spec.Selector, wantSelector)
				}
				if tt.pdb.Labels[PodUIDLabel] != string(tt.pod.UID) {
					t.Errorf("PDBWithLease() Labels[%s] = %q, want %q", PodUIDLabel, tt.pdb.Labels[PodUIDLabel], tt.pod.UID)
				}
				wantPolicy := policyv1.IfHealthyBudget
				if tt.pdb.Spec.UnhealthyPodEvictionPolicy == nil || *tt.pdb.Spec.UnhealthyPodEvictionPolicy != wantPolicy {
					t.Errorf("PDBWithLease() UnhealthyPodEvictionPolicy = %v, want %v", tt.pdb.Spec.UnhealthyPodEvictionPolicy, wantPolicy)
				}
				wantMaxUnavailable := &intstr.IntOrString{Type: intstr.String, StrVal: "100%"}
				if !reflect.DeepEqual(tt.pdb.Spec.MaxUnavailable, wantMaxUnavailable) {
					t.Errorf("PDBWithLease() MaxUnavailable = %v, want %v", tt.pdb.Spec.MaxUnavailable, wantMaxUnavailable)
				}
				wantOwner := "system:serviceaccount:kube-system:cluster-autoscaler"
				if tt.pdb.Annotations[BypassOwner] != wantOwner {
					t.Errorf("PDBWithLease() BypassOwner = %q, want %q", tt.pdb.Annotations[BypassOwner], wantOwner)
				}

				expirationStr, ok := tt.pdb.Annotations[BypassExpiration]
				if !ok {
					t.Fatalf("PDBWithLease() BypassExpiration annotation is missing")
				}
				expTime, err := time.Parse(ExpirationFormat, expirationStr)
				if err != nil {
					t.Fatalf("PDBWithLease() BypassExpiration annotation is not properly formatted: %v", err)
				}
				actualDuration := expTime.Sub(before)
				if actualDuration < tt.wantLeaseDuration-2*time.Second || actualDuration > tt.wantLeaseDuration+5*time.Second {
					t.Errorf("PDBWithLease() lease duration = %v, want ~%v", actualDuration, tt.wantLeaseDuration)
				}
			}
		})
	}
}

func TestUserCreatedPDBHelpers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = workloadsv1.AddToScheme(scheme)

	isController := true
	wcManagedPDB := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workload-pod-a",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "workloads.gke.io/v1",
					Kind:       "WorkloadClass",
					Name:       "my-wc",
					Controller: &isController,
				},
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	userPDB := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "user-web-pdb",
			Namespace: "default",
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}

	if !IsWorkloadClassManagedPDB(&wcManagedPDB) {
		t.Errorf("IsWorkloadClassManagedPDB(wcManagedPDB) = false, want true")
	}
	if IsWorkloadClassManagedPDB(&userPDB) {
		t.Errorf("IsWorkloadClassManagedPDB(userPDB) = true, want false")
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&wcManagedPDB, &userPDB).Build()
	userPDBs, err := ListUserCreatedPDBs(t.Context(), c, "default")
	if err != nil {
		t.Fatalf("ListUserCreatedPDBs() returned unexpected error: %v", err)
	}
	if len(userPDBs) != 1 || userPDBs[0].Name != "user-web-pdb" {
		t.Fatalf("ListUserCreatedPDBs() = %+v, want only [user-web-pdb]", userPDBs)
	}

	matchingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "web", "tier": "frontend"},
		},
	}
	unmatchedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "worker"},
		},
	}

	if !IsPodCoveredByUserPDB(matchingPod, userPDBs) {
		t.Errorf("IsPodCoveredByUserPDB(matchingPod) = false, want true")
	}
	if IsPodCoveredByUserPDB(unmatchedPod, userPDBs) {
		t.Errorf("IsPodCoveredByUserPDB(unmatchedPod) = true, want false")
	}
}

func TestIsPDBOpen(t *testing.T) {
	open := intstr.FromString("100%")
	half := intstr.FromString("50%")
	closed := intstr.FromInt32(0)

	tests := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want bool
	}{
		{name: "nil_pdb", pdb: nil, want: false},
		{name: "nil_max_unavailable", pdb: &policyv1.PodDisruptionBudget{}, want: false},
		{name: "closed", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &closed}}, want: false},
		{name: "partially_open", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &half}}, want: false},
		{name: "open", pdb: &policyv1.PodDisruptionBudget{Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &open}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPDBOpen(tt.pdb); got != tt.want {
				t.Errorf("IsPDBOpen() = %v, want %v", got, tt.want)
			}
		})
	}
}
