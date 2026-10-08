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

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestHandle(t *testing.T) {
	const (
		namespace     = "namespace"
		eviction      = "eviction"
		adminUsername = "admin@mycompany.com"
		podName       = "podrick"
	)
	var (
		inWindowDay        = time.Now().Weekday().String()
		outOfWindowDay     = time.Now().AddDate(0, 0, 1).Weekday().String()
		podNowCreationTime = time.Now()
		labels             = map[string]string{"labelA": "valueA"}
	)

	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
					{Name: "maintenance", DaysOfWeek: []string{inWindowDay}, StartTime: "00:00", EndTime: "23:59", TimeZone: "America/Toronto"},
				},
				MinInitialRunDurationDays:         2,
				MaxNonDisruptionDurationDays:      30,
				AllowedDisruptionsOutsideOfWindow: []workloadsv1.Subject{{Kind: rbacv1.UserKind, Name: adminUsername}},
			},
		},
	}
	wcOutOfWindow := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-out-of-window",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
					{Name: "maintenance", DaysOfWeek: []string{outOfWindowDay}, StartTime: "00:00", EndTime: "23:59", TimeZone: "America/Toronto"},
				},
			},
		},
	}
	wcValFailed := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		},
		Status: workloadsv1.WorkloadClassStatus{
			Conditions: []metav1.Condition{
				{
					Type:   workloadsv1.ConditionTypeValidated,
					Status: metav1.ConditionFalse,
				},
			},
		},
	}

	nsNoAnnotation := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-no-wc",
			Namespace: namespace,
			Annotations: map[string]string{
				"something-else": "true",
			},
		},
	}

	pod := &corev1.Pod{}
	pod.Namespace = namespace
	pod.Labels = labels
	pod.Name = podName

	evictionRequest := admissionv1.AdmissionRequest{
		Name:        podName,
		Namespace:   namespace,
		SubResource: eviction,
		UserInfo: authv1.UserInfo{
			Username: adminUsername,
		},
	}

	evictionRequestNonMatchingUser := admissionv1.AdmissionRequest{
		Name:        podName,
		Namespace:   namespace,
		SubResource: eviction,
		UserInfo: authv1.UserInfo{
			Username: "something-else",
		},
	}

	testCases := []struct {
		name              string
		desc              string
		req               admissionv1.AdmissionRequest
		getPodErr         error
		listWCErr         error
		listWCResp        *workloadsv1.WorkloadClassList
		getWCErr          error
		getWCResp         *workloadsv1.WorkloadClass
		guardrailValFails bool
		emergencyOverride bool
		inWindow          bool
		overdue           bool
		podCreationTime   time.Time
		readiness         workloadsv1.MaintenanceReadiness
		want              admission.Response
	}{
		{
			name:            "errorGettingPod",
			desc:            "Error getting pod, admission Errored",
			req:             evictionRequest,
			getPodErr:       fmt.Errorf("error getting Pod"),
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessReady,
			want:            admission.Errored(http.StatusInternalServerError, fmt.Errorf("error getting Pod")),
		},
		{
			name: "notAnEviction",
			desc: "Not an eviction, admission Allowed",
			req: admissionv1.AdmissionRequest{
				Name:        adminUsername,
				Namespace:   namespace,
				SubResource: "not-an-eviction",
			},
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("Not an eviction"),
		},
		{
			name:            "errorGettingBestMatchWC",
			desc:            "Error getting best match WC, admission Allowed",
			req:             evictionRequest,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			listWCErr:       fmt.Errorf("error listing WorkloadClasses"),
			want:            admission.Allowed("Failed to get WorkloadClass matches this pod or namespace"),
		},
		{
			name:            "bestMatchWCIsNil",
			desc:            "Best match WC is nil, admission Allowed",
			req:             evictionRequest,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("No WorkloadClass matches this pod or namespace"),
		},
		{
			name:            "guardrailValidationFailed",
			desc:            "Guardrail validation failed, admission Allowed",
			req:             evictionRequest,
			podCreationTime: podNowCreationTime,
			getWCResp:       wcValFailed,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("WorkloadClass failed Guardrail validation"),
		},
		{
			name:              "emergencyOverride",
			desc:              "Emergency override, admission Allowed",
			req:               evictionRequest,
			podCreationTime:   podNowCreationTime,
			getWCResp:         wc,
			readiness:         workloadsv1.ReadinessNotReady,
			emergencyOverride: true,
			want:              admission.Allowed("Emergency override active"),
		},
		{
			name:            "allowedUser",
			desc:            "Allowed user, admission Allowed",
			req:             evictionRequest,
			getWCResp:       wc,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("Disruption allowed for authorized user: VPA"),
		},
		{
			name:            "overdue",
			desc:            "Overdue, admission Allowed",
			req:             evictionRequestNonMatchingUser,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessOverdue,
			getWCResp:       wc,
			want:            admission.Allowed("Workload class is overdue for maintenance, bypassing constraints"),
		},
		{
			name:            "notInWindowNotOverdue",
			desc:            "Not in window, not overdue, admission Denied",
			req:             evictionRequestNonMatchingUser,
			podCreationTime: podNowCreationTime,
			getWCResp:       wcOutOfWindow,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", wcOutOfWindow.Name)),
		},
		{
			name:            "podIsTooNew",
			desc:            "Pod is too new, admission Denied",
			req:             evictionRequestNonMatchingUser,
			getWCResp:       wc,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want: admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				time.Since(podNowCreationTime).Round(time.Minute), wc.Spec.DisruptionPolicy.MinInitialRunDurationDays)),
		},
		{
			name:            "inWindowNotOverduePodNotTooNew",
			desc:            "In window, not overdue, Pod not too new, admission Allowed",
			req:             evictionRequestNonMatchingUser,
			readiness:       workloadsv1.ReadinessNotReady,
			podCreationTime: time.Now().AddDate(0, 0, -4),
			want:            admission.Allowed("Eviction allowed by WorkloadClass policy"),
		},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod.CreationTimestamp = metav1.Time{Time: tc.podCreationTime}

			if tc.getWCResp != nil {
				tc.getWCResp.Status = workloadsv1.WorkloadClassStatus{
					MaintenanceReadiness: tc.readiness,
				}
				tc.getWCResp.Spec.DisruptionPolicy.EmergencyOverride = tc.emergencyOverride
			}

			listResp := &workloadsv1.WorkloadClassList{}
			if tc.getWCResp != nil {
				listResp.Items = []workloadsv1.WorkloadClass{*tc.getWCResp}
			}

			client := createClient(nil, tc.getWCErr, tc.listWCErr, tc.getPodErr, nsNoAnnotation, tc.getWCResp, listResp, pod)
			v := &DisruptionWebhook{
				Client: client,
			}

			request := admission.Request{AdmissionRequest: tc.req}
			admissionResponse := v.Handle(ctx, request)

			if admissionResponse.Allowed != tc.want.Allowed {
				t.Errorf("Handle() returned an unexpected response: got %v, want %v", admissionResponse, tc.want)
			}
			// Allowed admissions have no message
			if admissionResponse.Allowed {
				return
			}
			if admissionResponse.Result.Message != tc.want.Result.Message {
				t.Errorf("Handle() returned an unexpected message: got %v, want %v", admissionResponse.Result.Message, tc.want.Result.Message)
			}
		})
	}
}

func TestMatchesUserKind(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1.Subject
		want     bool
		wantErr  bool
	}{
		{
			name: "exact_username_match_without_namespace",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind: "User",
				Name: "jane.doe@example.com",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "usernam_mismatch_without_namespace",
			userInfo: authv1.UserInfo{
				Username: "bob@example.com",
			},
			subject: workloadsv1.Subject{
				Kind: "User",
				Name: "jane.doe@example.com",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "returns_error_if_namespace_is_provided_even_if_username_matches",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind:      "User",
				Name:      "jane.doe@example.com",
				Namespace: "default", // Users are cluster-scoped, so this is invalid
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "returns_error_if_namespace_is_provided_mismatch",
			userInfo: authv1.UserInfo{
				Username: "bob@example.com",
			},
			subject: workloadsv1.Subject{
				Kind:      "User",
				Name:      "jane.doe@example.com",
				Namespace: "kube-system",
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "edge_case_empty_usernames_match",
			userInfo: authv1.UserInfo{
				Username: "",
			},
			subject: workloadsv1.Subject{
				Kind: "User",
				Name: "",
			},
			want:    true,
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesUserKind(tc.userInfo, tc.subject)

			if (err != nil) != tc.wantErr {
				t.Errorf("matchesUserKind() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("matchesUserKind() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesGroupKind(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1.Subject
		want     bool
		wantErr  bool
	}{
		{
			name: "exact_group_match_single_group",
			userInfo: authv1.UserInfo{
				Groups: []string{"system:masters"},
			},
			subject: workloadsv1.Subject{
				Kind: "Group",
				Name: "system:masters",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "exact_group_match_multiple_groups",
			userInfo: authv1.UserInfo{
				Groups: []string{"system:authenticated", "devops@example.com", "developers"},
			},
			subject: workloadsv1.Subject{
				Kind: "Group",
				Name: "system:authenticated",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "group_mismatch",
			userInfo: authv1.UserInfo{
				Groups: []string{"system:authenticated", "developers"},
			},
			subject: workloadsv1.Subject{
				Kind: "Group",
				Name: "system:masters",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "returns_error_if_namespace_is_provided_even_if_group_matches",
			userInfo: authv1.UserInfo{
				Groups: []string{"devops@example.com"},
			},
			subject: workloadsv1.Subject{
				Kind:      "Group",
				Name:      "devops@example.com",
				Namespace: "default", // Groups are cluster-scoped, so this is invalid
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "user_with_no_groups_does_not_match",
			userInfo: authv1.UserInfo{
				Groups: []string{},
			},
			subject: workloadsv1.Subject{
				Kind: "Group",
				Name: "developers",
			},
			want:    false,
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesGroupKind(tc.userInfo, tc.subject)

			if (err != nil) != tc.wantErr {
				t.Errorf("matchesGroupKind() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("matchesGroupKind() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesServiceAccountKind(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1.Subject
		want     bool
	}{
		{
			name: "exact_service_account_match",
			userInfo: authv1.UserInfo{
				Username: "system:serviceaccount:default:my-app-sa",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "my-app-sa",
				Namespace: "default",
			},
			want: true,
		},
		{
			name: "mismatch_on_namespace",
			userInfo: authv1.UserInfo{
				Username: "system:serviceaccount:kube-system:my-app-sa",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "my-app-sa",
				Namespace: "default",
			},
			want: false,
		},
		{
			name: "mismatch_on_name",
			userInfo: authv1.UserInfo{
				Username: "system:serviceaccount:default:other-sa",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "my-app-sa",
				Namespace: "default",
			},
			want: false,
		},
		{
			name: "missing_namespace_in_subject_results_in_mismatch",
			userInfo: authv1.UserInfo{
				Username: "system:serviceaccount:default:my-app-sa",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "my-app-sa",
				Namespace: "", // This will format to system:serviceaccount::my-app-sa
			},
			want: false,
		},
		{
			name: "regular_user_trying_to_spoof_service_account_structure_fails",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "jane.doe@example.com",
				Namespace: "default",
			},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := matchesServiceAccountKind(tc.userInfo, tc.subject)

			if got != tc.want {
				t.Errorf("matchesServiceAccountKind() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesIdentity(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1.Subject
		want     bool
		wantErr  bool
	}{
		// 1. Validation Logic
		{
			name: "returns_error_if_subject_name_is_empty",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind: "User",
				Name: "",
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "returns_error_for_unsupported_or_invalid_Kind",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind: "RoleBinding", // Invalid subject kind for this function
				Name: "my-role",
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "propagates_errors_from_sub-functions_User_with_Namespace",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind:      "User",
				Name:      "jane.doe@example.com",
				Namespace: "default", // Triggers an error inside matchesUserKind
			},
			want:    false,
			wantErr: true,
		},

		// 2. Routing Logic (Proving it correctly routes to the right sub-function)
		{
			name: "successfully_routes_and_matches_User",
			userInfo: authv1.UserInfo{
				Username: "jane.doe@example.com",
			},
			subject: workloadsv1.Subject{
				Kind: "User",
				Name: "jane.doe@example.com",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "successfully_routes_and_matches_Group",
			userInfo: authv1.UserInfo{
				Groups: []string{"devops@example.com"},
			},
			subject: workloadsv1.Subject{
				Kind: "Group",
				Name: "devops@example.com",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "successfully_routes_and_matches_ServiceAccount",
			userInfo: authv1.UserInfo{
				Username: "system:serviceaccount:default:my-app-sa",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "my-app-sa",
				Namespace: "default",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "correctly_routes_but_fails_match_for_ServiceAccount",
			userInfo: authv1.UserInfo{
				Username: "system:serviceaccount:kube-system:my-app-sa",
			},
			subject: workloadsv1.Subject{
				Kind:      "ServiceAccount",
				Name:      "my-app-sa",
				Namespace: "default",
			},
			want:    false,
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesIdentity(tc.userInfo, tc.subject)

			if (err != nil) != tc.wantErr {
				t.Errorf("matchesIdentity() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("matchesIdentity() got = %v, want %v", got, tc.want)
			}
		})
	}
}

type fakeClient struct {
	client.Client
	getPodError  error
	getPodResult *corev1.Pod

	listWCError  error
	listWCResult *workloadsv1.WorkloadClassList

	getNamespaceError  error
	getNamespaceResult *corev1.Namespace

	getWorkloadClassError  error
	getWorkloadClassResult *workloadsv1.WorkloadClass
}

func createClient(getNSErr, getWCErr, listWCErr, getPodErr error, getNSResult *corev1.Namespace, getWCResult *workloadsv1.WorkloadClass, listWCResult *workloadsv1.WorkloadClassList, getPodResult *corev1.Pod) fakeClient {
	return fakeClient{
		getNamespaceError:      getNSErr,
		getNamespaceResult:     getNSResult,
		getWorkloadClassError:  getWCErr,
		getPodError:            getPodErr,
		getWorkloadClassResult: getWCResult,
		listWCError:            listWCErr,
		listWCResult:           listWCResult,
		getPodResult:           getPodResult,
	}
}

func (fc fakeClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	fc.listWCResult.DeepCopyInto(list.(*workloadsv1.WorkloadClassList))
	return fc.listWCError
}

func (fc fakeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch typedObj := obj.(type) {
	case *corev1.Namespace:
		if fc.getNamespaceError != nil {
			return fc.getNamespaceError
		}
		fc.getNamespaceResult.DeepCopyInto(typedObj)
	case *workloadsv1.WorkloadClass:
		if fc.getWorkloadClassError != nil {
			return fc.getWorkloadClassError
		}
		fc.getWorkloadClassResult.DeepCopyInto(typedObj)
	case *corev1.Pod:
		if fc.getPodError != nil {
			return fc.getPodError
		}
		fc.getPodResult.DeepCopyInto(typedObj)
	default:
		return fmt.Errorf("unknown object type")
	}

	return nil
}
