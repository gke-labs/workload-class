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
	"strings"
	"testing"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
				AllowedDisruptionsOutsideOfWindow: []workloadsv1.Subject{{Kind: rbacv1.UserKind, Name: adminUsername}},
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

	// wcSelectorOverlap is out of window and only flagged for an overlapping PodSelector, so its constraints are
	// still enforced.
	wcSelectorOverlap := wcOutOfWindow.DeepCopy()
	wcSelectorOverlap.Status.Conditions = []metav1.Condition{
		{
			Type:   workloadsv1.ConditionTypeValidated,
			Status: metav1.ConditionFalse,
			Reason: workloadsv1.ReasonSelectorOverlap,
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
			name:            "allowedUserOutOfWindow",
			desc:            "Out of window, allowed user, admission Allowed",
			req:             evictionRequest,
			getWCResp:       wcOutOfWindow,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("Disruption allowed for authorized user"),
		},
		{
			name:            "allowedUserInWindowBypassesMinInitialRun",
			desc:            "In window, Pod is too new, but identities are checked first, so an allowed user is admitted",
			req:             evictionRequest,
			getWCResp:       wc,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("Disruption allowed for authorized user"),
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
			desc:            "Not in window, not overdue, user not allowed outside of window, admission Denied",
			req:             evictionRequestNonMatchingUser,
			podCreationTime: podNowCreationTime,
			getWCResp:       wcOutOfWindow,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", wcOutOfWindow.Name)),
		},
		{
			name:            "selectorOverlapStillEnforced",
			desc:            "Validated=False only because of SelectorOverlap, not in window, admission Denied",
			req:             evictionRequestNonMatchingUser,
			podCreationTime: podNowCreationTime,
			getWCResp:       wcSelectorOverlap,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", wcSelectorOverlap.Name)),
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
			getWCResp:       wc,
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

func TestHandle_AuthorizedSubjectLease(t *testing.T) {
	const (
		namespace     = "namespace"
		adminUsername = "admin@mycompany.com"
		adminGroup    = "sre-oncall"
		otherOwner    = "system:serviceaccount:kube-system:cluster-autoscaler"
		podName       = "podrick"
		podUID        = types.UID("podrick-uid")
	)
	labels := map[string]string{"labelA": "valueA"}
	adminSubject := workloadsv1.Subject{Kind: rbacv1.UserKind, Name: adminUsername}
	adminGroupSubject := workloadsv1.Subject{Kind: rbacv1.GroupKind, Name: adminGroup}

	// The WorkloadClass is outside of its disruption window and allows adminUsername and adminGroup to disrupt outside of it.
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: labels},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
					{Name: "maintenance", DaysOfWeek: []string{time.Now().AddDate(0, 0, 1).Weekday().String()}, StartTime: "00:00", EndTime: "23:59", TimeZone: "America/Toronto"},
				},
				AllowedDisruptionsOutsideOfWindow: []workloadsv1.Subject{adminSubject, adminGroupSubject},
			},
		},
		Status: workloadsv1.WorkloadClassStatus{MaintenanceReadiness: workloadsv1.ReadinessNotReady},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       podUID,
			Labels:    labels,
		},
	}

	isController := true
	closed := intstr.FromInt32(0)
	open := intstr.FromString("100%")
	newPDB := func(owned bool, podUIDLabel types.UID, maxUnavailable intstr.IntOrString, leaseOwner string, leaseExpiry time.Duration) *policyv1.PodDisruptionBudget {
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      utils.PDBName(podName),
				Namespace: namespace,
				Labels:    map[string]string{utils.PodUIDLabel: string(podUIDLabel)},
			},
			Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: &maxUnavailable},
		}
		if owned {
			pdb.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "workloads.gke.io/v1",
				Kind:       "WorkloadClass",
				Name:       wc.Name,
				Controller: &isController,
			}}
		}
		if leaseOwner != "" {
			pdb.Annotations = map[string]string{
				utils.BypassOwner:      leaseOwner,
				utils.BypassExpiration: time.Now().Add(leaseExpiry).Format(utils.ExpirationFormat),
			}
		}
		return pdb
	}

	testCases := []struct {
		name            string
		pdb             *policyv1.PodDisruptionBudget
		getPDBErr       error
		nilTracker      bool
		inWindow        bool
		wantAllowed     bool
		wantMsgContains string
		wantQueued      bool
	}{
		{
			// Identities are checked before the window, so a closed PDB (e.g., the WorkloadClass is NotReady
			// in-window because a Pod has not met MinInitialRunDurationDays) is still leased for allowed subjects.
			name:            "in_window_closed_pdb_queues_lease",
			pdb:             newPDB(true, podUID, closed, "", 0),
			inWindow:        true,
			wantAllowed:     true,
			wantMsgContains: "lease on PDB",
			wantQueued:      true,
		},
		{
			name:        "no_pdb_allows_without_lease",
			pdb:         nil,
			wantAllowed: true,
		},
		{
			name:        "error_getting_pdb_allows_without_lease",
			getPDBErr:   fmt.Errorf("error getting PDB"),
			wantAllowed: true,
		},
		{
			name:        "user_created_pdb_with_same_name_is_not_leased",
			pdb:         newPDB(false, podUID, closed, "", 0),
			wantAllowed: true,
		},
		{
			name:        "closed_pdb_without_lease_queues_lease",
			pdb:         newPDB(true, podUID, closed, "", 0),
			wantAllowed: true,
			wantQueued:  true,
		},
		{
			name:        "closed_pdb_with_expired_lease_queues_lease",
			pdb:         newPDB(true, podUID, closed, otherOwner, -time.Minute),
			wantAllowed: true,
			wantQueued:  true,
		},
		{
			name:        "stale_lease_for_previous_pod_instance_queues_lease",
			pdb:         newPDB(true, types.UID("old-uid"), open, otherOwner, time.Minute),
			wantAllowed: true,
			wantQueued:  true,
		},
		{
			name:        "subject_already_leasing_allows_without_new_lease",
			pdb:         newPDB(true, podUID, open, utils.BypassOwnerValue(adminSubject), time.Minute),
			wantAllowed: true,
		},
		{
			name:        "second_matching_subject_group_already_leasing_allows_without_new_lease",
			pdb:         newPDB(true, podUID, open, utils.BypassOwnerValue(adminGroupSubject), time.Minute),
			wantAllowed: true,
		},
		{
			name:            "other_subject_leasing_denies",
			pdb:             newPDB(true, podUID, open, otherOwner, time.Minute),
			wantAllowed:     false,
			wantMsgContains: "has an ongoing lease held by " + otherOwner,
		},
		{
			name:        "open_pdb_without_lease_allows_without_lease",
			pdb:         newPDB(true, podUID, open, "", 0),
			wantAllowed: true,
		},
		{
			name:        "nil_tracker_allows_without_lease",
			pdb:         newPDB(true, podUID, closed, "", 0),
			nilTracker:  true,
			wantAllowed: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testWC := wc.DeepCopy()
			if tc.inWindow {
				testWC.Spec.DisruptionPolicy.AllowedDisruptionWindows[0].DaysOfWeek = []string{time.Now().Weekday().String()}
			}
			listResp := &workloadsv1.WorkloadClassList{Items: []workloadsv1.WorkloadClass{*testWC}}
			c := createClient(nil, nil, nil, nil, ns, testWC, listResp, pod)
			c.getPDBResult = tc.pdb
			c.getPDBError = tc.getPDBErr

			tracker := utils.NewPendingLeaseTracker(1)
			v := &DisruptionWebhook{Client: c, LeaseTracker: tracker}
			if tc.nilTracker {
				v.LeaseTracker = nil
			}

			req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Name:        podName,
				Namespace:   namespace,
				SubResource: "eviction",
				UserInfo:    authv1.UserInfo{Username: adminUsername, Groups: []string{adminGroup}},
			}}
			resp := v.Handle(t.Context(), req)

			if resp.Allowed != tc.wantAllowed {
				t.Fatalf("Handle() allowed = %v, want %v (response: %+v)", resp.Allowed, tc.wantAllowed, resp.Result)
			}
			if tc.wantMsgContains != "" && (resp.Result == nil || !strings.Contains(resp.Result.Message, tc.wantMsgContains)) {
				t.Errorf("Handle() message = %+v, want it to contain %q", resp.Result, tc.wantMsgContains)
			}

			leases := tracker.Drain(types.NamespacedName{Namespace: namespace, Name: wc.Name})
			if !tc.wantQueued {
				if len(leases) != 0 {
					t.Errorf("Handle() queued leases %+v, want none", leases)
				}
				return
			}
			want := []utils.PendingLease{{PodName: podName, PodUID: podUID, Subject: adminSubject}}
			if len(leases) != 1 || leases[0] != want[0] {
				t.Errorf("Handle() queued leases %+v, want %+v", leases, want)
			}
			select {
			case ev := <-tracker.Events():
				if ev.Object.GetName() != wc.Name || ev.Object.GetNamespace() != namespace {
					t.Errorf("Handle() signaled %s/%s, want %s/%s", ev.Object.GetNamespace(), ev.Object.GetName(), namespace, wc.Name)
				}
			default:
				t.Error("Handle() did not signal the WorkloadClass controller")
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

	// getPDBResult is returned for PodDisruptionBudget Gets. If nil, a NotFound error is returned.
	getPDBError  error
	getPDBResult *policyv1.PodDisruptionBudget
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
	case *policyv1.PodDisruptionBudget:
		if fc.getPDBError != nil {
			return fc.getPDBError
		}
		if fc.getPDBResult == nil {
			return apierrors.NewNotFound(schema.GroupResource{Group: "policy", Resource: "poddisruptionbudgets"}, key.Name)
		}
		fc.getPDBResult.DeepCopyInto(typedObj)
	default:
		return fmt.Errorf("unknown object type")
	}

	return nil
}
