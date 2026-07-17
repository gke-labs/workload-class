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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestGetSpecificity(t *testing.T) {
	testCases := []struct {
		name             string
		desc             string
		matchLabels      map[string]string
		matchExpressions []metav1.LabelSelectorRequirement
		want             int
	}{
		{
			name: "nil_selector",
			desc: "LabelSelector is nil, return 0",
			want: 0,
		},
		{
			name: "nil_match_labels_not_nil_match_expressions",
			desc: "LabelSelector only has MatchExpressions, return length of MatchExpressions",
			matchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "k1", Operator: "op1", Values: []string{"v1"}},
				{Key: "k2", Operator: "op2", Values: []string{"v2"}},
			},
			want: 2,
		},
		{
			name:        "match_labels_nil_match_expressions",
			desc:        "LabelSelector only has MatchLabels, return length of MatchLabels",
			matchLabels: map[string]string{"k1": "v1", "k2": "v2"},
			want:        2,
		},
		{
			name:        "both_not_nil",
			desc:        "LabelSelector has both MatchLabels and MatchSelectors, returned combined length of both",
			matchLabels: map[string]string{"k1": "v1", "k2": "v2"},
			matchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "k1", Operator: "op1", Values: []string{"v1"}},
				{Key: "k2", Operator: "op2", Values: []string{"v2"}},
				{Key: "k3", Operator: "op3", Values: []string{"v3"}},
			},
			want: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selector := &metav1.LabelSelector{
				MatchLabels:      tc.matchLabels,
				MatchExpressions: tc.matchExpressions,
			}
			got := getSpecificity(selector)
			if got != tc.want {
				t.Errorf("getSpecificity() returned an unexpected value, want: %d, got: %d", tc.want, got)
			}
		})
	}
}

func TestUpdateBestMatch(t *testing.T) {
	const namespace = "namespace"
	latest := time.Now()
	oldest := latest.AddDate(0, 0, -1)
	wc1 := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
				},
			},
		},
	}
	wc1.Name = "wc1"
	wc1.Namespace = namespace
	wc1.CreationTimestamp = metav1.Time{Time: latest}

	wc2 := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
					"labelB": "valueB",
				},
			},
		},
	}
	wc2.Name = "wc2"
	wc2.Namespace = namespace
	wc2.CreationTimestamp = metav1.Time{Time: latest}

	wc22 := &workloadsv1.WorkloadClass{
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
					"labelB": "valueB",
				},
			},
		},
	}
	wc22.Name = "wc22"
	wc22.Namespace = namespace
	wc22.CreationTimestamp = metav1.Time{Time: oldest}

	testCases := []struct {
		name         string
		desc         string
		wc           *workloadsv1.WorkloadClass
		bm           *workloadsv1.WorkloadClass
		maxSpec      int
		otherMatches map[string]int

		wantBM           *workloadsv1.WorkloadClass
		wantMaxSpec      int
		wantOtherMatches map[string]int
	}{
		{
			name:             "nil_best_match_update_to_new_best_match",
			desc:             "No WC has been selected yet, update the best match",
			wc:               wc1,
			otherMatches:     map[string]int{},
			maxSpec:          -1,
			wantBM:           wc1,
			wantMaxSpec:      1,
			wantOtherMatches: map[string]int{},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_>_N_update_to_new_best_match",
			desc:             "A match has already been selected, the current WC being processed has a higher specificity, update best match",
			wc:               wc2,
			bm:               wc1,
			otherMatches:     map[string]int{},
			maxSpec:          1,
			wantBM:           wc2,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc1": 1},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_==_N_wc_is_older_update_to_new_best_match",
			desc:             "A match has already been selected, the current WC being processed has the same specificity but is older, update best match",
			wc:               wc22,
			bm:               wc2,
			otherMatches:     map[string]int{},
			maxSpec:          2,
			wantBM:           wc22,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc2": 2},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_==_N_wc_is_not_older_no_update",
			desc:             "A match has already been selected, the current WC being processed has the same specificity but is newer, no update to best match",
			wc:               wc22,
			bm:               wc2,
			otherMatches:     map[string]int{},
			maxSpec:          2,
			wantBM:           wc22,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc2": 2},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_<_N_no_update",
			desc:             "A match has already been selected, the current WC being processed has a lower specificity, no update to best match",
			wc:               wc1,
			bm:               wc2,
			otherMatches:     map[string]int{},
			maxSpec:          2,
			wantBM:           wc2,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc1": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotBM, gotOtherMatches := updateBestMatch(tc.wc, tc.bm, &tc.maxSpec, tc.otherMatches)
			if gotBM.Name != tc.wantBM.Name {
				t.Errorf("updateBestMatch() did not update the bestMatch as expected, got: %v, want: %v", tc.bm, tc.wantBM)
			}
			if tc.maxSpec != tc.wantMaxSpec {
				t.Errorf("updateBestMatch() did not update maxSpecificity as expected, got: %d, want %d", tc.maxSpec, tc.wantMaxSpec)
			}
			if !mapsEqual(gotOtherMatches, tc.wantOtherMatches) {
				t.Errorf("updateBestMatch() did not update otherMatches as expected, got: %v, want: %v", tc.otherMatches, tc.wantOtherMatches)
			}
		})
	}
}

func TestNamespaceDefaultWorkloadClass(t *testing.T) {
	const namespace = "namespace"
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
				},
			},
		},
	}
	nsDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-default",
			Namespace: namespace,
			Labels: map[string]string{
				workloadsv1.DefaultClassLabel: "wc",
			},
		},
	}
	nsNoDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-no-wc",
			Namespace: namespace,
			Labels: map[string]string{
				"something-else": "true",
			},
		},
	}
	pod := &corev1.Pod{}
	pod.Namespace = namespace

	testCases := []struct {
		name          string
		desc          string
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		wantBestMatch *workloadsv1.WorkloadClass
	}{
		{
			name:       "error_getting_namespace",
			desc:       "Error getting namespace, expect nil best match",
			getNSError: fmt.Errorf("error getting Namespace"),
		},
		{
			name:        "no_default_class_with_namespace",
			desc:        "Namespace does not have a default WC, expect nil best match",
			getNSResult: nsNoDefault,
		},
		{
			name:        "error_getting_wc",
			desc:        "Namespace has a default WC, but error getting WC, expect nil best match",
			getNSResult: nsDefault,
			getWCError:  fmt.Errorf("error getting WorkloadClass"),
		},
		{
			name:          "success_getting_namespace_default",
			desc:          "Namespace has WC, success getting WC, expect updated best match",
			getNSResult:   nsDefault,
			wantBestMatch: wc,
		},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := createClient(tc.getNSError, tc.getWCError, nil, nil, tc.getNSResult, wc, nil, nil)
			v := &DisruptionWebhook{
				Client: client,
			}
			gotWC := v.namespaceDefaultWorkloadClass(ctx, pod)
			if (gotWC != nil) != (tc.wantBestMatch != nil) {
				t.Errorf("namespaceDefaultWorkloadClass() returned an unexpected result, got: %v, want: %v", gotWC, tc.wantBestMatch)
			}
			if gotWC == nil {
				return
			}
			if gotWC.Name != tc.wantBestMatch.Name {
				t.Errorf("namespaceDefaultWorkloadClass() returned a different WorkloadClass than expected, got: %v, want: %v", gotWC, tc.wantBestMatch)
			}
		})
	}
}

func TestBestMatchWorkloadClass(t *testing.T) {
	const namespace = "namespace"
	wc := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
				},
			},
		},
	}
	wcDefault := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-default",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelB": "valueB", // Won't match with Pod
				},
			},
		},
	}
	nsNoDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-no-wc",
			Namespace: namespace,
			Labels: map[string]string{
				"something-else": "true",
			},
		},
	}
	nsWithDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-with-wc",
			Namespace: namespace,
			Labels: map[string]string{
				workloadsv1.DefaultClassLabel: "wc-default",
			},
		},
	}
	pod := &corev1.Pod{}
	pod.Namespace = namespace
	pod.Labels = map[string]string{"labelA": "valueA"}

	testCases := []struct {
		name          string
		desc          string
		listWCError   error
		listWCResult  *workloadsv1.WorkloadClassList
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		getWCResult   *workloadsv1.WorkloadClass
		wantBestMatch *workloadsv1.WorkloadClass
		wantErr       bool
	}{
		{
			name:         "error_listing_wcs",
			desc:         "Error listing WorkloadClasses, expect nil result and error",
			listWCError:  fmt.Errorf("error listing WorkloadClasses"),
			listWCResult: &workloadsv1.WorkloadClassList{},
			getNSResult:  nsNoDefault,
			wantErr:      true,
		},
		{
			name: "success_getting_namespace_default",
			desc: "Success getting namespace default as best match",
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{wc, wcDefault},
			},
			getNSResult:   nsWithDefault,
			getWCResult:   &wcDefault,
			wantBestMatch: &wcDefault,
			wantErr:       false,
		},
		{
			name: "success_no_namespace_default",
			desc: "Success getting best match with selectors (no namespace default)",
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{wc},
			},
			getNSResult:   nsNoDefault,
			wantBestMatch: &wc,
			wantErr:       false,
		},
	}

	ctx := t.Context()
	req := admission.Request{}
	req.Name = "name"
	req.Namespace = namespace
	req.UserInfo.Username = "test-user"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := createClient(tc.getNSError, tc.getWCError, tc.listWCError, nil, tc.getNSResult, tc.getWCResult, tc.listWCResult, nil)
			v := &DisruptionWebhook{
				Client: client,
			}
			gotBestMatch, err := v.bestMatchWorkloadClass(ctx, req, pod)
			if (err != nil) != tc.wantErr {
				t.Errorf("bestMatchWorkloadClass returned unexpected error, got: %v, wantErr: %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if (gotBestMatch != nil) != (tc.wantBestMatch != nil) {
				t.Errorf("bestMatchWorkloadClass() returned an unexpected result, got: %v, want: %v", gotBestMatch, tc.wantBestMatch)
			}
			if gotBestMatch == nil {
				return
			}
			if gotBestMatch.Name != tc.wantBestMatch.Name {
				t.Errorf("bestMatchWorkloadClass() returned a different WorkloadClass than expected, got: %v, want: %v", gotBestMatch, tc.wantBestMatch)
			}

		})
	}
}

func TestHandle(t *testing.T) {
	const (
		namespace     = "namespace"
		eviction      = "eviction"
		adminUsername = "admin@mycompany.com"
		podName       = "podrick"
	)
	torontoLoc, err := time.LoadLocation("America/Toronto")
	if err != nil {
		t.Fatalf("Failed to load timezone: %v", err)
	}

	var (
		inWindowDay        = time.Now().In(torontoLoc).Weekday().String()
		outOfWindowDay     = time.Now().In(torontoLoc).AddDate(0, 0, 1).Weekday().String()
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
			getWCResp:       wcOutOfWindow,
			podCreationTime: podNowCreationTime,
			readiness:       workloadsv1.ReadinessNotReady,
			want:            admission.Allowed("Disruption allowed for authorized user, PDB leased: created"),
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

func TestBestMatchWorkloadClass_EmitEvent(t *testing.T) {
	const namespace = "namespace"
	wc1 := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc1",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-10 * time.Minute)},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
				},
			},
		},
	}
	wc2 := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc2",
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
				},
			},
		},
	}
	pod := &corev1.Pod{}
	pod.Name = "my-pod"
	pod.Namespace = namespace
	pod.Labels = map[string]string{"labelA": "valueA"}
	listWCResult := &workloadsv1.WorkloadClassList{
		Items: []workloadsv1.WorkloadClass{wc1, wc2},
	}

	nsNoDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-no-wc",
			Namespace: namespace,
			Labels: map[string]string{
				"something-else": "true",
			},
		},
	}

	ctx := t.Context()
	req := admission.Request{}
	req.Name = "name"
	req.Namespace = namespace
	req.UserInfo.Username = "test-user"

	testClient := createClient(nil, nil, nil, nil, nsNoDefault, &wc1, listWCResult, nil)
	// Create fake event recorder
	fakeRecorder := events.NewFakeRecorder(10)
	v := &DisruptionWebhook{
		Client:   testClient,
		Recorder: fakeRecorder,
	}

	t.Run("validate_event_emitted", func(t *testing.T) {
		gotBestMatch, err := v.bestMatchWorkloadClass(ctx, req, pod)
		if err != nil {
			t.Fatalf("bestMatchWorkloadClass returned unexpected error: %v", err)
		}
		if gotBestMatch.Name != "wc1" {
			t.Errorf("Expected wc1 to be best match, got: %s", gotBestMatch.Name)
		}
		// Verify that the event was emitted
		select {
		case event := <-fakeRecorder.Events:
			expected := "Warning AmbiguousMatch the following WorkloadClasses match pods with the same specificity as the best match, but were not selected: namespace/wc2"
			if event != expected {
				t.Errorf("Expected event: %q, got: %q", expected, event)
			}
		default:
			t.Error("Expected AmbiguousMatch event to be emitted, but none was found")
		}
	})
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

func mapsEqual(m1, m2 map[string]int) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k, val1 := range m1 {
		if val2, ok := m2[k]; !ok || val1 != val2 {
			return false
		}
	}

	return true
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
	case *policyv1.PodDisruptionBudget:
		return k8serrors.NewNotFound(schema.GroupResource{Group: "policy", Resource: "poddisruptionbudgets"}, obj.GetName())
	default:
		return fmt.Errorf("unknown object type")
	}

	return nil
}

func (fc fakeClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return nil
}

func (fc fakeClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return nil
}

func (fc fakeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return nil
}

func TestTryAcquirePDBLease(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = workloadsv1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	wcName := "test-wc"

	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: wcName, Namespace: namespace},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
	}

	tests := []struct {
		name       string
		initObjs   []client.Object
		wantDenied bool
		wantMsg    string
	}{
		{
			name:       "pdb_does_not_exist_successfully_creates_and_leases",
			initObjs:   []client.Object{},
			wantDenied: false,
			wantMsg:    "Disruption allowed for authorized user, PDB leased",
		},
		{
			name: "pdb_exists_and_lease_is_allowed_successfully_updates",
			initObjs: []client.Object{
				&policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:      utils.PDBName(wcName),
						Namespace: namespace,
					},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
					},
				},
			},
			wantDenied: false,
			wantMsg:    "Disruption allowed for authorized user, PDB leased",
		},
		{
			name: "pdb_exists_but_has_ongoing_lease_denied",
			initObjs: []client.Object{
				&policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:      utils.PDBName(wcName),
						Namespace: namespace,
						Annotations: map[string]string{
							utils.BypassPod:        "other-pod",
							utils.BypassExpiration: time.Now().Add(time.Hour).Format(utils.ExpirationFormat),
						},
					},
				},
			},
			wantDenied: true,
			wantMsg:    "Disruption denied, PDB workload-test-wc has an ongoing lease",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.initObjs...).Build()
			v := &DisruptionWebhook{
				Client: fakeClient,
			}

			s := workloadsv1.Subject{Kind: "User", Name: "admin@example.com"}
			resp := v.tryAcquirePDBLease(context.Background(), wc, pod, s)
			if resp.Allowed == tt.wantDenied {
				t.Errorf("tryAcquirePDBLease() allowed = %v, wantDenied %v", resp.Allowed, tt.wantDenied)
			}
			if !strings.HasPrefix(resp.Result.Message, tt.wantMsg) {
				t.Errorf("tryAcquirePDBLease() msg = %v, want prefix %v", resp.Result.Message, tt.wantMsg)
			}
		})
	}
}

func TestTryBypassWindowByIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = workloadsv1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	wcName := "test-wc"

	testSubject := workloadsv1.Subject{
		Kind: "User",
		Name: "test-user",
	}

	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: wcName, Namespace: namespace},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			DisruptionPolicy: workloadsv1.DisruptionPolicy{
				AllowedDisruptionsOutsideOfWindow: []workloadsv1.Subject{testSubject},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
	}

	tests := []struct {
		name       string
		username   string
		initObjs   []client.Object
		wantDenied bool
		wantMsg    string
	}{
		{
			name:       "user_does_not_match_identity",
			username:   "unauthorized-user",
			initObjs:   []client.Object{},
			wantDenied: true,
			wantMsg:    "Eviction blocked: currently outside of allowed disruption windows",
		},
		{
			name:       "user_matches_identity_acquires_lease",
			username:   "test-user",
			initObjs:   []client.Object{},
			wantDenied: false,
			wantMsg:    "Disruption allowed for authorized user, PDB leased",
		},
		{
			name:     "user_matches_identity_but_lease_denied",
			username: "test-user",
			initObjs: []client.Object{
				&policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:      utils.PDBName(wcName),
						Namespace: namespace,
						Annotations: map[string]string{
							utils.BypassPod:        "other-pod",
							utils.BypassExpiration: time.Now().Add(time.Hour).Format(utils.ExpirationFormat),
						},
					},
				},
			},
			wantDenied: true,
			wantMsg:    "Disruption denied, PDB workload-test-wc has an ongoing lease",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.initObjs...).Build()
			v := &DisruptionWebhook{
				Client: fakeClient,
			}

			req := admission.Request{}
			req.UserInfo.Username = tt.username

			resp := v.tryBypassWindowByIdentity(context.Background(), wc, req, pod)
			if resp.Allowed == tt.wantDenied {
				t.Errorf("tryBypassWindowByIdentity() allowed = %v, wantDenied %v", resp.Allowed, tt.wantDenied)
			}
			if !strings.HasPrefix(resp.Result.Message, tt.wantMsg) {
				t.Errorf("tryBypassWindowByIdentity() msg = %v, want prefix %v", resp.Result.Message, tt.wantMsg)
			}
		})
	}
}
