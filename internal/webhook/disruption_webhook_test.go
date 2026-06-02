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
	v1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
		otherMatches []string

		wantBM           *workloadsv1.WorkloadClass
		wantMaxSpec      int
		wantOtherMatches []string
	}{
		{
			name:             "nil_best_match_update_to_new_best_match",
			desc:             "No WC has been selected yet, update the best match",
			wc:               wc1,
			otherMatches:     []string{},
			maxSpec:          -1,
			wantBM:           wc1,
			wantMaxSpec:      1,
			wantOtherMatches: []string{},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_>_N_update_to_new_best_match",
			desc:             "A match has already been selected, the current WC being processed has a higher specificity, update best match",
			wc:               wc2,
			bm:               wc1,
			otherMatches:     []string{},
			maxSpec:          1,
			wantBM:           wc2,
			wantMaxSpec:      2,
			wantOtherMatches: []string{"namespace/wc1"},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_==_N_wc_is_older_update_to_new_best_match",
			desc:             "A match has already been selected, the current WC being processed has the same specificity but is older, update best match",
			wc:               wc22,
			bm:               wc2,
			otherMatches:     []string{},
			maxSpec:          2,
			wantBM:           wc22,
			wantMaxSpec:      2,
			wantOtherMatches: []string{"namespace/wc2"},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_==_N_wc_is_not_older_no_update",
			desc:             "A match has already been selected, the current WC being processed has the same specificity but is newer, no update to best match",
			wc:               wc22,
			bm:               wc2,
			otherMatches:     []string{},
			maxSpec:          2,
			wantBM:           wc22,
			wantMaxSpec:      2,
			wantOtherMatches: []string{"namespace/wc2"},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_<_N_no_update",
			desc:             "A match has already been selected, the current WC being processed has a lower specificity, no update to best match",
			wc:               wc1,
			bm:               wc2,
			otherMatches:     []string{},
			maxSpec:          2,
			wantBM:           wc2,
			wantMaxSpec:      2,
			wantOtherMatches: []string{"namespace/wc1"},
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
			if !stringSlicesEqualUnordered(gotOtherMatches, tc.wantOtherMatches) {
				t.Errorf("updateBestMatch() did not update otherMatches as expected, got: %v, want: %v", tc.otherMatches, tc.wantOtherMatches)
			}
		})
	}
}

func TestNamespaceDefaultWorkloadClass(t *testing.T) {
	const defaultClassAnnotation = "workloads.gke.io/default-class"
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
			Annotations: map[string]string{
				defaultClassAnnotation: "wc",
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

	testCases := []struct {
		name          string
		desc          string
		bestMatch     *workloadsv1.WorkloadClass
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		wantBestMatch *workloadsv1.WorkloadClass
	}{
		{
			name:          "best_match_not_nil",
			desc:          "Best match WC is not nil, do not set to namespace default",
			bestMatch:     wc,
			wantBestMatch: wc,
		},
		{
			name:       "error_getting_namespace",
			desc:       "Error getting namespace, expect nil best match",
			getNSError: fmt.Errorf("error getting Namespace"),
		},
		{
			name:        "no_default_class_with_namespace",
			desc:        "Namespace does not have a default WC, expect nil best match",
			getNSResult: nsNoAnnotation,
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
			gotWC := v.namespaceDefaultWorkloadClass(ctx, pod, tc.bestMatch)
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
	pod.Labels = map[string]string{"labelA": "valueA"}

	testCases := []struct {
		name          string
		desc          string
		listWCError   error
		listWCResult  *workloadsv1.WorkloadClassList
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		wantBestMatch *workloadsv1.WorkloadClass
		wantErr       bool
	}{
		{
			name:         "error_listing_wcs",
			desc:         "Error listing WorkloadClasses, expect nil result and error",
			listWCError:  fmt.Errorf("error listing WorkloadClasses"),
			listWCResult: &workloadsv1.WorkloadClassList{},
			wantErr:      true,
		},
		{
			name: "success",
			desc: "Success getting best match",
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{wc},
			},
			getNSResult:   nsNoAnnotation,
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
			client := createClient(tc.getNSError, tc.getWCError, tc.listWCError, nil, tc.getNSResult, &wc, tc.listWCResult, nil)
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
		namespace = "namespace"
		eviction  = "eviction"
	)
	var (
		inWindowDay          = time.Now().Weekday().String()
		outOfWindowDay       = time.Now().AddDate(0, 0, 1).Weekday().String()
		podNowCreationTime   = time.Now()
		recentLastDisruption = time.Now().AddDate(0, 0, -5)
		labels               = map[string]string{"labelA": "valueA"}
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
				AllowedDisruptionsOutsideOfWindow: []string{"VPA"},
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

	evictionRequest := admissionv1.AdmissionRequest{
		Name:        "VPA",
		Namespace:   namespace,
		SubResource: eviction,
		UserInfo: v1.UserInfo{
			Username: "vpa-recommender",
		},
	}

	evictionRequestNonMatchingUser := admissionv1.AdmissionRequest{
		Name:        "something-else",
		Namespace:   namespace,
		SubResource: eviction,
		UserInfo: v1.UserInfo{
			Username: "something-else",
		},
	}

	testCases := []struct {
		name               string
		desc               string
		req                admissionv1.AdmissionRequest
		getPodErr          error
		listWCErr          error
		listWCResp         *workloadsv1.WorkloadClassList
		getWCErr           error
		getWCResp          *workloadsv1.WorkloadClass
		guardrailValFails  bool
		emergencyOverride  bool
		inWindow           bool
		overdue            bool
		podCreationTime    time.Time
		lastDisruptionTime time.Time
		want               admission.Response
	}{
		{
			name:               "errorGettingPod",
			desc:               "Error getting pod, admission Errored",
			req:                evictionRequest,
			getPodErr:          fmt.Errorf("error getting Pod"),
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: recentLastDisruption,
			want:               admission.Errored(http.StatusInternalServerError, fmt.Errorf("error getting Pod")),
		},
		{
			name: "notAnEviction",
			desc: "Not an eviction, admission Allowed",
			req: admissionv1.AdmissionRequest{
				Name:        "VPA",
				Namespace:   namespace,
				SubResource: "not-an-eviction",
			},
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: recentLastDisruption,
			want:               admission.Allowed("Not an eviction"),
		},
		{
			name:               "errorGettingBestMatchWC",
			desc:               "Error getting best match WC, admission Allowed",
			req:                evictionRequest,
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: recentLastDisruption,
			listWCErr:          fmt.Errorf("error listing WorkloadClasses"),
			want:               admission.Allowed("Failed to get WorkloadClass matches this pod or namespace"),
		},
		{
			name:               "bestMatchWCIsNil",
			desc:               "Best match WC is nil, admission Allowed",
			req:                evictionRequest,
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: recentLastDisruption,
			want:               admission.Allowed("No WorkloadClass matches this pod or namespace"),
		},
		{
			name:               "guardrailValidationFailed",
			desc:               "Guardrail validation failed, admission Allowed",
			req:                evictionRequest,
			podCreationTime:    podNowCreationTime,
			getWCResp:          wcValFailed,
			lastDisruptionTime: recentLastDisruption,
			want:               admission.Allowed("WorkloadClass failed Guardrail validation"),
		},
		{
			name:               "emergencyOverride",
			desc:               "Emergency override, admission Allowed",
			req:                evictionRequest,
			podCreationTime:    podNowCreationTime,
			getWCResp:          wc,
			lastDisruptionTime: recentLastDisruption,
			emergencyOverride:  true,
			want:               admission.Allowed("Emergency override active"),
		},
		{
			name:               "allowedUser",
			desc:               "Allowed user, admission Allowed",
			req:                evictionRequest,
			getWCResp:          wc,
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: recentLastDisruption,
			want:               admission.Allowed("Disruption allowed for authorized user: VPA"),
		},
		{
			name:               "overdue",
			desc:               "Overdue, admission Allowed",
			req:                evictionRequestNonMatchingUser,
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: time.Now().AddDate(-1, 0, 0),
			getWCResp:          wc,
			want:               admission.Allowed("Workload class is overdue for maintenance, bypassing constraints"),
		},
		{
			name:               "notInWindowNotOverdue",
			desc:               "Not in window, not overdue, admission Denied",
			req:                evictionRequestNonMatchingUser,
			podCreationTime:    podNowCreationTime,
			getWCResp:          wcOutOfWindow,
			lastDisruptionTime: recentLastDisruption,
			want:               admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", wcOutOfWindow.Name)),
		},
		{
			name:               "podIsTooNew",
			desc:               "Pod is too new, admission Denied",
			req:                evictionRequestNonMatchingUser,
			getWCResp:          wc,
			podCreationTime:    podNowCreationTime,
			lastDisruptionTime: recentLastDisruption,
			want: admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				time.Since(podNowCreationTime).Round(time.Minute), wc.Spec.DisruptionPolicy.MinInitialRunDurationDays)),
		},
		{
			name:               "inWindowNotOverduePodNotTooNew",
			desc:               "In window, not overdue, Pod not too new, admission Allowed",
			req:                evictionRequestNonMatchingUser,
			lastDisruptionTime: recentLastDisruption,
			podCreationTime:    time.Now().AddDate(0, 0, -4),
			want:               admission.Allowed("Eviction allowed by WorkloadClass policy"),
		},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod.CreationTimestamp = metav1.Time{Time: tc.podCreationTime}

			if tc.getWCResp != nil {
				tc.getWCResp.Status = workloadsv1.WorkloadClassStatus{
					LastDisruptionTime: &metav1.Time{Time: tc.lastDisruptionTime},
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

func stringSlicesEqualUnordered(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	counts := make(map[string]int)
	for _, s := range s1 {
		counts[s]++
	}

	for _, s := range s2 {
		if counts[s] == 0 {
			// Either s is not in s1, or we've seen it more times in s2
			return false
		}
		counts[s]--
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
	default:
		return fmt.Errorf("unknown object type")
	}

	return nil
}
