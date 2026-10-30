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
	"fmt"
	"strings"
	"testing"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
			wc:               wc2,
			bm:               wc22,
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

func TestNamespaceEnforcedWorkloadClass(t *testing.T) {
	const namespace = "namespace"
	now := metav1.Now()
	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc",
			Namespace:         namespace,
			CreationTimestamp: now,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"labelA": "valueA",
				},
			},
		},
	}
	deletingWC := wc.DeepCopy()
	deletingWC.DeletionTimestamp = &now

	nsDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "namespace-default",
			Namespace: namespace,
			Labels: map[string]string{
				workloadsv1.EnforcedClassLabel: "wc",
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

	testCases := []struct {
		name          string
		desc          string
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		getWCResult   *workloadsv1.WorkloadClass
		wantBestMatch *workloadsv1.WorkloadClass
		wantErr       bool
	}{
		{
			name:       "error_getting_namespace",
			desc:       "Error getting namespace, expect nil best match and error",
			getNSError: fmt.Errorf("error getting Namespace"),
			wantErr:    true,
		},
		{
			name:       "namespace_not_found",
			desc:       "Namespace not found, expect nil best match and no error",
			getNSError: apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, namespace),
		},
		{
			name:        "no_default_class_with_namespace",
			desc:        "Namespace does not have a default WC, expect nil best match",
			getNSResult: nsNoDefault,
		},
		{
			name:        "default_wc_not_found",
			desc:        "Namespace has a default WC label, but WC does not exist, expect nil best match and no error",
			getNSResult: nsDefault,
			getWCError:  apierrors.NewNotFound(schema.GroupResource{Group: "workloads.gke.io", Resource: "workloadclasses"}, "wc"),
		},
		{
			name:        "default_wc_being_deleted",
			desc:        "Namespace has a default WC that has a DeletionTimestamp, expect nil best match",
			getNSResult: nsDefault,
			getWCResult: deletingWC,
		},
		{
			name:        "error_getting_wc",
			desc:        "Namespace has a default WC, but unexpected error getting WC, expect nil best match and error",
			getNSResult: nsDefault,
			getWCError:  fmt.Errorf("error getting WorkloadClass"),
			wantErr:     true,
		},
		{
			name:          "success_getting_namespace_default",
			desc:          "Namespace has WC, success getting WC, expect updated best match",
			getNSResult:   nsDefault,
			getWCResult:   wc,
			wantBestMatch: wc,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wcResult := tc.getWCResult
			if wcResult == nil {
				wcResult = wc
			}
			c := createTestClient(tc.getNSError, tc.getWCError, nil, tc.getNSResult, wcResult, nil)
			gotWC, err := NamespaceEnforcedWorkloadClass(t.Context(), c, namespace)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NamespaceEnforcedWorkloadClass() error = %v, wantErr: %v", err, tc.wantErr)
			}
			if (gotWC != nil) != (tc.wantBestMatch != nil) {
				t.Errorf("NamespaceEnforcedWorkloadClass() returned an unexpected result, got: %v, want: %v", gotWC, tc.wantBestMatch)
			}
			if gotWC == nil {
				return
			}
			if gotWC.Name != tc.wantBestMatch.Name {
				t.Errorf("NamespaceEnforcedWorkloadClass() returned a different WorkloadClass than expected, got: %v, want: %v", gotWC, tc.wantBestMatch)
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
				"workloads.gke.io/enforced-class": "wc-default",
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
			name: "error_getting_namespace_default",
			desc: "Error getting namespace's enforced WorkloadClass, expect nil result and error without falling back to selector match",
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{wc},
			},
			getNSResult: nsWithDefault,
			getWCError:  fmt.Errorf("error getting WorkloadClass"),
			wantErr:     true,
		},
		{
			name: "success_getting_namespace_default",
			desc: "Success getting namespace-enforced WorkloadClass as best match",
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
			desc: "Success getting best match with selectors (no namespace-enforced WorkloadClass)",
			listWCResult: &workloadsv1.WorkloadClassList{
				Items: []workloadsv1.WorkloadClass{wc},
			},
			getNSResult:   nsNoDefault,
			wantBestMatch: &wc,
			wantErr:       false,
		},
	}

	ctx := t.Context()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := createTestClient(tc.getNSError, tc.getWCError, tc.listWCError, tc.getNSResult, tc.getWCResult, tc.listWCResult)
			gotBestMatch, err := FindBestMatchWorkloadClass(ctx, c, nil, pod)
			if (err != nil) != tc.wantErr {
				t.Errorf("FindBestMatchWorkloadClass returned unexpected error, got: %v, wantErr: %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if (gotBestMatch != nil) != (tc.wantBestMatch != nil) {
				t.Errorf("FindBestMatchWorkloadClass() returned an unexpected result, got: %v, want: %v", gotBestMatch, tc.wantBestMatch)
			}
			if gotBestMatch == nil {
				return
			}
			if gotBestMatch.Name != tc.wantBestMatch.Name {
				t.Errorf("FindBestMatchWorkloadClass() returned a different WorkloadClass than expected, got: %v, want: %v", gotBestMatch, tc.wantBestMatch)
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
	testClient := createTestClient(nil, nil, nil, nsNoDefault, &wc1, listWCResult)
	fakeRecorder := events.NewFakeRecorder(10)

	t.Run("validate_event_emitted", func(t *testing.T) {
		gotBestMatch, err := FindBestMatchWorkloadClass(ctx, testClient, fakeRecorder, pod)
		if err != nil {
			t.Fatalf("FindBestMatchWorkloadClass returned unexpected error: %v", err)
		}
		if gotBestMatch.Name != "wc1" {
			t.Errorf("Expected wc1 to be best match, got: %s", gotBestMatch.Name)
		}
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

func TestBestMatchWorkloadClass_IgnoresOtherNamespaces(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add client-go scheme: %v", err)
	}
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add workloads scheme: %v", err)
	}

	newWC := func(name, namespace string, matchLabels map[string]string) *workloadsv1.WorkloadClass {
		return &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       workloadsv1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels}},
		}
	}
	// The WorkloadClass in another namespace is more specific, so it would win if it were considered.
	sameNS := newWC("same-ns", "team-a", map[string]string{"app": "batch"})
	otherNS := newWC("other-ns", "team-b", map[string]string{"app": "batch", "tier": "gpu"})

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sameNS, otherNS).Build()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "p", Namespace: "team-a", Labels: map[string]string{"app": "batch", "tier": "gpu"},
	}}

	got, err := FindBestMatchWorkloadClass(t.Context(), c, nil, pod)
	if err != nil {
		t.Fatalf("FindBestMatchWorkloadClass returned unexpected error: %v", err)
	}
	if got == nil || got.Name != sameNS.Name {
		t.Errorf("FindBestMatchWorkloadClass() = %v, want %s (WorkloadClasses in other namespaces must be ignored)", got, sameNS.Name)
	}
}

func TestWorkloadClassPodSelector(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add client-go scheme: %v", err)
	}
	if err := workloadsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add workloads scheme: %v", err)
	}

	batchSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "batch"}}
	newNS := func(enforcedClass string) *corev1.Namespace {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}
		if enforcedClass != "" {
			ns.Labels = map[string]string{workloadsv1.EnforcedClassLabel: enforcedClass}
		}
		return ns
	}

	testCases := []struct {
		name          string
		ns            *corev1.Namespace
		podSelector   *metav1.LabelSelector
		podLabels     map[string]string
		wantSelected  bool
		wantErrSubstr string
	}{
		{
			name:         "selector_matches",
			ns:           newNS(""),
			podSelector:  batchSelector,
			podLabels:    map[string]string{"app": "batch"},
			wantSelected: true,
		},
		{
			name:         "selector_does_not_match",
			ns:           newNS(""),
			podSelector:  batchSelector,
			podLabels:    map[string]string{"app": "other"},
			wantSelected: false,
		},
		{
			name:         "nil_selector_selects_all",
			ns:           newNS(""),
			podLabels:    map[string]string{"app": "other"},
			wantSelected: true,
		},
		{
			name:         "namespace_default_selects_all_regardless_of_selector",
			ns:           newNS("wc"),
			podSelector:  batchSelector,
			podLabels:    map[string]string{"app": "other"},
			wantSelected: true,
		},
		{
			name:         "other_namespace_default_uses_selector",
			ns:           newNS("some-other-wc"),
			podSelector:  batchSelector,
			podLabels:    map[string]string{"app": "other"},
			wantSelected: false,
		},
		{
			name:         "missing_namespace_uses_selector",
			podSelector:  batchSelector,
			podLabels:    map[string]string{"app": "other"},
			wantSelected: false,
		},
		{
			name: "invalid_selector_returns_error",
			ns:   newNS(""),
			podSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: "Bogus"},
			}},
			wantErrSubstr: "invalid podSelector",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.ns != nil {
				builder = builder.WithObjects(tc.ns)
			}
			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "team-a"},
				Spec:       workloadsv1.WorkloadClassSpec{PodSelector: tc.podSelector},
			}

			selector, err := WorkloadClassPodSelector(t.Context(), builder.Build(), wc)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("WorkloadClassPodSelector() error = %v, want error containing %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("WorkloadClassPodSelector() returned unexpected error: %v", err)
			}
			if got := selector.Matches(labels.Set(tc.podLabels)); got != tc.wantSelected {
				t.Errorf("selector.Matches(%v) = %v, want %v", tc.podLabels, got, tc.wantSelected)
			}
		})
	}

	t.Run("error_getting_namespace_returns_error", func(t *testing.T) {
		wc := &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "team-a"},
			Spec:       workloadsv1.WorkloadClassSpec{PodSelector: batchSelector},
		}
		c := createTestClient(fmt.Errorf("transient apiserver error"), nil, nil, nil, nil, nil)
		if _, err := WorkloadClassPodSelector(t.Context(), c, wc); err == nil {
			t.Fatal("WorkloadClassPodSelector() error = nil, want error when getting Namespace fails")
		}
	})
}

func TestBestMatchWorkloadClassForPod(t *testing.T) {
	const namespace = "team-a"
	now := metav1.Now()
	older := metav1.NewTime(now.Add(-10 * time.Minute))

	wcSpecific := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-specific",
			Namespace:         namespace,
			CreationTimestamp: now,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "batch", "tier": "gpu"},
			},
		},
	}
	wcOlder := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-older",
			Namespace:         namespace,
			CreationTimestamp: older,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "batch"},
			},
		},
	}
	wcNewer := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-newer",
			Namespace:         namespace,
			CreationTimestamp: now,
		},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "batch"},
			},
		},
	}
	wcDeleting := wcSpecific
	wcDeleting.Name = "wc-deleting"
	wcDeleting.DeletionTimestamp = &now

	wcDefault := workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wc-default",
			Namespace:         namespace,
			CreationTimestamp: now,
		},
	}
	wcDefaultDeleting := wcDefault
	wcDefaultDeleting.DeletionTimestamp = &now

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: namespace,
			Labels:    map[string]string{"app": "batch", "tier": "gpu"},
		},
	}

	testCases := []struct {
		name       string
		pod        *corev1.Pod
		enforcedWC *workloadsv1.WorkloadClass
		allWCs     []workloadsv1.WorkloadClass
		wantName   string
	}{
		{
			name:     "nil_pod_returns_nil",
			pod:      nil,
			allWCs:   []workloadsv1.WorkloadClass{wcSpecific},
			wantName: "",
		},
		{
			name:       "active_default_wc_takes_precedence",
			pod:        pod,
			enforcedWC: &wcDefault,
			allWCs:     []workloadsv1.WorkloadClass{wcSpecific, wcOlder},
			wantName:   "wc-default",
		},
		{
			name:       "deleting_default_wc_falls_back_to_selectors",
			pod:        pod,
			enforcedWC: &wcDefaultDeleting,
			allWCs:     []workloadsv1.WorkloadClass{wcSpecific, wcOlder},
			wantName:   "wc-specific",
		},
		{
			name:     "highest_specificity_wins",
			pod:      pod,
			allWCs:   []workloadsv1.WorkloadClass{wcOlder, wcSpecific, wcNewer},
			wantName: "wc-specific",
		},
		{
			name:     "equal_specificity_oldest_wins",
			pod:      pod,
			allWCs:   []workloadsv1.WorkloadClass{wcNewer, wcOlder},
			wantName: "wc-older",
		},
		{
			name:     "deleting_wc_in_list_is_ignored",
			pod:      pod,
			allWCs:   []workloadsv1.WorkloadClass{wcDeleting, wcOlder},
			wantName: "wc-older",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := BestMatchWorkloadClassForPod(tc.pod, tc.enforcedWC, tc.allWCs)
			if tc.wantName == "" {
				if got != nil {
					t.Errorf("BestMatchWorkloadClassForPod() = %v, want nil", got.Name)
				}
				return
			}
			if got == nil || got.Name != tc.wantName {
				t.Errorf("BestMatchWorkloadClassForPod() = %v, want %q", got, tc.wantName)
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

type fakeWCClient struct {
	client.Client
	listWCError            error
	listWCResult           *workloadsv1.WorkloadClassList
	getNamespaceError      error
	getNamespaceResult     *corev1.Namespace
	getWorkloadClassError  error
	getWorkloadClassResult *workloadsv1.WorkloadClass
}

func createTestClient(getNSErr, getWCErr, listWCErr error, getNSResult *corev1.Namespace, getWCResult *workloadsv1.WorkloadClass, listWCResult *workloadsv1.WorkloadClassList) fakeWCClient {
	return fakeWCClient{
		getNamespaceError:      getNSErr,
		getNamespaceResult:     getNSResult,
		getWorkloadClassError:  getWCErr,
		getWorkloadClassResult: getWCResult,
		listWCError:            listWCErr,
		listWCResult:           listWCResult,
	}
}

func (fc fakeWCClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	fc.listWCResult.DeepCopyInto(list.(*workloadsv1.WorkloadClassList))
	return fc.listWCError
}

func (fc fakeWCClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
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
	default:
		return fmt.Errorf("unknown object type")
	}

	return nil
}

func TestIsValidated(t *testing.T) {
	testCases := []struct {
		name string
		cond *metav1.Condition
		want bool
	}{
		{name: "missing", cond: nil, want: false},
		{name: "true", cond: &metav1.Condition{Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonValidationPassed}, want: true},
		{name: "no_guardrails", cond: &metav1.Condition{Status: metav1.ConditionTrue, Reason: workloadsv1.ReasonNoGuardrails}, want: true},
		{name: "selector_overlap", cond: &metav1.Condition{Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonSelectorOverlap}, want: true},
		{name: "validation_failed", cond: &metav1.Condition{Status: metav1.ConditionFalse, Reason: workloadsv1.ReasonValidationFailed}, want: false},
		{name: "unknown", cond: &metav1.Condition{Status: metav1.ConditionUnknown}, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsValidated(tc.cond); got != tc.want {
				t.Errorf("IsValidated(%+v) = %t, want %t", tc.cond, got, tc.want)
			}
		})
	}
}
