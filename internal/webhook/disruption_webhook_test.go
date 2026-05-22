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
	"testing"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestGetSpecificity(t *testing.T) {
	testCases := []struct {
		name             string
		matchLabels      map[string]string
		matchExpressions []metav1.LabelSelectorRequirement
		want             int
	}{
		{
			name: "nil_selector",
			want: 0,
		},
		{
			name: "nil_match_labels_not_nil_match_expressions",
			matchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "k1", Operator: "op1", Values: []string{"v1"}},
				{Key: "k2", Operator: "op2", Values: []string{"v2"}},
			},
			want: 2,
		},
		{
			name:        "match_labels_nil_match_expressions",
			matchLabels: map[string]string{"k1": "v1", "k2": "v2"},
			want:        2,
		},
		{
			name:        "both_not_nil",
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
		wc           *workloadsv1.WorkloadClass
		bm           *workloadsv1.WorkloadClass
		maxSpec      int
		otherMatches []string

		wantBM           *workloadsv1.WorkloadClass
		wantMaxSpec      int
		wantOtherMatches []string
	}{
		{
			name:             "nil_best_match,_-1_specificity_update_to_new_best_match",
			wc:               wc1,
			otherMatches:     []string{},
			maxSpec:          -1,
			wantBM:           wc1,
			wantMaxSpec:      1,
			wantOtherMatches: []string{},
		},
		{
			name:             "not_nil_best_match_N_specificty_spec_>_N_update_to_new_best_match",
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
		bestMatch     *workloadsv1.WorkloadClass
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		wantBestMatch *workloadsv1.WorkloadClass
	}{
		{
			name:          "best_match_not_nil",
			bestMatch:     wc,
			wantBestMatch: wc,
		},
		{
			name:       "error_getting_namespace",
			getNSError: fmt.Errorf("error getting Namespace"),
		},
		{
			name:        "no_default_class_with_namespace",
			getNSResult: nsNoAnnotation,
		},
		{
			name:        "error_getting_wc",
			getNSResult: nsDefault,
			getWCError:  fmt.Errorf("error getting WorkloadClass"),
		},
		{
			name:          "success_getting_namespace_default",
			getNSResult:   nsDefault,
			wantBestMatch: wc,
		},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := createClient(tc.getNSError, tc.getWCError, nil, tc.getNSResult, wc, nil)
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
			listWCError:  fmt.Errorf("error listing WorkloadClasses"),
			listWCResult: &workloadsv1.WorkloadClassList{},
			wantErr:      true,
		},
		{
			name: "success",
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
			client := createClient(tc.getNSError, tc.getWCError, tc.listWCError, tc.getNSResult, &wc, tc.listWCResult)
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
	listError  error
	listResult *workloadsv1.WorkloadClassList

	getNamespaceError  error
	getNamespaceResult *corev1.Namespace

	getWorkloadClassError  error
	getWorkloadClassResult *workloadsv1.WorkloadClass
}

func createClient(getNSErr, getWCErr, listErr error, getNSResult *corev1.Namespace, getWCResult *workloadsv1.WorkloadClass, listWCResult *workloadsv1.WorkloadClassList) fakeClient {
	return fakeClient{
		getNamespaceError:      getNSErr,
		getNamespaceResult:     getNSResult,
		getWorkloadClassError:  getWCErr,
		getWorkloadClassResult: getWCResult,
		listError:              listErr,
		listResult:             listWCResult,
	}
}

func (fc fakeClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	fc.listResult.DeepCopyInto(list.(*workloadsv1.WorkloadClassList))
	return fc.listError
}

func (fc fakeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch typedObj := obj.(type) {
	case *corev1.Namespace:
		if fc.getNamespaceError != nil {
			return fc.getNamespaceError
		}
		fc.getNamespaceResult.DeepCopyInto(typedObj)
		return nil
	case *workloadsv1.WorkloadClass:
		if fc.getWorkloadClassError != nil {
			return fc.getWorkloadClassError
		}
		fc.getWorkloadClassResult.DeepCopyInto(typedObj)
		return nil
	default:
		return fmt.Errorf("unknown object type")
	}
}
