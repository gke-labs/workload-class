package utils

import (
	"context"
	"fmt"
	"testing"
	"time"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

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
				"workloads.gke.io/default-class": "wc-default",
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
			recorder := events.NewFakeRecorder(10)
			gotBestMatch, err := BestMatchWorkloadClass(ctx, client, recorder, pod)
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
	fakeRecorder := events.NewFakeRecorder(10)

	t.Run("validate_event_emitted", func(t *testing.T) {
		gotBestMatch, err := BestMatchWorkloadClass(ctx, testClient, fakeRecorder, pod)
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
			Labels: map[string]string{
				defaultClassAnnotation: "wc",
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
			gotWC := NamespaceDefaultWorkloadClass(ctx, client, pod)
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
