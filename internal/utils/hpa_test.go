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
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func testUtilizationMetric(name corev1.ResourceName, utilization int32) autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   name,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To(utilization)},
		},
	}
}

func testAutoscalingClass(name string, a *workloadsv1.Autoscaling) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shop"},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector:        &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "web"}},
			ResourceManagement: &workloadsv1.ResourceManagement{Autoscaling: a},
		},
	}
}

func TestWorkloadRefFor(t *testing.T) {
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"tier": "web"}}}
	meta := metav1.ObjectMeta{Name: "web", Namespace: "shop"}

	got, ok := WorkloadRefFor(&appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Template: template}})
	want := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web", TemplateLabels: map[string]string{"tier": "web"}}
	if !ok || !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("Deployment: got %+v, %v, want %+v", got, ok, want)
	}

	got, ok = WorkloadRefFor(&appsv1.StatefulSet{ObjectMeta: meta, Spec: appsv1.StatefulSetSpec{Template: template}})
	want.Kind = StatefulSetKind
	if !ok || !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("StatefulSet: got %+v, %v, want %+v", got, ok, want)
	}

	if _, ok := WorkloadRefFor(&appsv1.DaemonSet{ObjectMeta: meta}); ok {
		t.Errorf("expected a DaemonSet not to be a governed workload")
	}
}

func TestBestMatchWorkloadClassForTemplate(t *testing.T) {
	older := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	web := testAutoscalingClass("web", nil)
	web.CreationTimestamp = older
	webCanary := testAutoscalingClass("web-canary", nil)
	webCanary.Spec.PodSelector.MatchLabels = map[string]string{"tier": "web", "track": "canary"}
	otherNamespace := testAutoscalingClass("other-namespace", nil)
	otherNamespace.Namespace = "other"
	enforced := testAutoscalingClass("enforced", nil)
	enforced.Spec.PodSelector = nil
	classes := []workloadsv1.WorkloadClass{*web, *webCanary, *otherNamespace}

	testCases := []struct {
		name     string
		labels   map[string]string
		enforced *workloadsv1.WorkloadClass
		want     string
	}{
		{name: "matches_the_template_labels", labels: map[string]string{"tier": "web"}, want: "web"},
		{name: "most_specific_selector_wins", labels: map[string]string{"tier": "web", "track": "canary"}, want: "web-canary"},
		{name: "no_match", labels: map[string]string{"tier": "db"}},
		{name: "template_without_labels", labels: nil},
		{name: "enforced_class_governs_every_workload", labels: map[string]string{"tier": "db"}, enforced: enforced, want: "enforced"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web", TemplateLabels: tc.labels}
			got := BestMatchWorkloadClassForTemplate(w, tc.enforced, classes)
			switch {
			case tc.want == "" && got != nil:
				t.Errorf("expected no WorkloadClass, got %q", got.Name)
			case tc.want != "" && (got == nil || got.Name != tc.want):
				t.Errorf("got %v, want %q", got, tc.want)
			}
		})
	}
}

func TestManagedAutoscalerName(t *testing.T) {
	if got, want := ManagedAutoscalerName(DeploymentKind, "web"), "wc-deployment-web"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := ManagedAutoscalerName(StatefulSetKind, "web"), "wc-statefulset-web"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// A workload name at the 253-character limit does not fit after adding the prefix.
	long := strings.Repeat("a", 252) + "b"
	alsoLong := strings.Repeat("a", 252) + "c"
	got := ManagedAutoscalerName(DeploymentKind, long)
	if len(got) != MaxManagedAutoscalerNameLength {
		t.Errorf("truncated name has length %d, want %d", len(got), MaxManagedAutoscalerNameLength)
	}
	if !strings.HasPrefix(got, "wc-deployment-aaa") {
		t.Errorf("truncated name %q lost its prefix", got)
	}
	if got != ManagedAutoscalerName(DeploymentKind, long) {
		t.Errorf("truncated names must be stable")
	}
	if got == ManagedAutoscalerName(DeploymentKind, alsoLong) {
		t.Errorf("workloads that only differ after the truncation point must get distinct names")
	}
	if got == ManagedAutoscalerName(StatefulSetKind, long) {
		t.Errorf("a Deployment and a StatefulSet with the same name must get distinct names")
	}
}

func TestManagedAutoscalerLabels(t *testing.T) {
	labels := ManagedAutoscalerLabels(testAutoscalingClass("web-services", nil))
	want := map[string]string{ManagedByLabel: ManagedByValue, WorkloadClassLabel: "web-services"}
	if !equality.Semantic.DeepEqual(labels, want) {
		t.Errorf("labels = %v, want %v", labels, want)
	}

	// WorkloadClass names can be longer than a label value.
	longName := strings.Repeat("x", 100)
	value := ManagedAutoscalerLabels(testAutoscalingClass(longName, nil))[WorkloadClassLabel]
	if len(value) != 63 || !strings.HasPrefix(value, "xxxx") {
		t.Errorf("label value %q (length %d) is not a truncated 63-character value", value, len(value))
	}
	if other := ManagedAutoscalerLabels(testAutoscalingClass(longName+"y", nil))[WorkloadClassLabel]; other == value {
		t.Errorf("distinct long WorkloadClass names must get distinct label values")
	}
}

func TestIsWorkloadClassManagedAutoscaler(t *testing.T) {
	managed := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{ManagedByLabel: ManagedByValue}}}
	if !IsWorkloadClassManagedAutoscaler(managed) {
		t.Errorf("expected a labeled autoscaler to be managed")
	}
	for _, user := range []*autoscalingv2.HorizontalPodAutoscaler{
		{},
		{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{ManagedByLabel: "helm"}}},
	} {
		if IsWorkloadClassManagedAutoscaler(user) {
			t.Errorf("expected an autoscaler with labels %v to be user-created", user.Labels)
		}
	}
}

func TestBuildHPA(t *testing.T) {
	w := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web"}

	if BuildHPA(nil, w) != nil || BuildHPA(testAutoscalingClass("web", nil), w) != nil {
		t.Errorf("expected no HPA without autoscaling")
	}
	if got := BuildHPA(&workloadsv1.WorkloadClass{}, w); got != nil {
		t.Errorf("expected no HPA without resourceManagement, got %+v", got)
	}

	wc := testAutoscalingClass("web-services", &workloadsv1.Autoscaling{
		Replicas: workloadsv1.ReplicaBounds{Min: ptr.To[int32](2), Max: 20},
		Metrics:  []autoscalingv2.MetricSpec{testUtilizationMetric(corev1.ResourceCPU, 60)},
		Profile:  workloadsv1.AutoscalingProfileConservative,
	})
	got := BuildHPA(wc, w)
	want := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wc-deployment-web",
			Namespace: "shop",
			Labels:    map[string]string{ManagedByLabel: ManagedByValue, WorkloadClassLabel: "web-services"},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
			MinReplicas:    ptr.To[int32](2),
			MaxReplicas:    20,
			Metrics:        []autoscalingv2.MetricSpec{testUtilizationMetric(corev1.ResourceCPU, 60)},
			Behavior:       AutoscalingBehavior(workloadsv1.AutoscalingProfileConservative),
		},
	}
	if !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("HPA mismatch\n got: %+v\nwant: %+v", got, want)
	}

	// The metrics are copied, so changing the HPA never changes the cached WorkloadClass.
	*got.Spec.Metrics[0].Resource.Target.AverageUtilization = 99
	if v := *wc.Spec.ResourceManagement.Autoscaling.Metrics[0].Resource.Target.AverageUtilization; v != 60 {
		t.Errorf("BuildHPA shares the metrics with the WorkloadClass: averageUtilization is now %d", v)
	}

	// Defaults: replicas.min is 1 and the profile is Balanced. A StatefulSet is targeted by its own kind.
	defaults := BuildHPA(testAutoscalingClass("web", &workloadsv1.Autoscaling{
		Replicas: workloadsv1.ReplicaBounds{Max: 5},
		Metrics:  []autoscalingv2.MetricSpec{testUtilizationMetric(corev1.ResourceCPU, 60)},
	}), WorkloadRef{Kind: StatefulSetKind, Namespace: "shop", Name: "db"})
	if defaults.Name != "wc-statefulset-db" || defaults.Spec.ScaleTargetRef.Kind != StatefulSetKind {
		t.Errorf("unexpected StatefulSet HPA: name %q, target %+v", defaults.Name, defaults.Spec.ScaleTargetRef)
	}
	if defaults.Spec.MinReplicas == nil || *defaults.Spec.MinReplicas != 1 {
		t.Errorf("minReplicas = %v, want 1", defaults.Spec.MinReplicas)
	}
	if !equality.Semantic.DeepEqual(defaults.Spec.Behavior, AutoscalingBehavior(workloadsv1.AutoscalingProfileBalanced)) {
		t.Errorf("expected the Balanced behavior by default, got %+v", defaults.Spec.Behavior)
	}
}

func TestAutoscalingBehavior(t *testing.T) {
	percent := func(value, period int32) autoscalingv2.HPAScalingPolicy {
		return autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PercentScalingPolicy, Value: value, PeriodSeconds: period}
	}
	pods := func(value, period int32) autoscalingv2.HPAScalingPolicy {
		return autoscalingv2.HPAScalingPolicy{Type: autoscalingv2.PodsScalingPolicy, Value: value, PeriodSeconds: period}
	}
	testCases := []struct {
		profile                  workloadsv1.AutoscalingProfile
		upWindow, downWindow     int32
		upPolicies, downPolicies []autoscalingv2.HPAScalingPolicy
	}{
		{workloadsv1.AutoscalingProfileResponsive, 0, 60, []autoscalingv2.HPAScalingPolicy{percent(100, 15), pods(4, 15)}, []autoscalingv2.HPAScalingPolicy{percent(100, 15)}},
		// Balanced is the upstream HPA default behavior.
		{workloadsv1.AutoscalingProfileBalanced, 0, 300, []autoscalingv2.HPAScalingPolicy{percent(100, 15), pods(4, 15)}, []autoscalingv2.HPAScalingPolicy{percent(100, 15)}},
		{workloadsv1.AutoscalingProfileConservative, 60, 600, []autoscalingv2.HPAScalingPolicy{percent(50, 60)}, []autoscalingv2.HPAScalingPolicy{percent(10, 60)}},
		{"", 0, 300, []autoscalingv2.HPAScalingPolicy{percent(100, 15), pods(4, 15)}, []autoscalingv2.HPAScalingPolicy{percent(100, 15)}},
	}
	for _, tc := range testCases {
		t.Run("profile_"+string(tc.profile), func(t *testing.T) {
			b := AutoscalingBehavior(tc.profile)
			for _, rules := range []struct {
				name     string
				got      *autoscalingv2.HPAScalingRules
				window   int32
				policies []autoscalingv2.HPAScalingPolicy
			}{
				{"scaleUp", b.ScaleUp, tc.upWindow, tc.upPolicies},
				{"scaleDown", b.ScaleDown, tc.downWindow, tc.downPolicies},
			} {
				if rules.got == nil || rules.got.StabilizationWindowSeconds == nil || *rules.got.StabilizationWindowSeconds != rules.window {
					t.Errorf("%s stabilization window = %+v, want %d", rules.name, rules.got, rules.window)
					continue
				}
				if rules.got.SelectPolicy == nil || *rules.got.SelectPolicy != autoscalingv2.MaxChangePolicySelect {
					t.Errorf("%s selectPolicy = %v, want Max", rules.name, rules.got.SelectPolicy)
				}
				if !slices.Equal(rules.got.Policies, rules.policies) {
					t.Errorf("%s policies = %+v, want %+v", rules.name, rules.got.Policies, rules.policies)
				}
			}
		})
	}
}

func TestUtilizationTargetedResources(t *testing.T) {
	averageValue := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   corev1.ResourceMemory,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType},
		},
	}
	containerMemory := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ContainerResourceMetricSourceType,
		ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
			Name:      corev1.ResourceMemory,
			Container: "app",
			Target:    autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](70)},
		},
	}
	testCases := []struct {
		name    string
		metrics []autoscalingv2.MetricSpec
		want    []corev1.ResourceName
	}{
		{name: "no_metrics"},
		{name: "cpu_utilization", metrics: []autoscalingv2.MetricSpec{testUtilizationMetric(corev1.ResourceCPU, 60)}, want: []corev1.ResourceName{corev1.ResourceCPU}},
		{name: "average_value_is_not_utilization", metrics: []autoscalingv2.MetricSpec{averageValue}},
		{name: "pods_and_external_metrics", metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.PodsMetricSourceType}, {Type: autoscalingv2.ExternalMetricSourceType}}},
		{name: "container_resource_utilization", metrics: []autoscalingv2.MetricSpec{containerMemory}, want: []corev1.ResourceName{corev1.ResourceMemory}},
		{
			name: "sorted_without_duplicates",
			metrics: []autoscalingv2.MetricSpec{
				containerMemory, testUtilizationMetric(corev1.ResourceCPU, 60), testUtilizationMetric(corev1.ResourceMemory, 80), averageValue,
			},
			want: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
		},
		{name: "resource_metric_without_a_source", metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := UtilizationTargetedResources(tc.metrics); !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScaleTargetsWorkload(t *testing.T) {
	w := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web"}
	testCases := []struct {
		apiVersion, kind, name string
		want                   bool
	}{
		{"apps/v1", "Deployment", "web", true},
		{"apps/v1beta2", "Deployment", "web", true},
		{"apps/v1", "Deployment", "api", false},
		{"apps/v1", "StatefulSet", "web", false},
		{"argoproj.io/v1alpha1", "Deployment", "web", false},
		{"v1", "Deployment", "web", false},
		{"", "Deployment", "web", false},
	}
	for _, tc := range testCases {
		if got := ScaleTargetsWorkload(tc.apiVersion, tc.kind, tc.name, w); got != tc.want {
			t.Errorf("ScaleTargetsWorkload(%q, %q, %q) = %v, want %v", tc.apiVersion, tc.kind, tc.name, got, tc.want)
		}
	}
}
