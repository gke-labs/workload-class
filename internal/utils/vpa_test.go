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
	"slices"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/utils/ptr"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestVPAUpdateMode(t *testing.T) {
	inPlace := AutoscalerCapabilities{VPAInstalled: true, InPlaceOrRecreate: true}
	noInPlace := AutoscalerCapabilities{VPAInstalled: true}

	testCases := []struct {
		name         string
		rightsizing  workloadsv1.Rightsizing
		caps         AutoscalerCapabilities
		want         vpav1.UpdateMode
		wantFallback bool
		wantVPA      bool
	}{
		{name: "off", rightsizing: workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff}, caps: inPlace},
		{name: "recommend_by_default", caps: inPlace, want: vpav1.UpdateModeOff, wantVPA: true},
		{name: "recommend", rightsizing: workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeRecommend}, caps: inPlace, want: vpav1.UpdateModeOff, wantVPA: true},
		{name: "on_create", rightsizing: workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOnCreate}, caps: inPlace, want: vpav1.UpdateModeInitial, wantVPA: true},
		{
			name:        "auto_in_place_preferred_by_default",
			rightsizing: workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto},
			caps:        inPlace,
			want:        vpav1.UpdateModeInPlaceOrRecreate,
			wantVPA:     true,
		},
		{
			name:         "auto_in_place_preferred_without_vpa_support",
			rightsizing:  workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto, UpdateStrategy: workloadsv1.RightsizingUpdateStrategyInPlacePreferred},
			caps:         noInPlace,
			want:         vpav1.UpdateModeRecreate,
			wantFallback: true,
			wantVPA:      true,
		},
		{
			name:        "auto_recreate",
			rightsizing: workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto, UpdateStrategy: workloadsv1.RightsizingUpdateStrategyRecreate},
			caps:        noInPlace,
			want:        vpav1.UpdateModeRecreate,
			wantVPA:     true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, fallback, ok := VPAUpdateMode(&tc.rightsizing, tc.caps)
			if got != tc.want || fallback != tc.wantFallback || ok != tc.wantVPA {
				t.Errorf("VPAUpdateMode() = (%q, %t, %t), want (%q, %t, %t)", got, fallback, ok, tc.want, tc.wantFallback, tc.wantVPA)
			}
		})
	}
}

func TestRightsizedResources(t *testing.T) {
	cpu, memory := corev1.ResourceCPU, corev1.ResourceMemory
	memoryUtilization := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ContainerResourceMetricSourceType,
		ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
			Name:      memory,
			Container: "app",
			Target:    autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](80)},
		},
	}
	rm := func(resources []workloadsv1.RightsizedResource, metrics ...autoscalingv2.MetricSpec) *workloadsv1.ResourceManagement {
		rm := &workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{Resources: resources}}
		if len(metrics) > 0 {
			rm.Autoscaling = &workloadsv1.Autoscaling{Replicas: workloadsv1.ReplicaBounds{Max: 5}, Metrics: metrics}
		}
		return rm
	}

	testCases := []struct {
		name           string
		rm             *workloadsv1.ResourceManagement
		caps           AutoscalerCapabilities
		wantControlled []corev1.ResourceName
		wantExcluded   []corev1.ResourceName
	}{
		{name: "no_rightsizing", rm: &workloadsv1.ResourceManagement{}},
		{name: "both_by_default", rm: rm(nil), wantControlled: []corev1.ResourceName{cpu, memory}},
		{name: "memory_only", rm: rm([]workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceMemory}), wantControlled: []corev1.ResourceName{memory}},
		{
			name:           "cpu_utilization_excludes_cpu",
			rm:             rm(nil, cpuUtilizationMetric(60)),
			wantControlled: []corev1.ResourceName{memory},
			wantExcluded:   []corev1.ResourceName{cpu},
		},
		{
			name:           "coordinated_cpu_is_kept",
			rm:             rm(nil, cpuUtilizationMetric(60)),
			caps:           AutoscalerCapabilities{CoordinatedResources: []corev1.ResourceName{cpu}},
			wantControlled: []corev1.ResourceName{cpu, memory},
		},
		{
			name:           "non_utilization_metrics_exclude_nothing",
			rm:             rm(nil, externalMetric()),
			wantControlled: []corev1.ResourceName{cpu, memory},
		},
		{
			name:         "every_resource_excluded",
			rm:           rm(nil, cpuUtilizationMetric(60), memoryUtilization),
			caps:         AutoscalerCapabilities{CoordinatedResources: nil},
			wantExcluded: []corev1.ResourceName{cpu, memory},
		},
		{
			name:         "excluded_resource_that_is_not_rightsized_is_not_reported",
			rm:           rm([]workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceCPU}, cpuUtilizationMetric(60), memoryUtilization),
			wantExcluded: []corev1.ResourceName{cpu},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controlled, excluded := RightsizedResources(tc.rm, tc.caps)
			if !slices.Equal(controlled, tc.wantControlled) || !slices.Equal(excluded, tc.wantExcluded) {
				t.Errorf("RightsizedResources() = (%v, %v), want (%v, %v)", controlled, excluded, tc.wantControlled, tc.wantExcluded)
			}
		})
	}
}

// buildVPATestWorkload, buildVPATestCaps, and newBuildVPATestWC are shared by the BuildVPA tests.
var (
	buildVPATestWorkload = WorkloadRef{Kind: WorkloadKindStatefulSet, Name: "db", Namespace: "shop", UID: "uid-db"}
	buildVPATestCaps     = AutoscalerCapabilities{VPAInstalled: true, InPlaceOrRecreate: true}
)

func newBuildVPATestWC(rm *workloadsv1.ResourceManagement) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "databases", Namespace: "shop"},
		Spec:       workloadsv1.WorkloadClassSpec{ResourceManagement: rm},
	}
}

func TestBuildVPAWithoutRightsizing(t *testing.T) {
	for name, wc := range map[string]*workloadsv1.WorkloadClass{
		"nil_workload_class":     nil,
		"no_resource_management": newBuildVPATestWC(nil),
		"autoscaling_only":       newBuildVPATestWC(&workloadsv1.ResourceManagement{Autoscaling: &workloadsv1.Autoscaling{Replicas: workloadsv1.ReplicaBounds{Max: 3}}}),
		"mode_off":               newBuildVPATestWC(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff}}),
	} {
		got, outcome := BuildVPA(wc, buildVPATestWorkload, EffectiveResourceManagement{}, buildVPATestCaps)
		if got != nil || !reflect.DeepEqual(outcome, VPAOutcome{}) {
			t.Errorf("%s: BuildVPA() = (%+v, %+v), want (nil, empty outcome)", name, got, outcome)
		}
	}
}

func TestBuildVPAAllFields(t *testing.T) {
	wc := newBuildVPATestWC(&workloadsv1.ResourceManagement{
		Autoscaling: &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: 5},
			Metrics:  []autoscalingv2.MetricSpec{cpuUtilizationMetric(60)},
		},
		Rightsizing: &workloadsv1.Rightsizing{
			Mode:              workloadsv1.RightsizingModeAuto,
			UpdateStrategy:    workloadsv1.RightsizingUpdateStrategyInPlacePreferred,
			ExcludeContainers: []string{"istio-proxy", "log-shipper"},
			Bounds:            &workloadsv1.ContainerBounds{Min: resources("100m", "128Mi"), Max: resources("4", "")},
		},
	})
	effective := EffectiveResourceManagement{ContainerBounds: workloadsv1.ContainerBounds{Min: resources("10m", "32Mi"), Max: resources("16", "64Gi")}}

	got, outcome := BuildVPA(wc, buildVPATestWorkload, effective, buildVPATestCaps)
	if !reflect.DeepEqual(outcome, VPAOutcome{ExcludedResources: []corev1.ResourceName{corev1.ResourceCPU}}) {
		t.Errorf("outcome = %+v, want cpu excluded", outcome)
	}
	if got == nil {
		t.Fatal("BuildVPA() = nil, want a VerticalPodAutoscaler")
	}
	wantMeta := metav1.ObjectMeta{
		Name:      "wc-statefulset-db",
		Namespace: "shop",
		Labels:    map[string]string{workloadsv1.ManagedByLabel: workloadsv1.ManagedByLabelValue, workloadsv1.WorkloadClassLabel: "databases"},
	}
	if !reflect.DeepEqual(got.ObjectMeta, wantMeta) {
		t.Errorf("ObjectMeta = %+v, want %+v", got.ObjectMeta, wantMeta)
	}
	wantTarget := &autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db"}
	if !reflect.DeepEqual(got.Spec.TargetRef, wantTarget) {
		t.Errorf("TargetRef = %+v, want %+v", got.Spec.TargetRef, wantTarget)
	}
	if mode := got.Spec.UpdatePolicy.UpdateMode; mode == nil || *mode != vpav1.UpdateModeInPlaceOrRecreate {
		t.Errorf("UpdateMode = %v, want InPlaceOrRecreate", mode)
	}

	policies := got.Spec.ResourcePolicy.ContainerPolicies
	if len(policies) != 3 {
		t.Fatalf("ContainerPolicies = %+v, want 3 policies", policies)
	}
	defaultPolicy := policies[0]
	if defaultPolicy.ContainerName != "*" || defaultPolicy.Mode != nil || defaultPolicy.ControlledValues != nil {
		t.Errorf("default policy = %+v, want container '*' with the default mode and controlled values", defaultPolicy)
	}
	if defaultPolicy.ControlledResources == nil || !slices.Equal(*defaultPolicy.ControlledResources, []corev1.ResourceName{corev1.ResourceMemory}) {
		t.Errorf("ControlledResources = %v, want [memory]", defaultPolicy.ControlledResources)
	}
	// Only controlled resources get bounds. The class sets the memory floor; the guardrail sets the memory ceiling.
	if !equalQuantities(defaultPolicy.MinAllowed, resources("", "128Mi")) {
		t.Errorf("MinAllowed = %v, want memory 128Mi", defaultPolicy.MinAllowed)
	}
	if !equalQuantities(defaultPolicy.MaxAllowed, resources("", "64Gi")) {
		t.Errorf("MaxAllowed = %v, want memory 64Gi", defaultPolicy.MaxAllowed)
	}
	for i, name := range []string{"istio-proxy", "log-shipper"} {
		p := policies[i+1]
		if p.ContainerName != name || p.Mode == nil || *p.Mode != vpav1.ContainerScalingModeOff {
			t.Errorf("policy %d = %+v, want container %q with mode Off", i+1, p, name)
		}
	}
}

func TestBuildVPAGuardrailBoundsApplyWhenTheClassSetsNone(t *testing.T) {
	wc := newBuildVPATestWC(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}})
	effective := EffectiveResourceManagement{ContainerBounds: workloadsv1.ContainerBounds{Min: resources("10m", ""), Max: resources("", "64Gi")}}
	got, outcome := BuildVPA(wc, buildVPATestWorkload, effective, buildVPATestCaps)
	if got == nil || !reflect.DeepEqual(outcome, VPAOutcome{}) {
		t.Fatalf("BuildVPA() = (%+v, %+v), want a VerticalPodAutoscaler and an empty outcome", got, outcome)
	}
	p := got.Spec.ResourcePolicy.ContainerPolicies[0]
	if !slices.Equal(*p.ControlledResources, []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}) {
		t.Errorf("ControlledResources = %v, want [cpu memory]", *p.ControlledResources)
	}
	if !equalQuantities(p.MinAllowed, resources("10m", "")) || !equalQuantities(p.MaxAllowed, resources("", "64Gi")) {
		t.Errorf("bounds = (%v, %v), want the guardrail bounds", p.MinAllowed, p.MaxAllowed)
	}
	if mode := got.Spec.UpdatePolicy.UpdateMode; mode == nil || *mode != vpav1.UpdateModeOff {
		t.Errorf("UpdateMode = %v, want Off (Recommend)", mode)
	}
}

func TestBuildVPANoBounds(t *testing.T) {
	got, _ := BuildVPA(newBuildVPATestWC(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{}}), buildVPATestWorkload, EffectiveResourceManagement{}, buildVPATestCaps)
	if p := got.Spec.ResourcePolicy.ContainerPolicies[0]; p.MinAllowed != nil || p.MaxAllowed != nil {
		t.Errorf("bounds = (%v, %v), want none", p.MinAllowed, p.MaxAllowed)
	}
}

func TestBuildVPAEveryResourceExcluded(t *testing.T) {
	wc := newBuildVPATestWC(&workloadsv1.ResourceManagement{
		Autoscaling: &workloadsv1.Autoscaling{Replicas: workloadsv1.ReplicaBounds{Max: 5}, Metrics: []autoscalingv2.MetricSpec{cpuUtilizationMetric(60)}},
		Rightsizing: &workloadsv1.Rightsizing{
			Mode:      workloadsv1.RightsizingModeAuto,
			Resources: []workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceCPU},
		},
	})
	got, outcome := BuildVPA(wc, buildVPATestWorkload, EffectiveResourceManagement{}, AutoscalerCapabilities{VPAInstalled: true})
	if got != nil {
		t.Errorf("BuildVPA() = %+v, want nil", got)
	}
	if !reflect.DeepEqual(outcome, VPAOutcome{ExcludedResources: []corev1.ResourceName{corev1.ResourceCPU}}) {
		t.Errorf("outcome = %+v, want cpu excluded and no in-place fallback", outcome)
	}
}

func TestBuildVPAInPlaceFallback(t *testing.T) {
	wc := newBuildVPATestWC(&workloadsv1.ResourceManagement{Rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}})
	got, outcome := BuildVPA(wc, buildVPATestWorkload, EffectiveResourceManagement{}, AutoscalerCapabilities{VPAInstalled: true})
	if mode := got.Spec.UpdatePolicy.UpdateMode; mode == nil || *mode != vpav1.UpdateModeRecreate {
		t.Errorf("UpdateMode = %v, want Recreate", mode)
	}
	if !outcome.InPlaceFallback {
		t.Errorf("outcome = %+v, want InPlaceFallback", outcome)
	}
}

func TestParseCoordinatedResources(t *testing.T) {
	testCases := []struct {
		name    string
		values  []string
		want    []corev1.ResourceName
		wantErr bool
	}{
		{name: "empty"},
		{name: "cpu", values: []string{"cpu"}, want: []corev1.ResourceName{corev1.ResourceCPU}},
		{name: "deduplicated", values: []string{"memory", "cpu", "memory"}, want: []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceCPU}},
		{name: "unsupported", values: []string{"cpu", "ephemeral-storage"}, wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseCoordinatedResources(tc.values)
			if (err != nil) != tc.wantErr || !slices.Equal(got, tc.want) {
				t.Errorf("ParseCoordinatedResources(%v) = (%v, %v), want (%v, error: %t)", tc.values, got, err, tc.want, tc.wantErr)
			}
		})
	}
}
