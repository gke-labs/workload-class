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
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func testRightsizingClass(rs *workloadsv1.Rightsizing, metrics ...autoscalingv2.MetricSpec) *workloadsv1.WorkloadClass {
	wc := testAutoscalingClass("web-services", nil)
	wc.Spec.ResourceManagement.Rightsizing = rs
	if len(metrics) > 0 {
		wc.Spec.ResourceManagement.Autoscaling = &workloadsv1.Autoscaling{
			Replicas: workloadsv1.ReplicaBounds{Max: 10},
			Metrics:  metrics,
		}
	}
	return wc
}

func TestVPAUpdateMode(t *testing.T) {
	testCases := []struct {
		name               string
		rightsizing        *workloadsv1.Rightsizing
		inPlaceUnsupported bool
		want               string
	}{
		{name: "not_configured", rightsizing: nil, want: ""},
		{name: "off", rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff}, want: ""},
		{name: "default_mode_is_recommend", rightsizing: &workloadsv1.Rightsizing{}, want: "Off"},
		{name: "recommend", rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeRecommend}, want: "Off"},
		{name: "on_create", rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOnCreate}, want: "Initial"},
		{name: "auto_defaults_to_in_place", rightsizing: &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}, want: "InPlaceOrRecreate"},
		{
			name: "auto_in_place_preferred",
			rightsizing: &workloadsv1.Rightsizing{
				Mode: workloadsv1.RightsizingModeAuto, UpdateStrategy: workloadsv1.RightsizingUpdateStrategyInPlacePreferred,
			},
			want: "InPlaceOrRecreate",
		},
		{
			name:               "auto_in_place_preferred_falls_back_when_unsupported",
			rightsizing:        &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto},
			inPlaceUnsupported: true,
			want:               "Recreate",
		},
		{
			name: "auto_recreate",
			rightsizing: &workloadsv1.Rightsizing{
				Mode: workloadsv1.RightsizingModeAuto, UpdateStrategy: workloadsv1.RightsizingUpdateStrategyRecreate,
			},
			want: "Recreate",
		},
		{
			name:               "in_place_support_does_not_affect_other_modes",
			rightsizing:        &workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOnCreate},
			inPlaceUnsupported: true,
			want:               "Initial",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := VPAUpdateMode(tc.rightsizing, tc.inPlaceUnsupported); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRightsizedResources(t *testing.T) {
	cpu, memory := corev1.ResourceCPU, corev1.ResourceMemory
	cpuUtilization := testUtilizationMetric(cpu, 60)
	memoryUtilization := testUtilizationMetric(memory, 70)

	testCases := []struct {
		name           string
		rm             *workloadsv1.ResourceManagement
		coordinated    []corev1.ResourceName
		wantControlled []corev1.ResourceName
		wantSkipped    []corev1.ResourceName
	}{
		{name: "no_resource_management"},
		{name: "no_rightsizing", rm: testRightsizingClass(nil, cpuUtilization).Spec.ResourceManagement},
		{
			name:           "both_resources_by_default",
			rm:             testRightsizingClass(&workloadsv1.Rightsizing{}).Spec.ResourceManagement,
			wantControlled: []corev1.ResourceName{cpu, memory},
		},
		{
			name: "listed_resources_only",
			rm: testRightsizingClass(&workloadsv1.Rightsizing{
				Resources: []workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceMemory},
			}).Spec.ResourceManagement,
			wantControlled: []corev1.ResourceName{memory},
		},
		{
			// Scaling on CPU utilization leaves CPU to horizontal scaling.
			name:           "utilization_target_is_left_to_horizontal_scaling",
			rm:             testRightsizingClass(&workloadsv1.Rightsizing{}, cpuUtilization).Spec.ResourceManagement,
			wantControlled: []corev1.ResourceName{memory},
			wantSkipped:    []corev1.ResourceName{cpu},
		},
		{
			name:           "coordinated_resource_is_rightsized_too",
			rm:             testRightsizingClass(&workloadsv1.Rightsizing{}, cpuUtilization).Spec.ResourceManagement,
			coordinated:    []corev1.ResourceName{cpu},
			wantControlled: []corev1.ResourceName{cpu, memory},
		},
		{
			name:        "every_resource_is_left_to_horizontal_scaling",
			rm:          testRightsizingClass(&workloadsv1.Rightsizing{}, cpuUtilization, memoryUtilization).Spec.ResourceManagement,
			wantSkipped: []corev1.ResourceName{cpu, memory},
		},
		{
			// A resource that rightsizing was not asked to control is not reported as skipped.
			name: "unlisted_resource_is_not_skipped",
			rm: testRightsizingClass(&workloadsv1.Rightsizing{
				Resources: []workloadsv1.RightsizedResource{workloadsv1.RightsizedResourceMemory},
			}, cpuUtilization).Spec.ResourceManagement,
			wantControlled: []corev1.ResourceName{memory},
		},
		{
			name: "non_utilization_metrics_do_not_conflict",
			rm: testRightsizingClass(&workloadsv1.Rightsizing{},
				autoscalingv2.MetricSpec{Type: autoscalingv2.PodsMetricSourceType}).Spec.ResourceManagement,
			wantControlled: []corev1.ResourceName{cpu, memory},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controlled, skipped := RightsizedResources(tc.rm, tc.coordinated)
			if !slices.Equal(controlled, tc.wantControlled) || !slices.Equal(skipped, tc.wantSkipped) {
				t.Errorf("got controlled=%v skipped=%v, want controlled=%v skipped=%v", controlled, skipped, tc.wantControlled, tc.wantSkipped)
			}
		})
	}
}

func TestBuildVPA(t *testing.T) {
	w := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web"}
	guardrailBounds := EffectiveResourceManagement{
		ContainerBounds: workloadsv1.ContainerBounds{Min: testResources("10m", "32Mi"), Max: testResources("16", "64Gi")},
	}
	spec := func(vpa *unstructured.Unstructured) map[string]any {
		t.Helper()
		got, _, err := unstructured.NestedMap(vpa.Object, "spec")
		if err != nil {
			t.Fatalf("failed to read spec: %v", err)
		}
		return got
	}

	t.Run("no_vpa", func(t *testing.T) {
		for name, wc := range map[string]*workloadsv1.WorkloadClass{
			"nil WorkloadClass":      nil,
			"no resourceManagement":  {},
			"no rightsizing":         testRightsizingClass(nil),
			"mode Off":               testRightsizingClass(&workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeOff}),
			"every resource skipped": testRightsizingClass(&workloadsv1.Rightsizing{}, testUtilizationMetric(corev1.ResourceCPU, 60), testUtilizationMetric(corev1.ResourceMemory, 70)),
		} {
			if got := BuildVPA(wc, w, guardrailBounds, VPACapabilities{}); got != nil {
				t.Errorf("%s: expected no VPA, got %v", name, got.Object)
			}
		}
	})

	t.Run("full", func(t *testing.T) {
		wc := testRightsizingClass(&workloadsv1.Rightsizing{
			Mode:              workloadsv1.RightsizingModeAuto,
			ExcludeContainers: []string{"istio-proxy", "envoy", "istio-proxy"},
			Bounds:            &workloadsv1.ContainerBounds{Min: testResources("100m", "128Mi"), Max: testResources("4", "8Gi")},
		})
		got := BuildVPA(wc, w, guardrailBounds, VPACapabilities{})
		if got == nil {
			t.Fatal("expected a VPA")
		}
		if got.GroupVersionKind() != VPAGroupVersionKind || got.GetName() != "wc-deployment-web" || got.GetNamespace() != "shop" {
			t.Errorf("unexpected identity: %s %s/%s", got.GroupVersionKind(), got.GetNamespace(), got.GetName())
		}
		wantLabels := map[string]string{ManagedByLabel: ManagedByValue, WorkloadClassLabel: "web-services"}
		if !equality.Semantic.DeepEqual(got.GetLabels(), wantLabels) {
			t.Errorf("labels = %v, want %v", got.GetLabels(), wantLabels)
		}
		want := map[string]any{
			"targetRef":    map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
			"updatePolicy": map[string]any{"updateMode": "InPlaceOrRecreate"},
			"resourcePolicy": map[string]any{"containerPolicies": []any{
				map[string]any{
					"containerName":       "*",
					"controlledResources": []any{"cpu", "memory"},
					"minAllowed":          map[string]any{"cpu": "100m", "memory": "128Mi"},
					"maxAllowed":          map[string]any{"cpu": "4", "memory": "8Gi"},
				},
				// Excluded containers are sorted and deduplicated.
				map[string]any{"containerName": "envoy", "mode": "Off"},
				map[string]any{"containerName": "istio-proxy", "mode": "Off"},
			}},
		}
		if !equality.Semantic.DeepEqual(spec(got), want) {
			t.Errorf("spec mismatch\n got: %v\nwant: %v", spec(got), want)
		}
	})

	t.Run("guardrail_bounds_apply_where_the_class_sets_none", func(t *testing.T) {
		wc := testRightsizingClass(&workloadsv1.Rightsizing{
			// The class only bounds the memory maximum.
			Bounds: &workloadsv1.ContainerBounds{Max: testResources("", "8Gi")},
		})
		got := BuildVPA(wc, WorkloadRef{Kind: StatefulSetKind, Namespace: "shop", Name: "db"}, guardrailBounds, VPACapabilities{})
		policies, _, _ := unstructured.NestedSlice(got.Object, "spec", "resourcePolicy", "containerPolicies")
		want := []any{map[string]any{
			"containerName":       "*",
			"controlledResources": []any{"cpu", "memory"},
			"minAllowed":          map[string]any{"cpu": "10m", "memory": "32Mi"},
			"maxAllowed":          map[string]any{"cpu": "16", "memory": "8Gi"},
		}}
		if !equality.Semantic.DeepEqual(policies, want) {
			t.Errorf("containerPolicies mismatch\n got: %v\nwant: %v", policies, want)
		}
		if kind, _, _ := unstructured.NestedString(got.Object, "spec", "targetRef", "kind"); kind != "StatefulSet" || got.GetName() != "wc-statefulset-db" {
			t.Errorf("expected the VPA to target the StatefulSet, got kind %q and name %q", kind, got.GetName())
		}
		if mode := VPAUpdateModeOf(got); mode != "Off" {
			t.Errorf("update mode = %q, want Off for the default Recommend mode", mode)
		}
	})

	t.Run("no_bounds_at_all", func(t *testing.T) {
		got := BuildVPA(testRightsizingClass(&workloadsv1.Rightsizing{}), w, EffectiveResourceManagement{}, VPACapabilities{})
		policies, _, _ := unstructured.NestedSlice(got.Object, "spec", "resourcePolicy", "containerPolicies")
		want := []any{map[string]any{"containerName": "*", "controlledResources": []any{"cpu", "memory"}}}
		if !equality.Semantic.DeepEqual(policies, want) {
			t.Errorf("containerPolicies mismatch\n got: %v\nwant: %v", policies, want)
		}
	})

	t.Run("conflict_rule_and_capabilities", func(t *testing.T) {
		wc := testRightsizingClass(&workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}, testUtilizationMetric(corev1.ResourceCPU, 60))

		// CPU is left to horizontal scaling, and its bounds are left out with it.
		got := BuildVPA(wc, w, guardrailBounds, VPACapabilities{InPlaceUnsupported: true})
		policies, _, _ := unstructured.NestedSlice(got.Object, "spec", "resourcePolicy", "containerPolicies")
		want := []any{map[string]any{
			"containerName":       "*",
			"controlledResources": []any{"memory"},
			"minAllowed":          map[string]any{"memory": "32Mi"},
			"maxAllowed":          map[string]any{"memory": "64Gi"},
		}}
		if !equality.Semantic.DeepEqual(policies, want) {
			t.Errorf("containerPolicies mismatch\n got: %v\nwant: %v", policies, want)
		}
		if mode := VPAUpdateModeOf(got); mode != "Recreate" {
			t.Errorf("update mode = %q, want the Recreate fallback", mode)
		}

		// A platform that coordinates CPU rightsizes it as well.
		coordinated := BuildVPA(wc, w, guardrailBounds, VPACapabilities{CoordinatedResources: []corev1.ResourceName{corev1.ResourceCPU}})
		controlled, _, _ := unstructured.NestedSlice(coordinated.Object, "spec", "resourcePolicy", "containerPolicies")
		if got := controlled[0].(map[string]any)["controlledResources"]; !equality.Semantic.DeepEqual(got, []any{"cpu", "memory"}) {
			t.Errorf("controlledResources = %v, want cpu and memory", got)
		}
	})
}

func TestVPATargetsWorkload(t *testing.T) {
	w := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web"}
	vpa := BuildVPA(testRightsizingClass(&workloadsv1.Rightsizing{}), w, EffectiveResourceManagement{}, VPACapabilities{})
	if !VPATargetsWorkload(vpa, w) {
		t.Errorf("expected the VPA to target its workload")
	}
	if VPATargetsWorkload(vpa, WorkloadRef{Kind: StatefulSetKind, Name: "web"}) || VPATargetsWorkload(vpa, WorkloadRef{Kind: DeploymentKind, Name: "api"}) {
		t.Errorf("expected the VPA not to target other workloads")
	}
	if VPATargetsWorkload(nil, w) || VPATargetsWorkload(&unstructured.Unstructured{Object: map[string]any{}}, w) {
		t.Errorf("expected a VPA without a targetRef not to target the workload")
	}
	if VPAUpdateModeOf(nil) != "" {
		t.Errorf("expected no update mode for a nil VPA")
	}
}

func TestApplyVPASpec(t *testing.T) {
	w := WorkloadRef{Kind: DeploymentKind, Namespace: "shop", Name: "web"}
	desired := BuildVPA(testRightsizingClass(&workloadsv1.Rightsizing{Mode: workloadsv1.RightsizingModeAuto}), w, EffectiveResourceManagement{}, VPACapabilities{})

	// Fields that BuildVPA does not manage are kept; managed fields are replaced.
	existing := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"targetRef":    map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "old"},
			"updatePolicy": map[string]any{"updateMode": "Off", "minReplicas": int64(2)},
			"recommenders": []any{map[string]any{"name": "custom"}},
			"resourcePolicy": map[string]any{"containerPolicies": []any{
				map[string]any{"containerName": "stale", "mode": "Off"},
			}},
		},
		"status": map[string]any{"recommendation": map[string]any{}},
	}}
	if err := ApplyVPASpec(existing, desired); err != nil {
		t.Fatalf("ApplyVPASpec failed: %v", err)
	}
	want := map[string]any{
		"targetRef":    map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
		"updatePolicy": map[string]any{"updateMode": "InPlaceOrRecreate", "minReplicas": int64(2)},
		"recommenders": []any{map[string]any{"name": "custom"}},
		"resourcePolicy": map[string]any{"containerPolicies": []any{
			map[string]any{"containerName": "*", "controlledResources": []any{"cpu", "memory"}},
		}},
	}
	got, _, _ := unstructured.NestedMap(existing.Object, "spec")
	if !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("spec mismatch\n got: %v\nwant: %v", got, want)
	}
	if _, found, _ := unstructured.NestedMap(existing.Object, "status"); !found {
		t.Errorf("expected the status to be kept")
	}

	// The desired VPA is not aliased by the existing one.
	policies, _, _ := unstructured.NestedSlice(existing.Object, "spec", "resourcePolicy", "containerPolicies")
	policies[0].(map[string]any)["containerName"] = "changed"
	_ = unstructured.SetNestedSlice(existing.Object, policies, "spec", "resourcePolicy", "containerPolicies")
	desiredPolicies, _, _ := unstructured.NestedSlice(desired.Object, "spec", "resourcePolicy", "containerPolicies")
	if desiredPolicies[0].(map[string]any)["containerName"] != "*" {
		t.Errorf("ApplyVPASpec shares data with the desired VPA")
	}

	// Applying onto an empty object creates the spec.
	empty := &unstructured.Unstructured{Object: map[string]any{}}
	if err := ApplyVPASpec(empty, desired); err != nil {
		t.Fatalf("ApplyVPASpec failed: %v", err)
	}
	if !equality.Semantic.DeepEqual(empty.Object["spec"], desired.Object["spec"]) {
		t.Errorf("spec mismatch on an empty object\n got: %v\nwant: %v", empty.Object["spec"], desired.Object["spec"])
	}
}

func TestParseCoordinatedResources(t *testing.T) {
	testCases := []struct {
		value   string
		want    []corev1.ResourceName
		wantErr bool
	}{
		{value: ""},
		{value: " , "},
		{value: "cpu", want: []corev1.ResourceName{corev1.ResourceCPU}},
		{value: "memory, cpu ,cpu", want: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}},
		{value: "cpu,gpu", wantErr: true},
		{value: "CPU", wantErr: true},
	}
	for _, tc := range testCases {
		got, err := ParseCoordinatedResources(tc.value)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseCoordinatedResources(%q) error = %v, wantErr %v", tc.value, err, tc.wantErr)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("ParseCoordinatedResources(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}
