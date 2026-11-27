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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func testContainer(name string, requests, limits corev1.ResourceList) corev1.Container {
	return corev1.Container{Name: name, Image: "busybox", Resources: corev1.ResourceRequirements{Requests: requests, Limits: limits}}
}

func TestApplyInitialResources(t *testing.T) {
	initial := func(policy workloadsv1.LimitPolicy, exclude ...string) *workloadsv1.InitialResources {
		return &workloadsv1.InitialResources{
			Requests:          testResources("250m", "256Mi"),
			LimitPolicy:       policy,
			ExcludeContainers: exclude,
		}
	}

	testCases := []struct {
		name       string
		ir         *workloadsv1.InitialResources
		containers []corev1.Container
		want       []corev1.Container
		wantFilled []string
	}{
		{
			name:       "no_requests_or_limits_with_default_policy",
			ir:         initial(""),
			containers: []corev1.Container{testContainer("app", nil, nil)},
			want:       []corev1.Container{testContainer("app", testResources("250m", "256Mi"), testResources("", "256Mi"))},
			wantFilled: []string{"cpu-request", "memory-request", "memory-limit"},
		},
		{
			name:       "missing_cpu_request_only",
			ir:         initial(workloadsv1.LimitPolicyMemoryEqualsRequest),
			containers: []corev1.Container{testContainer("app", testResources("", "1Gi"), nil)},
			// The memory limit follows the request that the container set, not the initial request.
			want:       []corev1.Container{testContainer("app", testResources("250m", "1Gi"), testResources("", "1Gi"))},
			wantFilled: []string{"cpu-request", "memory-limit"},
		},
		{
			name:       "missing_memory_request_only",
			ir:         initial(workloadsv1.LimitPolicyMemoryEqualsRequest),
			containers: []corev1.Container{testContainer("app", testResources("2", ""), nil)},
			want:       []corev1.Container{testContainer("app", testResources("2", "256Mi"), testResources("", "256Mi"))},
			wantFilled: []string{"memory-request", "memory-limit"},
		},
		{
			name:       "requests_and_limits_already_set",
			ir:         initial(workloadsv1.LimitPolicyEqualsRequest),
			containers: []corev1.Container{testContainer("app", testResources("1", "1Gi"), testResources("2", "2Gi"))},
			want:       []corev1.Container{testContainer("app", testResources("1", "1Gi"), testResources("2", "2Gi"))},
		},
		{
			name:       "equals_request_policy_sets_both_limits",
			ir:         initial(workloadsv1.LimitPolicyEqualsRequest),
			containers: []corev1.Container{testContainer("app", nil, nil)},
			want:       []corev1.Container{testContainer("app", testResources("250m", "256Mi"), testResources("250m", "256Mi"))},
			wantFilled: []string{"cpu-request", "memory-request", "cpu-limit", "memory-limit"},
		},
		{
			name:       "equals_request_policy_keeps_an_existing_limit",
			ir:         initial(workloadsv1.LimitPolicyEqualsRequest),
			containers: []corev1.Container{testContainer("app", testResources("1", "1Gi"), testResources("", "4Gi"))},
			want:       []corev1.Container{testContainer("app", testResources("1", "1Gi"), testResources("1", "4Gi"))},
			wantFilled: []string{"cpu-limit"},
		},
		{
			name:       "none_policy_leaves_limits_unset",
			ir:         initial(workloadsv1.LimitPolicyNone),
			containers: []corev1.Container{testContainer("app", nil, nil)},
			want:       []corev1.Container{testContainer("app", testResources("250m", "256Mi"), nil)},
			wantFilled: []string{"cpu-request", "memory-request"},
		},
		{
			// The API server copies a limit into a missing request before admission. If a request is still
			// missing here, the resource is left alone so that a request is never written above the limit.
			name:       "limit_without_request_is_left_alone",
			ir:         initial(workloadsv1.LimitPolicyMemoryEqualsRequest),
			containers: []corev1.Container{testContainer("app", nil, testResources("100m", "64Mi"))},
			want:       []corev1.Container{testContainer("app", nil, testResources("100m", "64Mi"))},
		},
		{
			name: "excluded_containers_are_left_alone",
			ir:   initial(workloadsv1.LimitPolicyMemoryEqualsRequest, "istio-proxy"),
			containers: []corev1.Container{
				testContainer("app", nil, nil),
				testContainer("istio-proxy", nil, nil),
			},
			want: []corev1.Container{
				testContainer("app", testResources("250m", "256Mi"), testResources("", "256Mi")),
				testContainer("istio-proxy", nil, nil),
			},
			wantFilled: []string{"cpu-request", "memory-request", "memory-limit"},
		},
		{
			name: "each_container_is_filled_separately",
			ir:   initial(workloadsv1.LimitPolicyNone),
			containers: []corev1.Container{
				testContainer("has-cpu", testResources("1", ""), nil),
				testContainer("has-memory", testResources("", "1Gi"), nil),
				testContainer("has-both", testResources("1", "1Gi"), nil),
			},
			want: []corev1.Container{
				testContainer("has-cpu", testResources("1", "256Mi"), nil),
				testContainer("has-memory", testResources("250m", "1Gi"), nil),
				testContainer("has-both", testResources("1", "1Gi"), nil),
			},
			wantFilled: []string{"cpu-request", "memory-request"},
		},
		{
			name: "non_positive_initial_requests_are_never_written",
			ir: &workloadsv1.InitialResources{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0"), corev1.ResourceMemory: resource.MustParse("-1Mi")},
			},
			containers: []corev1.Container{testContainer("app", nil, nil)},
			want:       []corev1.Container{testContainer("app", nil, nil)},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: tc.containers}}
			filled, skipReason := ApplyInitialResources(pod, tc.ir)
			if skipReason != "" {
				t.Errorf("unexpected skip reason %q", skipReason)
			}
			if !slices.Equal(filled, tc.wantFilled) {
				t.Errorf("filled = %v, want %v", filled, tc.wantFilled)
			}
			if !equality.Semantic.DeepEqual(pod.Spec.Containers, tc.want) {
				t.Errorf("containers mismatch\n got: %+v\nwant: %+v", pod.Spec.Containers, tc.want)
			}

			// Applying initial resources again changes nothing.
			again, _ := ApplyInitialResources(pod, tc.ir)
			if len(again) != 0 {
				t.Errorf("second call filled %v, want nothing", again)
			}
			if !equality.Semantic.DeepEqual(pod.Spec.Containers, tc.want) {
				t.Errorf("second call changed the containers: %+v", pod.Spec.Containers)
			}
		})
	}
}

func TestApplyInitialResourcesLeavesOtherContainersAlone(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{
			testContainer("init", nil, nil),
			{Name: "native-sidecar", Image: "busybox", RestartPolicy: &always},
		},
		Containers:          []corev1.Container{testContainer("app", nil, nil)},
		EphemeralContainers: []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}},
	}}

	filled, _ := ApplyInitialResources(pod, &workloadsv1.InitialResources{Requests: testResources("250m", "256Mi")})
	if want := []string{"cpu-request", "memory-request", "memory-limit"}; !slices.Equal(filled, want) {
		t.Errorf("filled = %v, want %v", filled, want)
	}
	for _, c := range pod.Spec.InitContainers {
		if len(c.Resources.Requests)+len(c.Resources.Limits) != 0 {
			t.Errorf("init container %q was changed: %+v", c.Name, c.Resources)
		}
	}
	if r := pod.Spec.EphemeralContainers[0].Resources; len(r.Requests)+len(r.Limits) != 0 {
		t.Errorf("ephemeral container was changed: %+v", r)
	}
}

func TestApplyInitialResourcesSkipsPodLevelResources(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Resources:  &corev1.ResourceRequirements{Requests: testResources("1", "1Gi")},
		Containers: []corev1.Container{testContainer("app", nil, nil)},
	}}

	filled, skipReason := ApplyInitialResources(pod, &workloadsv1.InitialResources{Requests: testResources("250m", "256Mi")})
	if skipReason != InitialResourcesSkipPodLevelResources || len(filled) != 0 {
		t.Errorf("got filled=%v skipReason=%q, want the Pod to be skipped for Pod-level resources", filled, skipReason)
	}
	if r := pod.Spec.Containers[0].Resources; len(r.Requests)+len(r.Limits) != 0 {
		t.Errorf("container was changed: %+v", r)
	}
}

func TestApplyInitialResourcesNilInputs(t *testing.T) {
	if filled, skipReason := ApplyInitialResources(nil, &workloadsv1.InitialResources{}); filled != nil || skipReason != "" {
		t.Errorf("nil Pod: got %v, %q", filled, skipReason)
	}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{testContainer("app", nil, nil)}}}
	if filled, skipReason := ApplyInitialResources(pod, nil); filled != nil || skipReason != "" {
		t.Errorf("nil initial resources: got %v, %q", filled, skipReason)
	}
}

func TestInitialResourcesAnnotationValue(t *testing.T) {
	testCases := []struct {
		filled     []string
		skipReason string
		want       string
	}{
		{want: ""},
		{filled: []string{"cpu-request", "memory-request", "memory-limit"}, want: "cpu-request,memory-request,memory-limit"},
		{skipReason: InitialResourcesSkipPodLevelResources, want: "skipped: pod-level-resources"},
	}
	for _, tc := range testCases {
		if got := InitialResourcesAnnotationValue(tc.filled, tc.skipReason); got != tc.want {
			t.Errorf("InitialResourcesAnnotationValue(%v, %q) = %q, want %q", tc.filled, tc.skipReason, got, tc.want)
		}
	}
}
