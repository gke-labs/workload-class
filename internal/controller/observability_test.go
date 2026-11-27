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

package controller

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gke-labs/workload-class/internal/utils"
)

func TestManagedAutoscalerCollector(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{autoscalingv2.AddToScheme, vpav1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("failed to build scheme: %v", err)
		}
	}
	managed := utils.ManagedAutoscalerLabels("web")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "wc-deployment-a", Namespace: "a", Labels: managed}},
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "wc-deployment-b", Namespace: "b", Labels: managed}},
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "user", Namespace: "a"}},
		&vpav1.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "wc-deployment-a", Namespace: "a", Labels: managed}},
		&vpav1.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "user", Namespace: "a"}},
	).Build()

	testCases := []struct {
		name         string
		vpaInstalled bool
		want         string
	}{
		{
			name:         "with_vpa",
			vpaInstalled: true,
			want: `
# HELP workloadclass_managed_autoscalers Number of HorizontalPodAutoscalers and VerticalPodAutoscalers that WorkloadClasses manage, by kind.
# TYPE workloadclass_managed_autoscalers gauge
workloadclass_managed_autoscalers{kind="HorizontalPodAutoscaler"} 2
workloadclass_managed_autoscalers{kind="VerticalPodAutoscaler"} 1
`,
		},
		{
			name: "without_vpa",
			want: `
# HELP workloadclass_managed_autoscalers Number of HorizontalPodAutoscalers and VerticalPodAutoscalers that WorkloadClasses manage, by kind.
# TYPE workloadclass_managed_autoscalers gauge
workloadclass_managed_autoscalers{kind="HorizontalPodAutoscaler"} 2
`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collector := &ManagedAutoscalerCollector{Reader: c, VPAInstalled: tc.vpaInstalled}
			if err := testutil.CollectAndCompare(collector, strings.NewReader(tc.want)); err != nil {
				t.Errorf("unexpected metrics: %v", err)
			}
		})
	}
}

func TestTrimWorkloadForCache(t *testing.T) {
	meta := metav1.ObjectMeta{
		Name:       "web",
		Namespace:  "shop",
		UID:        "uid-web",
		Generation: 3,
		Labels:     map[string]string{"team": "a"},
		Annotations: map[string]string{
			corev1.LastAppliedConfigAnnotation: `{"a":"very large manifest"}`,
			"other":                            "kept",
		},
		ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
		OwnerReferences: []metav1.OwnerReference{{Kind: "Rollout", Name: "web"}},
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}, Annotations: map[string]string{"a": "b"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app"}}},
	}

	check := func(t *testing.T, got client.Object, gotTemplate corev1.PodTemplateSpec) {
		t.Helper()
		if got.GetName() != meta.Name || got.GetNamespace() != meta.Namespace || got.GetUID() != meta.UID || got.GetGeneration() != meta.Generation {
			t.Errorf("identity was not kept: %s/%s %s generation %d", got.GetNamespace(), got.GetName(), got.GetUID(), got.GetGeneration())
		}
		if got.GetLabels()["team"] != "a" || len(got.GetOwnerReferences()) != 1 {
			t.Errorf("labels or owner references were not kept: %v %v", got.GetLabels(), got.GetOwnerReferences())
		}
		if _, ok := got.GetAnnotations()[corev1.LastAppliedConfigAnnotation]; ok || got.GetAnnotations()["other"] != "kept" {
			t.Errorf("annotations = %v, want only the other annotation", got.GetAnnotations())
		}
		if got.GetManagedFields() != nil {
			t.Errorf("managed fields were kept")
		}
		want := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}}}
		if gotTemplate.Labels["app"] != "web" || len(gotTemplate.Spec.Containers) != 0 || gotTemplate.Annotations != nil {
			t.Errorf("template = %+v, want %+v", gotTemplate, want)
		}
	}

	t.Run("deployment", func(t *testing.T) {
		in := &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3), Template: template}}
		in.Status.Replicas = 3
		out, err := TrimWorkloadForCache(in)
		if err != nil {
			t.Fatalf("TrimWorkloadForCache() error = %v", err)
		}
		got := out.(*appsv1.Deployment)
		check(t, got, got.Spec.Template)
		if got.Spec.Replicas != nil || got.Status.Replicas != 0 {
			t.Errorf("spec or status were kept: %+v %+v", got.Spec, got.Status)
		}
		if _, ok := in.Annotations[corev1.LastAppliedConfigAnnotation]; !ok {
			t.Errorf("the input object was modified")
		}
		again, _ := TrimWorkloadForCache(got)
		if again.(*appsv1.Deployment).Spec.Template.Labels["app"] != "web" {
			t.Errorf("TrimWorkloadForCache() is not idempotent")
		}
	})

	t.Run("statefulset", func(t *testing.T) {
		in := &appsv1.StatefulSet{ObjectMeta: meta, Spec: appsv1.StatefulSetSpec{ServiceName: "web", Template: template}}
		out, err := TrimWorkloadForCache(in)
		if err != nil {
			t.Fatalf("TrimWorkloadForCache() error = %v", err)
		}
		got := out.(*appsv1.StatefulSet)
		check(t, got, got.Spec.Template)
		if got.Spec.ServiceName != "" {
			t.Errorf("spec was kept: %+v", got.Spec)
		}
	})

	t.Run("other_objects", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: meta}
		out, err := TrimWorkloadForCache(pod)
		if err != nil || out != pod {
			t.Errorf("TrimWorkloadForCache(Pod) = (%v, %v), want the Pod unchanged", out, err)
		}
		tombstone := "not an object"
		if out, _ := TrimWorkloadForCache(tombstone); out != tombstone {
			t.Errorf("TrimWorkloadForCache() changed a non-object")
		}
	})
}
