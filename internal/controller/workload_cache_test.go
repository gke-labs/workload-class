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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	"github.com/gke-labs/workload-class/internal/utils"
)

func TestTrimWorkload(t *testing.T) {
	const workloadName = "web"
	now := metav1.Now()
	meta := metav1.ObjectMeta{
		Name:              workloadName,
		Namespace:         wasNamespace,
		UID:               "uid-web",
		ResourceVersion:   "42",
		Generation:        7,
		CreationTimestamp: now,
		DeletionTimestamp: &now,
		Labels:            map[string]string{"app": "web"},
		Annotations:       map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "{...}"},
		Finalizers:        []string{"example.com/finalizer"},
		OwnerReferences:   []metav1.OwnerReference{{APIVersion: "example.com/v1", Kind: "App", Name: "shop", UID: "uid-app"}},
		ManagedFields:     []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"tier": "web"}, Annotations: map[string]string{"sidecar": "true"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.com/web:1"}}},
	}
	wantMeta := metav1.ObjectMeta{
		Name:              workloadName,
		Namespace:         wasNamespace,
		UID:               "uid-web",
		ResourceVersion:   "42",
		Generation:        7,
		CreationTimestamp: now,
		DeletionTimestamp: &now,
		Labels:            map[string]string{"app": "web"},
		OwnerReferences:   meta.OwnerReferences,
	}
	wantTemplate := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"tier": "web"}}}

	t.Run("deployment", func(t *testing.T) {
		deploy := &appsv1.Deployment{
			TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
			ObjectMeta: meta,
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To[int32](3),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "web"}},
				Template: template,
			},
			Status: appsv1.DeploymentStatus{Replicas: 3, ReadyReplicas: 3},
		}
		got, err := TrimWorkload(deploy)
		if err != nil {
			t.Fatalf("TrimWorkload failed: %v", err)
		}
		want := &appsv1.Deployment{TypeMeta: deploy.TypeMeta, ObjectMeta: wantMeta, Spec: appsv1.DeploymentSpec{Template: wantTemplate}}
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("trimmed Deployment mismatch\n got: %+v\nwant: %+v", got, want)
		}
		// What the controllers read is intact.
		ref, ok := utils.WorkloadRefFor(got.(*appsv1.Deployment))
		if !ok || ref.Kind != utils.DeploymentKind || ref.Name != workloadName || ref.Namespace != wasNamespace || ref.TemplateLabels["tier"] != wasWebLabels["tier"] {
			t.Errorf("unexpected WorkloadRef of the trimmed Deployment: %+v", ref)
		}
	})

	t.Run("statefulset", func(t *testing.T) {
		sts := &appsv1.StatefulSet{
			ObjectMeta: meta,
			Spec: appsv1.StatefulSetSpec{
				ServiceName:          "web",
				Template:             template,
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			},
			Status: appsv1.StatefulSetStatus{Replicas: 3},
		}
		got, err := TrimWorkload(sts)
		if err != nil {
			t.Fatalf("TrimWorkload failed: %v", err)
		}
		want := &appsv1.StatefulSet{ObjectMeta: wantMeta, Spec: appsv1.StatefulSetSpec{Template: wantTemplate}}
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("trimmed StatefulSet mismatch\n got: %+v\nwant: %+v", got, want)
		}
	})

	t.Run("other_objects_are_returned_unchanged", func(t *testing.T) {
		tombstone := cache.DeletedFinalStateUnknown{Key: "shop/web", Obj: &appsv1.Deployment{ObjectMeta: meta}}
		got, err := TrimWorkload(tombstone)
		if err != nil {
			t.Fatalf("TrimWorkload failed: %v", err)
		}
		if _, ok := got.(cache.DeletedFinalStateUnknown); !ok {
			t.Errorf("expected the tombstone to be returned unchanged, got %T", got)
		}
	})
}

func TestWorkloadCacheByObject(t *testing.T) {
	byObject := WorkloadCacheByObject()
	if len(byObject) != 2 {
		t.Fatalf("expected a cache configuration for Deployments and StatefulSets, got %d entries", len(byObject))
	}
	for obj, config := range byObject {
		if config.Transform == nil {
			t.Errorf("%T: expected a transform", obj)
		}
		if _, ok := utils.WorkloadRefFor(obj); !ok {
			t.Errorf("%T is not a workload that resource management governs", obj)
		}
	}
}
