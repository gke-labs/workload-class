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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadCacheByObject returns the cache configuration for the workloads that the workload autoscaling
// controllers watch. It trims every cached Deployment and StatefulSet down to the fields that the
// controllers read (see TrimWorkload).
//
// The controllers watch every Deployment and StatefulSet in the cluster. Without trimming, the manager would
// hold a full copy of each of them in memory, including their pod templates, status and managed fields.
//
// Objects read from this cache are incomplete, so they must never be written back to the API server. The
// controllers only read workloads, and use them as owners of the autoscalers they manage.
func WorkloadCacheByObject() map[client.Object]cache.ByObject {
	return map[client.Object]cache.ByObject{
		&appsv1.Deployment{}:  {Transform: TrimWorkload},
		&appsv1.StatefulSet{}: {Transform: TrimWorkload},
	}
}

// TrimWorkload is a cache transform that keeps only the parts of a Deployment or StatefulSet that the workload
// autoscaling controllers read:
//   - the metadata that identifies the object and its lifecycle: name, namespace, UID, resource version,
//     generation, creation and deletion timestamps, labels and owner references;
//   - the labels of the pod template, which decide the WorkloadClass that governs the workload.
//
// Everything else is dropped: annotations, managed fields, finalizers, the rest of the spec, and the status.
// Objects of other types (such as the tombstones of deleted objects) are returned unchanged.
func TrimWorkload(obj any) (any, error) {
	switch w := obj.(type) {
	case *appsv1.Deployment:
		return &appsv1.Deployment{
			TypeMeta:   w.TypeMeta,
			ObjectMeta: trimObjectMeta(&w.ObjectMeta),
			Spec:       appsv1.DeploymentSpec{Template: trimPodTemplate(&w.Spec.Template)},
		}, nil
	case *appsv1.StatefulSet:
		return &appsv1.StatefulSet{
			TypeMeta:   w.TypeMeta,
			ObjectMeta: trimObjectMeta(&w.ObjectMeta),
			Spec:       appsv1.StatefulSetSpec{Template: trimPodTemplate(&w.Spec.Template)},
		}, nil
	default:
		return obj, nil
	}
}

func trimObjectMeta(m *metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:              m.Name,
		Namespace:         m.Namespace,
		UID:               m.UID,
		ResourceVersion:   m.ResourceVersion,
		Generation:        m.Generation,
		CreationTimestamp: m.CreationTimestamp,
		DeletionTimestamp: m.DeletionTimestamp,
		Labels:            m.Labels,
		OwnerReferences:   m.OwnerReferences,
	}
}

func trimPodTemplate(t *corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: t.Labels}}
}
