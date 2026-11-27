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
	"maps"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TrimWorkloadForCache is a cache transform for Deployments and StatefulSets. The workload autoscaling controllers
// cache every Deployment and StatefulSet in the cluster, but only read their object metadata and the labels of
// their Pod template, so the transform drops everything else to limit the memory the cache uses: the rest of the
// spec, the status, the managed fields, and the kubectl last-applied-configuration annotation.
//
// Cached workloads are therefore incomplete: the manager's client must never be used to update a Deployment or
// StatefulSet read from the cache. Other objects, including tombstones of deleted objects, are returned as is.
func TrimWorkloadForCache(obj any) (any, error) {
	switch w := obj.(type) {
	case *appsv1.Deployment:
		return &appsv1.Deployment{
			TypeMeta:   w.TypeMeta,
			ObjectMeta: trimObjectMeta(w.ObjectMeta),
			Spec:       appsv1.DeploymentSpec{Template: trimPodTemplate(w.Spec.Template)},
		}, nil
	case *appsv1.StatefulSet:
		return &appsv1.StatefulSet{
			TypeMeta:   w.TypeMeta,
			ObjectMeta: trimObjectMeta(w.ObjectMeta),
			Spec:       appsv1.StatefulSetSpec{Template: trimPodTemplate(w.Spec.Template)},
		}, nil
	default:
		return obj, nil
	}
}

func trimObjectMeta(m metav1.ObjectMeta) metav1.ObjectMeta {
	m.ManagedFields = nil
	if _, ok := m.Annotations[corev1.LastAppliedConfigAnnotation]; ok {
		m.Annotations = maps.Clone(m.Annotations)
		delete(m.Annotations, corev1.LastAppliedConfigAnnotation)
	}
	return m
}

func trimPodTemplate(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: t.Labels}}
}
