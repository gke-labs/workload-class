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
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// SafeToEvictAnnotation is the Cluster Autoscaler annotation that blocks pod eviction when set to "false".
	SafeToEvictAnnotation = "cluster-autoscaler.kubernetes.io/safe-to-evict"

	// SafeToEvictSuspendedAnnotation records that WorkloadClass temporarily removed safe-to-evict="false"
	// while a disruption window is open so it can be restored when the window closes.
	SafeToEvictSuspendedAnnotation = "workloads.gke.io/safe-to-evict-suspended"

	annotationValueFalse = "false"
	annotationValueTrue  = "true"
)

// reconcileSafeToEvictAnnotations toggles the Cluster Autoscaler safe-to-evict="false" annotation on
// active Pods governed by wc when wc.Spec.DisruptionPolicy.ManageSafeToEvictAnnotation is enabled.
//
// During open maintenance windows (Ready/Overdue) for a valid WorkloadClass, safe-to-evict="false" is
// removed and SafeToEvictSuspendedAnnotation="true" is recorded. Outside windows, when invalid, or when
// ManageSafeToEvictAnnotation is turned off, any previously suspended Pod has safe-to-evict="false" restored.
// Pods that never had safe-to-evict="false" (including "true" or unset) are left untouched.
func (r *WorkloadClassReconciler) reconcileSafeToEvictAnnotations(
	ctx context.Context,
	wc *workloadsv1.WorkloadClass,
	readiness workloadsv1.MaintenanceReadiness,
	isValid bool,
) error {
	if !wc.DeletionTimestamp.IsZero() {
		return nil
	}

	selector, err := utils.WorkloadClassPodSelector(ctx, r.Client, wc)
	if err != nil {
		return fmt.Errorf("failed to resolve PodSelector for safe-to-evict reconciliation: %w", err)
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return fmt.Errorf("failed to list Pods for safe-to-evict reconciliation: %w", err)
	}

	enforcedWC, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, wc.Namespace)
	if err != nil {
		return err
	}
	wcs := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, wcs, client.InNamespace(wc.Namespace)); err != nil {
		return fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	shouldSuspend := wc.Spec.DisruptionPolicy.ManageSafeToEvictAnnotation &&
		isValid &&
		(readiness == workloadsv1.ReadinessReady || readiness == workloadsv1.ReadinessOverdue)

	var errs []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !utils.IsPodActive(pod) {
			continue
		}
		if best := utils.BestMatchWorkloadClassForPod(pod, enforcedWC, wcs.Items); best == nil || best.Name != wc.Name {
			continue
		}
		if shouldSuspend {
			if pod.Annotations[SafeToEvictAnnotation] == annotationValueFalse {
				if err := r.patchSafeToEvict(ctx, client.ObjectKeyFromObject(pod), true); err != nil {
					errs = append(errs, err)
				}
			}
		} else {
			if pod.Annotations[SafeToEvictSuspendedAnnotation] == annotationValueTrue {
				if err := r.patchSafeToEvict(ctx, client.ObjectKeyFromObject(pod), false); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	return errors.Join(errs...)
}

func (r *WorkloadClassReconciler) patchSafeToEvict(ctx context.Context, key client.ObjectKey, suspend bool) error {
	log := logf.FromContext(ctx).WithValues("pod", key.Name, "namespace", key.Namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Pod{}
		if err := r.Get(ctx, key, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get Pod %s: %w", key.Name, err)
		}

		patch := client.MergeFrom(latest.DeepCopy())
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}

		if suspend {
			if latest.Annotations[SafeToEvictAnnotation] != annotationValueFalse {
				return nil
			}
			delete(latest.Annotations, SafeToEvictAnnotation)
			latest.Annotations[SafeToEvictSuspendedAnnotation] = annotationValueTrue
			if err := r.Patch(ctx, latest, patch); err != nil {
				return fmt.Errorf("failed to suspend safe-to-evict on Pod %s: %w", key.Name, err)
			}
			log.Info("Suspended safe-to-evict annotation during open disruption window")
			return nil
		}

		if latest.Annotations[SafeToEvictSuspendedAnnotation] != annotationValueTrue {
			return nil
		}
		latest.Annotations[SafeToEvictAnnotation] = annotationValueFalse
		delete(latest.Annotations, SafeToEvictSuspendedAnnotation)
		if err := r.Patch(ctx, latest, patch); err != nil {
			return fmt.Errorf("failed to restore safe-to-evict on Pod %s: %w", key.Name, err)
		}
		log.Info("Restored safe-to-evict annotation after disruption window closed")
		return nil
	})
}
