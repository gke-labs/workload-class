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
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// FindBestMatchWorkloadClass finds the most suitable WorkloadClass for a given Pod.
//
// Selection preference is given to the namespace's enforced WorkloadClass.
// If no enforced WorkloadClass is defined, the WorkloadClass in the Pod's namespace that has the
// highest number of matching labels and expressions against the Pod is selected.
// If two or more WorkloadClasses match in specificity to the Pod, the oldest
// WorkloadClass takes precedence.
//
// If no specific or enforced WorkloadClass is found, it returns nil.
func FindBestMatchWorkloadClass(ctx context.Context, c client.Client, recorder events.EventRecorder, pod *corev1.Pod) (bestMatch *workloadsv1.WorkloadClass, err error) {
	if pod == nil {
		return nil, nil
	}

	// Use the namespace's enforced WorkloadClass if it exists
	bestMatch, err = NamespaceEnforcedWorkloadClass(ctx, c, pod.Namespace)
	if err != nil {
		return nil, err
	}
	if bestMatch != nil {
		return bestMatch, nil
	}

	// WorkloadClass is namespaced and only applies to Pods in its own namespace, so only list that namespace
	// rather than every WorkloadClass in the cluster on each Pod admission or eviction.
	wcs := &workloadsv1.WorkloadClassList{}
	if err := c.List(ctx, wcs, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %v", err)
	}

	bestMatch, otherMatches, maxSpecificity := selectBestMatchFromList(pod, wcs.Items)

	// Emit warning message for WorkloadClasses that matched, but are ignored
	emitAmbiguousMatchWarning(ctx, recorder, pod, bestMatch, otherMatches, maxSpecificity)

	return bestMatch, nil
}

// BestMatchWorkloadClassForPod evaluates the best-matching WorkloadClass for a Pod in memory
// using a pre-fetched namespace's enforced WorkloadClass (if any) and list of WorkloadClasses.
//
// If enforcedWC is non-nil, in the Pod's namespace, and not being deleted, it takes immediate precedence.
// Otherwise, the active WorkloadClass in the Pod's namespace with the highest selector specificity
// (breaking ties by oldest CreationTimestamp) is returned, or nil if none match.
func BestMatchWorkloadClassForPod(pod *corev1.Pod, enforcedWC *workloadsv1.WorkloadClass, allWCs []workloadsv1.WorkloadClass) *workloadsv1.WorkloadClass {
	if pod == nil {
		return nil
	}
	if enforcedWC != nil && enforcedWC.Namespace == pod.Namespace && enforcedWC.DeletionTimestamp.IsZero() {
		return enforcedWC
	}
	bestMatch, _, _ := selectBestMatchFromList(pod, allWCs)
	return bestMatch
}

func selectBestMatchFromList(pod *corev1.Pod, allWCs []workloadsv1.WorkloadClass) (*workloadsv1.WorkloadClass, map[string]int, int) {
	var bestMatch *workloadsv1.WorkloadClass
	otherMatches := map[string]int{}
	maxSpecificity := -1

	for i := range allWCs {
		wc := &allWCs[i]
		if wc.Namespace != pod.Namespace || !wc.DeletionTimestamp.IsZero() {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			bestMatch, otherMatches = updateBestMatch(wc, bestMatch, &maxSpecificity, otherMatches)
		}
	}

	return bestMatch, otherMatches, maxSpecificity
}

func emitAmbiguousMatchWarning(ctx context.Context, recorder events.EventRecorder, pod *corev1.Pod, bestMatch *workloadsv1.WorkloadClass, matches map[string]int, maxSpecificity int) {
	if len(matches) == 0 {
		return
	}

	log := logf.FromContext(ctx).WithValues("name", pod.Name, "namespace", pod.Namespace)
	log.Info(fmt.Sprintf("Multiple WorkloadClasses matched Pod %s/%s, but were not the best match: %v", pod.Namespace, pod.Name, matches))

	var matchesWithMaxSpecificity []string
	for m, s := range matches {
		if s == maxSpecificity {
			matchesWithMaxSpecificity = append(matchesWithMaxSpecificity, m)
		}
	}

	// Emit a warning specifically for those with max specificity that were not selected
	if len(matchesWithMaxSpecificity) != 0 && recorder != nil {
		warning := fmt.Sprintf("the following WorkloadClasses match pods with the same specificity as the best match, but were not selected: %s", strings.Join(matchesWithMaxSpecificity, ", "))
		// Emit a warning event
		recorder.Eventf(
			bestMatch,
			nil,
			corev1.EventTypeWarning,
			"AmbiguousMatch",
			"SelectWorkloadClass",
			"%s",
			warning,
		)
	}
}

func updateBestMatch(wc, bestMatch *workloadsv1.WorkloadClass, maxSpecificity *int, otherMatches map[string]int) (*workloadsv1.WorkloadClass, map[string]int) {
	spec := getSpecificity(wc.Spec.PodSelector)
	equalSpecOlderWC := spec == *maxSpecificity && wc.CreationTimestamp.Before(&bestMatch.CreationTimestamp)
	if spec > *maxSpecificity || equalSpecOlderWC {
		if bestMatch != nil {
			otherMatches[fmt.Sprintf("%s/%s", bestMatch.Namespace, bestMatch.Name)] = *maxSpecificity
		}
		*maxSpecificity = spec
		return wc, otherMatches
	}
	// This WC still matched the Pod, track it for logging
	otherMatches[fmt.Sprintf("%s/%s", wc.Namespace, wc.Name)] = spec
	return bestMatch, otherMatches
}

func getSpecificity(sel *metav1.LabelSelector) int {
	if sel == nil {
		return 0
	}
	return len(sel.MatchLabels) + len(sel.MatchExpressions)
}

// WorkloadClassPodSelector returns a selector for the Pods that a WorkloadClass applies to, for use when listing
// or counting a WorkloadClass's Pods.
//
// The namespace's enforced WorkloadClass applies to every Pod in its namespace, regardless of its PodSelector,
// because FindBestMatchWorkloadClass selects it for all of them. For any other WorkloadClass, the PodSelector is
// used, and a nil PodSelector selects all Pods. Callers that must exclude Pods owned by a more specific
// WorkloadClass should still filter the results through FindBestMatchWorkloadClass.
func WorkloadClassPodSelector(ctx context.Context, c client.Client, wc *workloadsv1.WorkloadClass) (labels.Selector, error) {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: wc.Namespace}, ns); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("error getting Namespace %s: %w", wc.Namespace, err)
		}
	} else if ns.Labels[workloadsv1.EnforcedClassLabel] == wc.Name {
		return labels.Everything(), nil
	}
	if wc.Spec.PodSelector == nil {
		return labels.Everything(), nil
	}
	selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid podSelector: %w", err)
	}
	return selector, nil
}

// NamespaceEnforcedWorkloadClass returns the active enforced WorkloadClass configured on the given Namespace
// via the "workloads.gke.io/enforced-class" label.
//
// It returns (nil, nil) if the Namespace does not exist, has no enforced-class label, if the referenced
// WorkloadClass does not exist, or if the referenced WorkloadClass is currently being deleted.
func NamespaceEnforcedWorkloadClass(ctx context.Context, c client.Client, namespace string) (*workloadsv1.WorkloadClass, error) {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("error getting Namespace %s: %w", namespace, err)
	}

	enforcedClass, ok := ns.Labels[workloadsv1.EnforcedClassLabel]
	if !ok || enforcedClass == "" {
		return nil, nil
	}

	wc := &workloadsv1.WorkloadClass{}
	if err := c.Get(ctx, client.ObjectKey{Name: enforcedClass, Namespace: namespace}, wc); err != nil {
		if apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Info("Referenced enforced WorkloadClass not found", "workloadClass", enforcedClass, "namespace", namespace)
			return nil, nil
		}
		return nil, err
	}

	if !wc.DeletionTimestamp.IsZero() {
		return nil, nil
	}

	return wc, nil
}

// IsValidated reports whether a WorkloadClass Validated condition allows the WorkloadClass to govern its Pods:
// the condition is True, or it is False only because the WorkloadClass PodSelector overlaps the PodSelector of
// another WorkloadClass in the namespace (ReasonSelectorOverlap).
//
// An overlapping WorkloadClass still complies with the guardrails. Each Pod is governed by its single best-match
// WorkloadClass (see BestMatchWorkloadClassForPod), so the overlap is resolved per Pod rather than by dropping the
// policy of every WorkloadClass involved.
func IsValidated(cond *metav1.Condition) bool {
	if cond == nil {
		return false
	}
	return cond.Status == metav1.ConditionTrue || cond.Reason == workloadsv1.ReasonSelectorOverlap
}
