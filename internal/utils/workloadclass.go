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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// FindBestMatchWorkloadClass finds the most suitable WorkloadClass for a given Pod.
//
// Selection preference is given to the namespace's default WorkloadClass.
// If no default is defined, the WorkloadClass in the Pod's namespace that has the
// highest number of matching labels and expressions against the Pod is selected.
// If two or more WorkloadClasses match in specificity to the Pod, the oldest
// WorkloadClass takes precedence.
//
// If no specific or default WorkloadClass is found, it returns nil.
func FindBestMatchWorkloadClass(ctx context.Context, c client.Client, recorder events.EventRecorder, pod *corev1.Pod) (bestMatch *workloadsv1.WorkloadClass, err error) {
	// Use the namespace's default workload class if it exists
	if bestMatch = namespaceDefaultWorkloadClass(ctx, c, pod); bestMatch != nil {
		return bestMatch, nil
	}

	wcs := &workloadsv1.WorkloadClassList{}
	if err := c.List(ctx, wcs); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %v", err)
	}

	// Keep track of all other matches to emit a warning message
	otherMatches := map[string]int{}
	maxSpecificity := -1

	for _, wc := range wcs.Items {
		if wc.Namespace != "" && pod.Namespace != "" && wc.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			bestMatch, otherMatches = updateBestMatch(&wc, bestMatch, &maxSpecificity, otherMatches)
		}
	}

	// Emit warning message for WorkloadClasses that matched, but are ignored
	emitAmbiguousMatchWarning(ctx, recorder, pod, bestMatch, otherMatches, maxSpecificity)

	return bestMatch, nil
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

func namespaceDefaultWorkloadClass(ctx context.Context, c client.Client, pod *corev1.Pod) *workloadsv1.WorkloadClass {
	const defaultClassLabel = "workloads.gke.io/default-class"
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: pod.Namespace}, ns); err == nil && len(ns.GetLabels()) > 0 {
		if defaultClass, ok := ns.Labels[defaultClassLabel]; ok {
			wc := &workloadsv1.WorkloadClass{}
			if err := c.Get(ctx, client.ObjectKey{Name: defaultClass, Namespace: pod.Namespace}, wc); err == nil {
				return wc
			}
			if err := c.Get(ctx, client.ObjectKey{Name: defaultClass}, wc); err == nil {
				return wc
			}
		}
	}
	return nil
}
