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
	"crypto/sha256"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// WorkloadKindDeployment and WorkloadKindStatefulSet are the kinds of workload that resourceManagement applies
	// to. Both are in the apps/v1 API group version.
	WorkloadKindDeployment  = "Deployment"
	WorkloadKindStatefulSet = "StatefulSet"

	// WorkloadAPIVersion is the API version that managed autoscalers use to reference their workload.
	WorkloadAPIVersion = "apps/v1"

	// ManagedAutoscalerNamePrefix prefixes the names of the autoscalers that the controller manages.
	ManagedAutoscalerNamePrefix = "wc-"

	// MaxManagedAutoscalerNameLength is the Kubernetes DNS-1123 subdomain limit for object names.
	MaxManagedAutoscalerNameLength = 253

	// maxLabelValueLength is the Kubernetes limit for label values.
	maxLabelValueLength = 63
)

// WorkloadRef identifies a Deployment or StatefulSet and the labels of its Pod template.
type WorkloadRef struct {
	// Kind is WorkloadKindDeployment or WorkloadKindStatefulSet.
	Kind      string
	Name      string
	Namespace string
	UID       types.UID

	// TemplateLabels are the labels of the workload's Pod template, which decide its best-match WorkloadClass.
	TemplateLabels map[string]string
}

// WorkloadRefFor returns the WorkloadRef of a Deployment or StatefulSet, and false for any other object.
func WorkloadRefFor(obj client.Object) (WorkloadRef, bool) {
	ref := WorkloadRef{Name: obj.GetName(), Namespace: obj.GetNamespace(), UID: obj.GetUID()}
	switch w := obj.(type) {
	case *appsv1.Deployment:
		ref.Kind, ref.TemplateLabels = WorkloadKindDeployment, w.Spec.Template.Labels
	case *appsv1.StatefulSet:
		ref.Kind, ref.TemplateLabels = WorkloadKindStatefulSet, w.Spec.Template.Labels
	default:
		return WorkloadRef{}, false
	}
	return ref, true
}

// BestMatchWorkloadClassForTemplate returns the WorkloadClass that governs a workload: the best match for a Pod
// built from the workload's namespace and Pod template labels, as BestMatchWorkloadClassForPod selects it.
//
// Matching on the template rather than on the running Pods gives one answer for the whole workload, even during a
// rollout or when it is scaled to zero, and applies the same enforced-class, specificity, and age precedence as
// for Pods.
func BestMatchWorkloadClassForTemplate(workload WorkloadRef, enforcedWC *workloadsv1.WorkloadClass, wcs []workloadsv1.WorkloadClass) *workloadsv1.WorkloadClass {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: workload.Namespace, Labels: workload.TemplateLabels}}
	return BestMatchWorkloadClassForPod(pod, enforcedWC, wcs)
}

// ManagedAutoscalerName returns the name of the HorizontalPodAutoscaler and the VerticalPodAutoscaler that the
// controller manages for a workload: wc-<kind>-<name>, with the kind in lower case (e.g. wc-deployment-web).
// If the name exceeds the 253-character limit, it is truncated and suffixed with a deterministic 8-character
// SHA-256 hash of the full name, in the same way as PDBName.
func ManagedAutoscalerName(kind, name string) string {
	return truncateWithHash(ManagedAutoscalerNamePrefix+strings.ToLower(kind)+"-"+name, MaxManagedAutoscalerNameLength)
}

// ManagedAutoscalerLabels returns the labels of an autoscaler that the controller manages for the WorkloadClass.
func ManagedAutoscalerLabels(wcName string) map[string]string {
	return map[string]string{
		workloadsv1.ManagedByLabel:     workloadsv1.ManagedByLabelValue,
		workloadsv1.WorkloadClassLabel: WorkloadClassLabelValue(wcName),
	}
}

// WorkloadClassLabelValue returns the value of the workloads.gke.io/workload-class label for a WorkloadClass. Names
// longer than the 63-character label value limit are truncated and suffixed with a hash of the full name.
func WorkloadClassLabelValue(wcName string) string {
	return truncateWithHash(wcName, maxLabelValueLength)
}

// IsWorkloadClassManagedAutoscaler reports whether a HorizontalPodAutoscaler or VerticalPodAutoscaler is managed
// by the controller, i.e. has the workloads.gke.io/managed-by: workload-class label. Autoscalers without it are
// user-created, and take precedence over the WorkloadClass.
func IsWorkloadClassManagedAutoscaler(obj metav1.Object) bool {
	return obj != nil && obj.GetLabels()[workloadsv1.ManagedByLabel] == workloadsv1.ManagedByLabelValue
}

// truncateWithHash returns s if it fits in maxLen characters. Otherwise it truncates s and appends "-" and the first
// 8 hex characters of the SHA-256 hash of s, so different long values keep distinct results. Trailing '-' and '.'
// are trimmed from the truncated part, so the result stays a valid name or label value.
func truncateWithHash(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	suffix := fmt.Sprintf("-%x", sum[:4])
	return strings.TrimRight(s[:maxLen-len(suffix)], "-.") + suffix
}
