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
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	// ManagedByLabel marks the autoscaler objects (HorizontalPodAutoscalers and VerticalPodAutoscalers) that
	// are created and managed for a workload from the resourceManagement of its WorkloadClass. Autoscalers
	// without this label are user-created and take precedence.
	ManagedByLabel = "workloads.gke.io/managed-by"
	// ManagedByValue is the value of ManagedByLabel on managed autoscalers.
	ManagedByValue = "workload-class"
	// WorkloadClassLabel records the WorkloadClass that a managed autoscaler was built from.
	WorkloadClassLabel = "workloads.gke.io/workload-class"

	// ManagedAutoscalerNamePrefix is the prefix of the names of managed autoscalers.
	ManagedAutoscalerNamePrefix = "wc-"
	// MaxManagedAutoscalerNameLength is the Kubernetes DNS-1123 subdomain limit for object names.
	MaxManagedAutoscalerNameLength = 253
	// maxLabelValueLength is the Kubernetes limit for label values.
	maxLabelValueLength = 63

	// DeploymentKind is the kind of apps/v1 Deployments.
	DeploymentKind = "Deployment"
	// StatefulSetKind is the kind of apps/v1 StatefulSets.
	StatefulSetKind = "StatefulSet"
	// WorkloadAPIVersion is the API version of the workloads that resource management governs.
	WorkloadAPIVersion = "apps/v1"
)

// WorkloadRef identifies a Deployment or StatefulSet that a WorkloadClass can govern, with the labels of its
// pod template.
type WorkloadRef struct {
	// Kind is DeploymentKind or StatefulSetKind.
	Kind      string
	Namespace string
	Name      string
	// TemplateLabels are the labels of the workload's pod template (spec.template.metadata.labels).
	TemplateLabels map[string]string
}

// WorkloadRefFor returns the WorkloadRef of a Deployment or StatefulSet. It returns false for any other object.
func WorkloadRefFor(obj client.Object) (WorkloadRef, bool) {
	switch w := obj.(type) {
	case *appsv1.Deployment:
		return WorkloadRef{Kind: DeploymentKind, Namespace: w.Namespace, Name: w.Name, TemplateLabels: w.Spec.Template.Labels}, true
	case *appsv1.StatefulSet:
		return WorkloadRef{Kind: StatefulSetKind, Namespace: w.Namespace, Name: w.Name, TemplateLabels: w.Spec.Template.Labels}, true
	default:
		return WorkloadRef{}, false
	}
}

// BestMatchWorkloadClassForTemplate returns the WorkloadClass that governs the workload: the best match for
// its pod template, evaluated exactly like the best match for a Pod (see BestMatchWorkloadClassForPod).
//
// Matching on the pod template rather than on running Pods gives one answer for the whole workload, also
// during a rollout and when the workload is scaled to zero. A Pod created from the template carries the
// same labels, so the Pod webhook resolves the same WorkloadClass for it.
func BestMatchWorkloadClassForTemplate(w WorkloadRef, enforcedWC *workloadsv1.WorkloadClass, allWCs []workloadsv1.WorkloadClass) *workloadsv1.WorkloadClass {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: w.Namespace, Labels: w.TemplateLabels}}
	return BestMatchWorkloadClassForPod(pod, enforcedWC, allWCs)
}

// ManagedAutoscalerName returns the name of the managed autoscaler of a workload:
// wc-<kind>-<workload name>, for example wc-deployment-web. The kind keeps the names of a Deployment and a
// StatefulSet with the same name apart. If the name exceeds the 253-character limit, it is truncated and a
// hash of the kind and workload name is appended, in the same way as PDBName.
func ManagedAutoscalerName(kind, workloadName string) string {
	name := ManagedAutoscalerNamePrefix + strings.ToLower(kind) + "-" + workloadName
	if len(name) <= MaxManagedAutoscalerNameLength {
		return name
	}
	return truncateWithHash(name, kind+"/"+workloadName, MaxManagedAutoscalerNameLength)
}

// ManagedAutoscalerLabels returns the labels that mark an autoscaler as managed, and record the
// WorkloadClass it was built from. A WorkloadClass name that is too long for a label value is truncated with
// a hash appended.
func ManagedAutoscalerLabels(wc *workloadsv1.WorkloadClass) map[string]string {
	className := wc.Name
	if len(className) > maxLabelValueLength {
		className = truncateWithHash(className, className, maxLabelValueLength)
	}
	return map[string]string{
		ManagedByLabel:     ManagedByValue,
		WorkloadClassLabel: className,
	}
}

// truncateWithHash shortens value to maxLen characters, ending in a hash of key so that distinct keys keep
// distinct results.
func truncateWithHash(value, key string, maxLen int) string {
	sum := sha256.Sum256([]byte(key))
	suffix := fmt.Sprintf("-%x", sum[:4])
	prefix := strings.TrimRight(value[:maxLen-len(suffix)], "-.")
	return prefix + suffix
}

// IsWorkloadClassManagedAutoscaler reports whether the object carries the label that marks managed
// autoscalers. Autoscalers without it are user-created.
func IsWorkloadClassManagedAutoscaler(obj client.Object) bool {
	return obj != nil && obj.GetLabels()[ManagedByLabel] == ManagedByValue
}
