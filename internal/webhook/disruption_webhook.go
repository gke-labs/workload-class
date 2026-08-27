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

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client   client.Client
	decoder  *admission.Decoder
	Recorder events.EventRecorder
}

// +kubebuilder:webhook:path=/validate-disruption,mutating=false,failurePolicy=fail,sideEffects=None,groups="",resources=pods;pods/eviction,verbs=create;delete,versions=v1,name=vpoddisruption.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod evictions.
func (v *DisruptionWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	// 1.1 Verify this is an eviction, not a standard pod create/delete
	if req.SubResource != "eviction" {
		return admission.Allowed("Not an eviction")
	}

	// 1. Identify the Pod
	pod := &corev1.Pod{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: req.Name, Namespace: req.Namespace}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 2. Find matching WorkloadClasses
	bestWC, err := utils.BestMatchWorkloadClass(ctx, v.Client, v.Recorder, pod)
	if err != nil {
		log.Error(err, "failed to get WorkloadClass for Pod", "pod", pod)
		return admission.Allowed("Failed to get WorkloadClass matches this pod or namespace")
	}

	if bestWC == nil {
		// No WorkloadClass matches this pod or namespace, allow eviction by default.
		return admission.Allowed("No WorkloadClass matches this pod or namespace")
	}

	log = log.WithValues("workloadClass", bestWC.Name)

	// 3. Check Guardrail validation (if it fails, we don't enforce constraints)
	for _, cond := range bestWC.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			log.Info("WorkloadClass failed Guardrail validation, ignoring constraints")
			return admission.Allowed("WorkloadClass failed Guardrail validation")
		}
	}

	// 4. Emergency Override
	if bestWC.Spec.DisruptionPolicy.EmergencyOverride {
		return admission.Allowed("Emergency override active")
	}

	// 4. Identity-Based Filtering
	for _, allowedSubject := range bestWC.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		allowed, err := matchesIdentity(req.UserInfo, allowedSubject)
		if err != nil {
			log.Error(err, "Failed to check if UserInfo matches allowed Subject", "subject", allowedSubject)
		}
		if allowed {
			return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", allowedSubject))
		}
	}

	// 5. Temporal Enforcement
	now := time.Now().UTC()
	inWindow, _ := utils.IsTimeInWindows(ctx, now, bestWC.Spec.DisruptionPolicy.AllowedDisruptionWindows)

	// 6. Maintenance Starvation (Override on Overdue)
	if bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue {
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	}

	if !inWindow {
		return admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", bestWC.Name))
	}

	// 7. Pod Lifecycle Protection (Min Initial Run)
	if bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
			return admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				now.Sub(pod.CreationTimestamp.Time).Round(time.Minute), bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays))
		}
	}

	return admission.Allowed("Eviction allowed by WorkloadClass policy")
}

func matchesIdentity(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Name == "" {
		return false, fmt.Errorf("subject name cannot be empty")
	}

	switch subject.Kind {
	case rbacv1.UserKind:
		return matchesUserKind(userInfo, subject)
	case rbacv1.GroupKind:
		return matchesGroupKind(userInfo, subject)
	case rbacv1.ServiceAccountKind:
		return matchesServiceAccountKind(userInfo, subject)
	default:
		return false, fmt.Errorf("subject has invalid Kind: %s", subject.Kind)
	}
}

func matchesUserKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return userInfo.Username == subject.Name, nil
}

func matchesGroupKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return slices.Contains(userInfo.Groups, subject.Name), nil
}

func matchesServiceAccountKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	expectedUsername := fmt.Sprintf("system:serviceaccount:%s:%s", subject.Namespace, subject.Name)
	return userInfo.Username == expectedUsername, nil
}

// InjectDecoder injects the decoder.
func (v *DisruptionWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
