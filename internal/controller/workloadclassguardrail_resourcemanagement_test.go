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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestGuardrailValidateResourceManagementConstraints(t *testing.T) {
	r := &WorkloadClassGuardrailReconciler{}

	valid := rmGuardrails(&workloadsv1.ResourceManagementConstraints{
		RequireInitialResources: true,
		ContainerBounds:         &workloadsv1.ContainerBounds{Min: rmResources("10m", "32Mi"), Max: rmResources("16", "64Gi")},
	}).Items[0]
	if cond := r.validate(context.Background(), &valid); cond.Status != metav1.ConditionTrue {
		t.Errorf("expected a valid guardrail, got %s: %s", cond.Status, cond.Message)
	}

	invalid := rmGuardrails(&workloadsv1.ResourceManagementConstraints{
		ContainerBounds: &workloadsv1.ContainerBounds{Min: rmResources("2", "32Mi"), Max: rmResources("1", "64Gi")},
	}).Items[0]
	cond := r.validate(context.Background(), &invalid)
	if cond.Status != metav1.ConditionFalse || cond.Reason != workloadsv1.ReasonValidationFailed {
		t.Fatalf("expected Validated=False/ValidationFailed, got %s/%s", cond.Status, cond.Reason)
	}
	if want := "`constraints.resourceManagement.containerBounds.min` cpu must not exceed"; !strings.Contains(cond.Message, want) {
		t.Errorf("message = %q, want it to contain %q", cond.Message, want)
	}
}
