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

package v1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func rmTestGuardrail(c *workloadsv1.ResourceManagementConstraints) *workloadsv1.WorkloadClassGuardrail {
	return &workloadsv1.WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: "resource-management"},
		Spec: workloadsv1.WorkloadClassGuardrailSpec{Constraints: workloadsv1.Constraints{
			Disruption:         workloadsv1.Disruption{MaxNonDisruptionDurationDays: 30},
			ResourceManagement: c,
		}},
	}
}

// rmTestScheme builds a scheme without Gomega assertions, so that it can be used from plain Go tests that do
// not run inside the Ginkgo suite.
func rmTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, workloadsv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("failed to build scheme: %v", err)
		}
	}
	return scheme
}

func rmTestResources(cpu, memory string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)}
}

func TestValidateGuardrailSpecRejectsInvalidResourceManagementConstraints(t *testing.T) {
	g := rmTestGuardrail(&workloadsv1.ResourceManagementConstraints{
		ContainerBounds: &workloadsv1.ContainerBounds{Min: rmTestResources("10m", "2Gi"), Max: rmTestResources("16", "1Gi")},
	})
	_, err := validateGuardrailSpec(g)
	if err == nil {
		t.Fatal("expected the guardrail to be rejected")
	}
	if want := "invalid resourceManagement constraints: `constraints.resourceManagement.containerBounds.min` memory must not exceed"; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err.Error(), want)
	}

	g.Spec.Constraints.ResourceManagement.ContainerBounds.Min = rmTestResources("10m", "32Mi")
	if _, err := validateGuardrailSpec(g); err != nil {
		t.Errorf("expected the guardrail to be accepted, got %v", err)
	}
}

// Existing WorkloadClasses never block a guardrail change: violations are returned as admission warnings.
func TestGuardrailWebhookWarnsAboutResourceManagementViolations(t *testing.T) {
	compliantDisruption := workloadsv1.DisruptionPolicy{MaxNonDisruptionDurationDays: 7}
	withInitialResources := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "with-initial-resources", Namespace: testNamespace},
		Spec: workloadsv1.WorkloadClassSpec{
			DisruptionPolicy: compliantDisruption,
			ResourceManagement: &workloadsv1.ResourceManagement{
				InitialResources: &workloadsv1.InitialResources{Requests: rmTestResources("250m", "256Mi")},
			},
		},
	}
	withoutResourceManagement := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "without-resource-management", Namespace: testNamespace},
		Spec:       workloadsv1.WorkloadClassSpec{DisruptionPolicy: compliantDisruption},
	}
	v := &WorkloadClassGuardrailCustomValidator{
		Client: fake.NewClientBuilder().WithScheme(rmTestScheme(t)).WithObjects(withInitialResources, withoutResourceManagement).Build(),
	}

	g := rmTestGuardrail(&workloadsv1.ResourceManagementConstraints{RequireInitialResources: true})
	warnings, err := v.ValidateCreate(context.Background(), g)
	if err != nil {
		t.Fatalf("expected the guardrail to be admitted, got %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	for _, want := range []string{
		"existing WorkloadClass '" + testNamespace + "/without-resource-management'",
		"`resourceManagement.initialResources` must be specified (guardrail limit)",
	} {
		if !strings.Contains(warnings[0], want) {
			t.Errorf("warning = %q, want it to contain %q", warnings[0], want)
		}
	}

	// A guardrail without resource management constraints warns about neither WorkloadClass.
	warnings, err = v.ValidateUpdate(context.Background(), g, rmTestGuardrail(nil))
	if err != nil || len(warnings) != 0 {
		t.Errorf("expected no warnings and no error, got %v, %v", warnings, err)
	}
}
