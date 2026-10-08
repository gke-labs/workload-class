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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

var _ = Describe("WorkloadClassGuardrail Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name: resourceName, // Cluster-scoped, no namespace
		}
		workloadclassguardrail := &workloadsv1.WorkloadClassGuardrail{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind WorkloadClassGuardrail")
			err := k8sClient.Get(ctx, typeNamespacedName, workloadclassguardrail)
			if err != nil && errors.IsNotFound(err) {
				resource := &workloadsv1.WorkloadClassGuardrail{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: workloadsv1.WorkloadClassGuardrailSpec{
						Constraints: workloadsv1.Constraints{
							Disruption: workloadsv1.Disruption{
								MaxNonDisruptionDurationDays: 1,
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &workloadsv1.WorkloadClassGuardrail{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance WorkloadClassGuardrail")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Updating the resource with valid days")
			g := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, g)).To(Succeed())

			validDays := []string{"Sunday", "Monday"}
			g.Spec.Constraints.Disruption.AllowedDisruptionDays = validDays
			Expect(k8sClient.Update(ctx, g)).To(Succeed())

			By("Reconciling the created resource")
			controllerReconciler := &WorkloadClassGuardrailReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the status for validated")
			updatedGuardrail := &workloadsv1.WorkloadClassGuardrail{}
			Eventually(func() bool {
				Expect(k8sClient.Get(ctx, typeNamespacedName, updatedGuardrail)).To(Succeed())
				for _, cond := range updatedGuardrail.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionTrue &&
							cond.Reason == workloadsv1.ReasonValidationPassed
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})

		It("should fail validation if it contains an invalid day in AllowedDisruptionDays", func() {
			By("updating the resource with an invalid day")
			g := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, g)).To(Succeed())

			invalidDays := []string{"Christmas", "Eid", "Birthday"}
			g.Spec.Constraints.Disruption.AllowedDisruptionDays = invalidDays
			Expect(k8sClient.Update(ctx, g)).To(Succeed())

			By("reconiling")
			controllerReconciler := &WorkloadClassGuardrailReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the status for violations")
			updatedGuardrail := &workloadsv1.WorkloadClassGuardrail{}
			Eventually(func() bool {
				Expect(k8sClient.Get(ctx, typeNamespacedName, updatedGuardrail)).To(Succeed())
				for _, cond := range updatedGuardrail.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, "allowedDisruptionDays contains invalid days, valid days are")
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})

		It("should pass validation when pluginConstraints has valid gke-spot-placement parameters", func() {
			By("updating the resource with valid pluginConstraints")
			g := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, g)).To(Succeed())

			g.Spec.PluginConstraints = &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name:                  workloadsv1.PluginNameGKESpotPlacement,
						AllowedConfigTemplate: "K8sAllowedSpotRatio",
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Allowed","minSpotRatio":"80%","fallback":{"allowFallbackToOnDemand":true,"maxFallbackRatio":"20%"},"reversion":{"requiredReversionAction":"Active","maxFallbackDuration":"2h"}}`),
						},
					},
				},
			}
			Expect(k8sClient.Update(ctx, g)).To(Succeed())

			By("reconciling")
			controllerReconciler := &WorkloadClassGuardrailReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the status for validated")
			updatedGuardrail := &workloadsv1.WorkloadClassGuardrail{}
			Eventually(func() bool {
				Expect(k8sClient.Get(ctx, typeNamespacedName, updatedGuardrail)).To(Succeed())
				for _, cond := range updatedGuardrail.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionTrue &&
							cond.Reason == workloadsv1.ReasonValidationPassed
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})

		It("should fail validation when pluginConstraints has invalid gke-spot-placement parameters", func() {
			By("updating the resource with invalid pluginConstraints")
			g := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, g)).To(Succeed())

			g.Spec.PluginConstraints = &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"InvalidMode","minSpotRatio":"150%"}`),
						},
					},
				},
			}
			Expect(k8sClient.Update(ctx, g)).To(Succeed())

			By("reconciling")
			controllerReconciler := &WorkloadClassGuardrailReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the status for violations")
			updatedGuardrail := &workloadsv1.WorkloadClassGuardrail{}
			Eventually(func() bool {
				Expect(k8sClient.Get(ctx, typeNamespacedName, updatedGuardrail)).To(Succeed())
				for _, cond := range updatedGuardrail.Status.Conditions {
					if cond.Type == workloadsv1.ConditionTypeValidated {
						return cond.Status == metav1.ConditionFalse &&
							cond.Reason == workloadsv1.ReasonValidationFailed &&
							strings.Contains(cond.Message, `invalid enforcementMode "InvalidMode"`) &&
							strings.Contains(cond.Message, `invalid minSpotRatio "150%"`)
					}
				}
				return false
			}, "10s", "1s").Should(BeTrue())
		})
	})
})

func TestValidatePluginConstraints(t *testing.T) {
	testCases := []struct {
		name               string
		pc                 *workloadsv1.PluginConstraints
		wantViolationCount int
		wantSubstring      string
	}{
		{
			name:               "nil_plugin_constraints_passes",
			pc:                 nil,
			wantViolationCount: 0,
		},
		{
			name: "valid_gke_spot_placement_parameters_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Required","minSpotRatio":"80%","fallback":{"allowFallbackToOnDemand":true,"maxFallbackRatio":5},"reversion":{"requiredReversionAction":"Lazy","maxFallbackDuration":"30m"}}`),
						},
					},
				},
			},
			wantViolationCount: 0,
		},
		{
			name: "valid_non_gke_plugin_arbitrary_json_object_passes",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: "karpenter-spot-placement",
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"maxSpotPrice":"0.05","instanceFamilies":["m6i","c6i"]}`),
						},
					},
				},
			},
			wantViolationCount: 0,
		},
		{
			name: "empty_plugin_name_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: "   ",
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "name must not be empty",
		},
		{
			name: "unknown_field_in_gke_spot_parameters_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"unknownField":"value"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "invalid gke-spot-placement parameters",
		},
		{
			name: "invalid_enforcement_mode_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"enforcementMode":"Optional"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid enforcementMode "Optional"`,
		},
		{
			name: "invalid_min_spot_ratio_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"minSpotRatio":"80"}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid minSpotRatio "80"`,
		},
		{
			name: "invalid_max_fallback_ratio_percentage_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"fallback":{"maxFallbackRatio":"120%"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid fallback.maxFallbackRatio "120%"`,
		},
		{
			name: "negative_max_fallback_ratio_int_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"fallback":{"maxFallbackRatio":-1}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      "invalid fallback.maxFallbackRatio -1",
		},
		{
			name: "invalid_required_reversion_action_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"reversion":{"requiredReversionAction":"Immediate"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid reversion.requiredReversionAction "Immediate"`,
		},
		{
			name: "negative_max_fallback_duration_fails",
			pc: &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{
					{
						Name: workloadsv1.PluginNameGKESpotPlacement,
						Parameters: &apiextensionsv1.JSON{
							Raw: []byte(`{"reversion":{"maxFallbackDuration":"-10m"}}`),
						},
					},
				},
			},
			wantViolationCount: 1,
			wantSubstring:      `invalid reversion.maxFallbackDuration "-10m0s"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			violations := validatePluginConstraints(tc.pc)
			if len(violations) != tc.wantViolationCount {
				t.Fatalf("got %d violations (%v), want %d", len(violations), violations, tc.wantViolationCount)
			}
			if tc.wantSubstring != "" && !strings.Contains(strings.Join(violations, "; "), tc.wantSubstring) {
				t.Errorf("got violations %v, want substring %q", violations, tc.wantSubstring)
			}
		})
	}
}
