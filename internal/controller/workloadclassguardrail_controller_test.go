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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
	})
})

var _ = Describe("WorkloadClassGuardrail Controller triggering Validation", func() {
	var (
		wc1Name, wc2Name, wc3Name, wcgName = "wc1", "wc2", "wc3", "wcg"
	)

	testCases := []struct {
		name            string
		desc            string
		newAllowedDays  []string
		wantInvalidated []string
	}{
		{
			name: "none_invalidated",
			desc: "Updating the guardrail does not cause workload classes to become invalid",
		},
		{
			name:            "some_invalidated",
			desc:            "Updating the guardrail causes some workload classes to become invalid",
			newAllowedDays:  []string{"Sunday"},
			wantInvalidated: []string{wc1Name, wc2Name},
		},
		{
			name:            "all_invalidated",
			desc:            "Updating the guardrail causes all workloadclasses to become invalid",
			newAllowedDays:  []string{"Tuesday"},
			wantInvalidated: []string{wc1Name, wc2Name, wc3Name},
		},
	}

	for _, tc := range testCases {
		It(tc.desc, func() {
			ctx := context.Background()

			wcgNamespacedName := types.NamespacedName{Name: wcgName}
			wcg := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: wcgName},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxNonDisruptionDurationDays: 20,
							MaxAllowedWindows:            5,
							AllowedDisruptionDays:        []string{"Friday", "Saturday", "Sunday"},
						},
					},
				},
			}
			wc1 := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wc1Name, Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 15,
						AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
							{Name: "Friday Maintenance", DaysOfWeek: []string{"Friday"}, TimeZone: "America/Toronto", StartTime: "00:00", EndTime: "23:59"},
						},
					},
				},
			}
			wc2 := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wc2Name, Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 15,
						AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
							{Name: "Saturday Maintenance", DaysOfWeek: []string{"Saturday"}, TimeZone: "America/Toronto", StartTime: "00:00", EndTime: "23:59"},
						},
					},
				},
			}
			wc3 := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wc3Name, Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 15,
						AllowedDisruptionWindows: []workloadsv1.DisruptionWindow{
							{Name: "Sunday Maintenance", DaysOfWeek: []string{"Sunday"}, TimeZone: "America/Toronto", StartTime: "00:00", EndTime: "23:59"},
						},
					},
				},
			}

			// Clean up objects after the test finishes
			defer func() {
				_ = k8sClient.Delete(ctx, wcg)
				_ = k8sClient.Delete(ctx, wc1)
				_ = k8sClient.Delete(ctx, wc2)
				_ = k8sClient.Delete(ctx, wc3)
			}()

			Expect(k8sClient.Create(ctx, wcg)).To(Succeed())
			Expect(k8sClient.Create(ctx, wc1)).To(Succeed())
			Expect(k8sClient.Create(ctx, wc2)).To(Succeed())
			Expect(k8sClient.Create(ctx, wc3)).To(Succeed())

			controllerReconciler := &WorkloadClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Run initial reconcile for all WorkloadClasses to set initial status
			for _, wcName := range []string{wc1Name, wc2Name, wc3Name} {
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: wcName, Namespace: "default"},
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify initial creation was processed
			wcList := &workloadsv1.WorkloadClassList{}
			Expect(k8sClient.List(ctx, wcList)).To(Succeed())
			validCount := 0
			for _, wc := range wcList.Items {
				for _, c := range wc.Status.Conditions {
					if c.Reason == workloadsv1.ReasonValidationPassed {
						validCount++
					}
				}
			}
			Expect(validCount).To(Equal(3))

			// Update the guardrail
			guardrail := &workloadsv1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, wcgNamespacedName, guardrail)).To(Succeed())
			guardrail.Spec.Constraints.Disruption.AllowedDisruptionDays = tc.newAllowedDays
			Expect(k8sClient.Update(ctx, guardrail)).To(Succeed())

			// Emulate the Watch event triggering the map function and enqueuing requests
			requests := controllerReconciler.findWorkloadClassesToReconcile(ctx, guardrail)
			for _, req := range requests {
				_, err := controllerReconciler.Reconcile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify the updated status matches expectations
			Expect(k8sClient.List(ctx, wcList)).To(Succeed())
			invalidatedWCs := []string{}
			for _, wc := range wcList.Items {
				for _, c := range wc.Status.Conditions {
					if c.Reason == workloadsv1.ReasonValidationFailed {
						invalidatedWCs = append(invalidatedWCs, wc.Name)
					}
				}
			}
			Expect(ElementsMatch(invalidatedWCs, tc.wantInvalidated)).To(BeTrue(), "expected invalidated %v to match %v", invalidatedWCs, tc.wantInvalidated)
		})

	}
})

// ElementsMatch checks if two slices contain the exact same elements, regardless of order.
func ElementsMatch(a, b []string) bool {
	// If lengths differ, they can't have the same items
	if len(a) != len(b) {
		return false
	}

	// Count occurrences of each item in the first slice
	counts := make(map[string]int)
	for _, item := range a {
		counts[item]++
	}

	// Verify occurrences match in the second slice
	for _, item := range b {
		counts[item]--
		if counts[item] < 0 {
			return false // Either item not in 'a', or appears more times in 'b'
		}
	}

	return true
}
