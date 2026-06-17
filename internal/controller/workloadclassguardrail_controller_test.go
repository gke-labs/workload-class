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
			err := k8sClient.Update(ctx, g)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Unsupported value"))
		})
	})
})
