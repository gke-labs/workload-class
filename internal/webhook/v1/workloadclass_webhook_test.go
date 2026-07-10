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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("WorkloadClass Webhook", func() {
	var (
		obj       *workloadsv1.WorkloadClass
		oldObj    *workloadsv1.WorkloadClass
		validator WorkloadClassCustomValidator
		defaulter WorkloadClassCustomDefaulter
	)

	BeforeEach(func() {
		obj = &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-wc",
				Namespace: "default",
			},
		}
		oldObj = &workloadsv1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-wc",
				Namespace: "default",
			},
		}
		validator = WorkloadClassCustomValidator{Client: k8sClient}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = WorkloadClassCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	Context("When creating or updating WorkloadClass under Validating Webhook", func() {
		It("Should admit creation if there are no other workloadclass defaults", func() {
			obj.Labels = map[string]string{"workloads.gke.io/default-class": "true"}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(warnings).To(BeEmpty())
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should deny creation if there is another workloadclass default", func() {
			existingWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-wc-1",
					Namespace: "default",
					Labels:    map[string]string{"workloads.gke.io/default-class": "true"},
				},
			}
			Expect(k8sClient.Create(ctx, existingWC)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, existingWC)).To(Succeed())
			}()

			obj.Labels = map[string]string{"workloads.gke.io/default-class": "true"}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(warnings).To(HaveLen(1))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already contains a namespace default: existing-wc-1"))
		})

		It("Should deny updates if it adds the namespace default label and there is already a default", func() {
			existingWC := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-wc-2",
					Namespace: "default",
					Labels:    map[string]string{"workloads.gke.io/default-class": "true"},
				},
			}
			Expect(k8sClient.Create(ctx, existingWC)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, existingWC)).To(Succeed())
			}()

			obj.Labels = map[string]string{"workloads.gke.io/default-class": "true"}
			warnings, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(warnings).To(HaveLen(1))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already contains a namespace default: existing-wc-2"))
		})
	})

})
