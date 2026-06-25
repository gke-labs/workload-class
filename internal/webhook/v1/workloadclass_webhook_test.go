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
)

var _ = Describe("WorkloadClass Webhook", func() {
	var (
		obj       *workloadsv1.WorkloadClass
		oldObj    *workloadsv1.WorkloadClass
		validator WorkloadClassCustomValidator
		defaulter WorkloadClassCustomDefaulter
	)

	BeforeEach(func() {
		obj = &workloadsv1.WorkloadClass{}
		oldObj = &workloadsv1.WorkloadClass{}
		validator = WorkloadClassCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		defaulter = WorkloadClassCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
	})

	AfterEach(func() {})

	Context("When creating or updating WorkloadClass under Validating Webhook", func() {
		It("Should deny creation if AllowedDisruptionsOutsideOfWindow contains invalid values", func() {
			By("simulating an invalid creation scenario")
			obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{VPA, "ClusterAutobot"}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(warnings).To(ContainElement(ContainSubstring("invalid identities found in allowedDisruptionsOutsideOfWindow: ClusterAutobot")))
		})

		It("Should deny creation if AllowedDisruptionsOutsideOfWindow contains invalid values - case-sensitive", func() {
			By("simulating an invalid creation scenario")
			obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{"Vpa"}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(warnings).To(ContainElement(ContainSubstring("invalid identities found in allowedDisruptionsOutsideOfWindow: Vpa")))
		})

		It("Should admit creation if AllowedDisruptionsOutsideOfWindow only contains valid values", func() {
			By("simulating an invalid creation scenario")
			obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{VPA, CA}
			Expect(validator.ValidateCreate(ctx, obj)).To(BeNil())
		})

		It("Should validate updates correctly", func() {
			By("simulating a valid update scenario")
			oldObj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{CA}
			obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{VPA, CA}
			Expect(validator.ValidateUpdate(ctx, oldObj, obj)).To(BeNil())
		})
	})

	Context("When creating WorkloadClass under Defaulting Webhook", func() {
		It("Should remove duplicate values from AllowedDisruptionsOutsideOfWindow", func() {
			By("simulating a scenario where defaults should be applied")
			obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{VPA, VPA, VPA}
			By("calling the Default method to apply defaults")
			err := defaulter.Default(ctx, obj)
			By("checking that the default values are set")
			Expect(obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow).To(Equal([]string{VPA}))
			Expect(err).ToNot(HaveOccurred())
		})
		It("Should not change values in AllowedDisruptionsOutsideOfWindow if it has no duplicates", func() {
			By("simulating a scenario where defaults should be applied")
			obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow = []string{VPA, CA}
			By("calling the Default method to apply defaults")
			err := defaulter.Default(ctx, obj)
			By("checking that the default values are set")
			// Also sorts the strings
			Expect(obj.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow).To(Equal([]string{CA, VPA}))
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
