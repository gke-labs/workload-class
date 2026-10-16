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

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

var _ = Describe("WorkloadClassGuardrail Webhook", func() {
	var (
		obj       *workloadsv1.WorkloadClassGuardrail
		validator WorkloadClassGuardrailCustomValidator
	)

	BeforeEach(func() {
		obj = &workloadsv1.WorkloadClassGuardrail{}
		validator = WorkloadClassGuardrailCustomValidator{
			Client: k8sClient,
		}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	Context("When creating or updating WorkloadClassGuardrail under Validating Webhook", func() {
		It("Should admit valid guardrail when no WorkloadClasses exist", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "test-guardrail"},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit valid guardrail when compliant WorkloadClass exists", func() {
			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "wc-1",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "test-guardrail"},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should deny creation if a WorkloadClass violates the guardrail", func() {
			wc := &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lax-workloadclass",
					Namespace: "default",
				},
				Spec: workloadsv1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 30, // Violates the guardrail limit of 10
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "strict-guardrail"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1.Constraints{
						Disruption: workloadsv1.Disruption{
							MaxNonDisruptionDurationDays: 10,
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("guardrail is too restrictive"))
			Expect(warnings).NotTo(BeEmpty())
		})

		It("Should deny creation if pluginConstraints are invalid", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid-plugin-guardrail"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					PluginConstraints: &workloadsv1.PluginConstraints{
						Placement: []workloadsv1.PluginConstraint{{
							Name: workloadsv1.PluginNameGKESpotPlacement,
							Parameters: &apiextensionsv1.JSON{
								Raw: []byte(`{"reversion":{"requiredReversionAction":"None","maxFallbackDuration":"2h"}}`),
							},
						}},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid pluginConstraints"))
			Expect(err.Error()).To(ContainSubstring("reversion.maxFallbackDuration cannot be set when reversion.requiredReversionAction is None"))
			Expect(warnings).NotTo(BeEmpty())
		})

		It("Should deny an update that makes pluginConstraints invalid", func() {
			oldObj := &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "plugin-guardrail"},
			}
			newObj := oldObj.DeepCopy()
			newObj.Spec.PluginConstraints = &workloadsv1.PluginConstraints{
				Placement: []workloadsv1.PluginConstraint{{
					Name: workloadsv1.PluginNameGKESpotPlacement,
					Parameters: &apiextensionsv1.JSON{
						Raw: []byte(`{"enforcementMode":"InvalidMode"}`),
					},
				}},
			}
			warnings, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`invalid enforcementMode "InvalidMode"`))
			Expect(warnings).NotTo(BeEmpty())
		})

		It("Should admit a guardrail with valid pluginConstraints", func() {
			obj = &workloadsv1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "valid-plugin-guardrail"},
				Spec: workloadsv1.WorkloadClassGuardrailSpec{
					PluginConstraints: &workloadsv1.PluginConstraints{
						Placement: []workloadsv1.PluginConstraint{{
							Name: workloadsv1.PluginNameGKESpotPlacement,
							Parameters: &apiextensionsv1.JSON{
								Raw: []byte(`{"enforcementMode":"Required","minSpotRatio":"50%","reversion":{"requiredReversionAction":"Active","maxFallbackDuration":"2h"}}`),
							},
						}},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})
	})
})
