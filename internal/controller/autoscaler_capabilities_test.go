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
	"encoding/json"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gke-labs/workload-class/internal/utils"
)

// vpaCRD returns a VerticalPodAutoscaler CustomResourceDefinition whose v1 schema allows the given update modes.
func vpaCRD(t *testing.T, modes ...string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	enum := make([]apiextensionsv1.JSON, 0, len(modes))
	for _, mode := range modes {
		raw, err := json.Marshal(mode)
		if err != nil {
			t.Fatalf("failed to marshal %q: %v", mode, err)
		}
		enum = append(enum, apiextensionsv1.JSON{Raw: raw})
	}
	updateMode := apiextensionsv1.JSONSchemaProps{Type: "string", Enum: enum}
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: VPACustomResourceDefinitionName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1beta2"},
				{
					Name: "v1",
					Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"updatePolicy": {Properties: map[string]apiextensionsv1.JSONSchemaProps{"updateMode": updateMode}},
							}},
						},
					}},
				},
			},
		},
	}
}

func TestDetectVPA(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to build scheme: %v", err)
	}
	withVPA := meta.NewDefaultRESTMapper(nil)
	withVPA.Add(vpav1.SchemeGroupVersion.WithKind("VerticalPodAutoscaler"), meta.RESTScopeNamespace)
	withoutVPA := meta.NewDefaultRESTMapper(nil)

	testCases := []struct {
		name          string
		mapper        meta.RESTMapper
		objects       []client.Object
		wantInstalled bool
		wantInPlace   bool
		wantErr       bool
	}{
		{name: "not_installed", mapper: withoutVPA},
		{
			name:          "vpa_1_4_or_later",
			mapper:        withVPA,
			objects:       []client.Object{vpaCRD(t, "Off", "Initial", "Recreate", "InPlaceOrRecreate", "Auto")},
			wantInstalled: true,
			wantInPlace:   true,
		},
		{
			name:          "older_vpa",
			mapper:        withVPA,
			objects:       []client.Object{vpaCRD(t, "Off", "Initial", "Recreate", "Auto")},
			wantInstalled: true,
		},
		{
			name:          "no_enum",
			mapper:        withVPA,
			objects:       []client.Object{vpaCRD(t)},
			wantInstalled: true,
		},
		{
			name:          "unreadable_crd",
			mapper:        withVPA,
			wantInstalled: true,
			wantErr:       true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			caps := utils.AutoscalerCapabilities{InPlaceOrRecreate: true}
			err := DetectVPA(t.Context(), tc.mapper, reader, &caps)
			if (err != nil) != tc.wantErr {
				t.Errorf("DetectVPA() error = %v, want error: %t", err, tc.wantErr)
			}
			if caps.VPAInstalled != tc.wantInstalled || caps.InPlaceOrRecreate != tc.wantInPlace {
				t.Errorf("DetectVPA() = (installed %t, in-place %t), want (%t, %t)",
					caps.VPAInstalled, caps.InPlaceOrRecreate, tc.wantInstalled, tc.wantInPlace)
			}
		})
	}
}
