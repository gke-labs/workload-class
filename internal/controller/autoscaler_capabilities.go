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
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	vpav1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/workload-class/internal/utils"
)

// VPACustomResourceDefinitionName is the name of the VerticalPodAutoscaler CustomResourceDefinition.
const VPACustomResourceDefinitionName = "verticalpodautoscalers.autoscaling.k8s.io"

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get,resourceNames=verticalpodautoscalers.autoscaling.k8s.io

// DetectVPA reports whether the VerticalPodAutoscaler API is installed, and whether it accepts the InPlaceOrRecreate
// update mode (VPA 1.4 or later), into caps.
//
// The REST mapper tells whether autoscaling.k8s.io/v1 VerticalPodAutoscalers are served. Watching a type that
// isn't served would stop the manager from starting, so the controllers only watch VerticalPodAutoscalers when
// they are. Support for InPlaceOrRecreate is read from the update mode enum of the CustomResourceDefinition. If it
// can't be read, InPlaceOrRecreate is assumed unsupported, and InPlacePreferred falls back to Recreate.
//
// The result is computed once at startup: installing the VerticalPodAutoscaler later takes a restart.
func DetectVPA(ctx context.Context, mapper meta.RESTMapper, reader client.Reader, caps *utils.AutoscalerCapabilities) error {
	gvk := vpav1.SchemeGroupVersion.WithKind("VerticalPodAutoscaler")
	if _, err := mapper.RESTMapping(schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}, gvk.Version); err != nil {
		if meta.IsNoMatchError(err) {
			caps.VPAInstalled, caps.InPlaceOrRecreate = false, false
			return nil
		}
		return fmt.Errorf("failed to look up the VerticalPodAutoscaler API: %w", err)
	}
	caps.VPAInstalled = true

	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := reader.Get(ctx, client.ObjectKey{Name: VPACustomResourceDefinitionName}, crd); err != nil {
		caps.InPlaceOrRecreate = false
		return fmt.Errorf("failed to read the %s CustomResourceDefinition, assuming InPlaceOrRecreate is not supported: %w",
			VPACustomResourceDefinitionName, err)
	}
	caps.InPlaceOrRecreate = supportsUpdateMode(crd, gvk.Version, vpav1.UpdateModeInPlaceOrRecreate)
	return nil
}

// supportsUpdateMode reports whether the schema of a version of the VerticalPodAutoscaler CustomResourceDefinition
// lists the update mode in the enum of spec.updatePolicy.updateMode.
func supportsUpdateMode(crd *apiextensionsv1.CustomResourceDefinition, version string, mode vpav1.UpdateMode) bool {
	for _, v := range crd.Spec.Versions {
		if v.Name != version || v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			continue
		}
		props := v.Schema.OpenAPIV3Schema
		for _, field := range []string{"spec", "updatePolicy", "updateMode"} {
			next, ok := props.Properties[field]
			if !ok {
				return false
			}
			props = &next
		}
		for _, e := range props.Enum {
			var value string
			if err := json.Unmarshal(e.Raw, &value); err == nil && value == string(mode) {
				return true
			}
		}
	}
	return false
}
