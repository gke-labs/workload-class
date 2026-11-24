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

package utils

import (
	"context"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// UpdateWorkloadClassStatus updates a WorkloadClass's status subresource inside a retry.RetryOnConflict loop.
//
// On each attempt, it fetches the latest WorkloadClass using reader (falling back to c when reader is nil)
// so callers can pass an uncached API reader (mgr.GetAPIReader()) and avoid re-reading stale resourceVersions
// from the informer cache after a 409 Conflict. When the update succeeds (or mutate returns false),
// latest is copied back into wc so callers observe the updated status and ResourceVersion without
// clobbering concurrent condition updates from other controllers.
func UpdateWorkloadClassStatus(
	ctx context.Context,
	c client.Client,
	reader client.Reader,
	wc *workloadsv1.WorkloadClass,
	mutate func(latest *workloadsv1.WorkloadClass) bool,
) error {
	if reader == nil {
		reader = c
	}
	key := client.ObjectKeyFromObject(wc)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &workloadsv1.WorkloadClass{}
		if err := reader.Get(ctx, key, latest); err != nil {
			return err
		}
		if !mutate(latest) {
			latest.DeepCopyInto(wc)
			return nil
		}
		if err := c.Status().Update(ctx, latest); err != nil {
			return err
		}
		latest.DeepCopyInto(wc)
		return nil
	})
}

type apiReaderClient struct {
	client.Client
	reader client.Reader
}

func (c apiReaderClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reader.Get(ctx, key, obj, opts...)
}

// ClientWithReader returns a client.Client whose Get calls go through reader (e.g. mgr.GetAPIReader())
// while all other operations delegate to c. If reader is nil, c is returned unchanged.
func ClientWithReader(c client.Client, reader client.Reader) client.Client {
	if reader == nil {
		return c
	}
	return apiReaderClient{Client: c, reader: reader}
}
