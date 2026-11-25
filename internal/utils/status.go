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

// RetryOnConflictWithReader runs fn inside retry.RetryOnConflict.
//
// Attempt 0 receives the cached client c to avoid live apiserver GETs in steady state.
// Retries after a 409 Conflict switch GET calls to reader (e.g. mgr.GetAPIReader()) when non-nil
// so they do not re-read stale resourceVersions from the informer cache.
func RetryOnConflictWithReader(c client.Client, reader client.Reader, fn func(client.Client) error) error {
	attempt := 0
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rc := c
		if attempt > 0 && reader != nil {
			rc = apiReaderClient{Client: c, reader: reader}
		}
		attempt++
		return fn(rc)
	})
}

// UpdateWorkloadClassStatus updates a WorkloadClass's status subresource inside a retry.RetryOnConflict loop.
//
// Attempt 0 reads from the cached client c; retries after a 409 Conflict read from reader
// (e.g. mgr.GetAPIReader()) when non-nil so they see the latest resourceVersion and preserve
// concurrent condition updates from other controllers. When the update succeeds (or mutate returns false),
// latest is copied back into wc.
func UpdateWorkloadClassStatus(
	ctx context.Context,
	c client.Client,
	reader client.Reader,
	wc *workloadsv1.WorkloadClass,
	mutate func(latest *workloadsv1.WorkloadClass) bool,
) error {
	key := client.ObjectKeyFromObject(wc)
	return RetryOnConflictWithReader(c, reader, func(rc client.Client) error {
		latest := &workloadsv1.WorkloadClass{}
		if err := rc.Get(ctx, key, latest); err != nil {
			return err
		}
		if !mutate(latest) {
			latest.DeepCopyInto(wc)
			return nil
		}
		if err := rc.Status().Update(ctx, latest); err != nil {
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
