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
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// DefaultDisruptionBufferSize is the default capacity of the DisruptionTracker event channel.
const DefaultDisruptionBufferSize = 1024

// DisruptionTracker records observed disruption timestamps from the Disruption Webhook and signals
// the WorkloadClass controller to persist Status.LastDisruptionTime asynchronously.
//
// Like PendingLeaseTracker, this keeps the validating webhook read-only so admission stays fast and
// status writes remain serialized through the WorkloadClass controller's workqueue. The pending map
// is the source of truth; events are only wake-up signals.
type DisruptionTracker struct {
	mu      sync.Mutex
	pending map[types.NamespacedName]time.Time
	events  chan event.GenericEvent
}

// NewDisruptionTracker returns a DisruptionTracker whose event channel has the given buffer size.
// A non-positive bufferSize uses DefaultDisruptionBufferSize.
func NewDisruptionTracker(bufferSize int) *DisruptionTracker {
	if bufferSize <= 0 {
		bufferSize = DefaultDisruptionBufferSize
	}
	return &DisruptionTracker{
		pending: map[types.NamespacedName]time.Time{},
		events:  make(chan event.GenericEvent, bufferSize),
	}
}

// Record stores the latest observed disruption time for the WorkloadClass and signals the controller.
//
// If a timestamp is already pending for the same WorkloadClass, the newer timestamp is retained.
// Record returns true if the controller was signaled, or false if the tracker or WorkloadClass is nil,
// or if the event channel is full (in which case the timestamp is still stored for the next reconcile).
func (t *DisruptionTracker) Record(wc *workloadsv1.WorkloadClass, at time.Time) bool {
	if t == nil || wc == nil {
		return false
	}

	wcKey := types.NamespacedName{Namespace: wc.Namespace, Name: wc.Name}

	t.mu.Lock()
	if existing, ok := t.pending[wcKey]; !ok || at.After(existing) {
		t.pending[wcKey] = at
	}
	t.mu.Unlock()

	return t.signal(wcKey)
}

// Drain atomically retrieves and removes the pending disruption timestamp for the given WorkloadClass.
func (t *DisruptionTracker) Drain(wcKey types.NamespacedName) (time.Time, bool) {
	if t == nil {
		return time.Time{}, false
	}

	t.mu.Lock()
	at, ok := t.pending[wcKey]
	delete(t.pending, wcKey)
	t.mu.Unlock()

	return at, ok
}

// Events returns the channel on which the tracker signals WorkloadClasses with recorded disruptions,
// for use with source.Channel.
func (t *DisruptionTracker) Events() <-chan event.GenericEvent {
	return t.events
}

func (t *DisruptionTracker) signal(wcKey types.NamespacedName) bool {
	obj := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wcKey.Name,
			Namespace: wcKey.Namespace,
		},
	}

	select {
	case t.events <- event.GenericEvent{Object: obj}:
		return true
	default:
		return false
	}
}
