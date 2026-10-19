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

// Package placement implements workload-aware engine scheduling: the
// operator mirrors a per-engine label (resources.WorkloadPlacementLabel)
// onto every node hosting a bound pod matching the engine's workloadSelector,
// and the DaemonSet nodeSelector (resources.EffectiveNodeSelector) requires
// that label. Engine pods therefore only exist where the workload runs, and
// are removed after the workload has been gone for releaseDelaySeconds —
// keeping the warm KV cache across typical same-node rolling updates while
// still cleaning up after genuine departures.
package placement

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lmcachev1alpha1 "github.com/LMCache/LMCache/api/v1alpha1"
	"github.com/LMCache/LMCache/internal/resources"
)

// Selector returns the labels.Selector for an engine's workload, or an
// error when the configured LabelSelector is malformed. Validation rejects
// malformed selectors when the feature is enabled; a later parse error
// therefore only occurs if validation was bypassed.
func Selector(engine *lmcachev1alpha1.LMCacheEngine) (labels.Selector, error) {
	parsed, err := metav1.LabelSelectorAsSelector(engine.Spec.WorkloadAwareScheduling.WorkloadSelector)
	if err != nil {
		return nil, fmt.Errorf("parsing workloadSelector: %w", err)
	}
	return parsed, nil
}

// Matches reports whether podLabels belong to the engine's workload.
// Malformed selectors never match.
func Matches(engine *lmcachev1alpha1.LMCacheEngine, podLabels map[string]string) bool {
	sel, err := Selector(engine)
	if err != nil {
		return false
	}
	return sel.Matches(labels.Set(podLabels))
}

// DesiredNodes returns the sorted, deduplicated list of nodes hosting bound,
// non-terminating workload pods in the engine's namespace. "Bound"
// (spec.nodeName set) rather than "Ready" deliberately: the engine must be
// booting while the workload still loads weights; both finish long before
// the KV connector's server handshake timeout elapses.
func DesiredNodes(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine) ([]string, error) {
	sel, err := Selector(engine)
	if err != nil {
		return nil, err
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods,
		client.InNamespace(engine.Namespace),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	nodes := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if _, dup := seen[pod.Spec.NodeName]; dup {
			continue
		}
		seen[pod.Spec.NodeName] = struct{}{}
		nodes = append(nodes, pod.Spec.NodeName)
	}
	sort.Strings(nodes)
	return nodes, nil
}

// Reconcile syncs the managed node labels for one engine. It returns the
// duration until the next node exits its release window (zero when nothing
// is pending), which the controller turns into a RequeueAfter.
//
// Failure direction: any error adding a label requeues the whole engine; any
// node that must be labeled but momentarily cannot be stays unlabeled, which
// fails explicitly (no engine where the workload is) rather than silently.
// The opposite mistake — labels left on nodes the workload left — is the
// safe one resources already suffer under affinity-only placement.
func Reconcile(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine, now time.Time) (time.Duration, error) {
	key, value := resources.WorkloadPlacementLabel(engine.Namespace, engine.Name)

	// Disabled or never enabled: strip any residue from a previous enable
	// and reset the bookkeeping — only writing status when something
	// actually changes, so a stable disabled CR does not churn.
	if w := engine.Spec.WorkloadAwareScheduling; w == nil || !w.Enabled {
		if err := purge(ctx, c, key); err != nil {
			return 0, err
		}
		changed := false
		if engine.Status.NodeLastSeen != nil {
			engine.Status.NodeLastSeen = nil
			changed = true
		}
		if len(engine.Status.WorkloadNodes) > 0 {
			engine.Status.WorkloadNodes = nil
			changed = true
		}
		if engine.Status.WorkloadLabelKey != "" {
			engine.Status.WorkloadLabelKey = ""
			changed = true
		}
		if changed {
			return 0, updateStatus(ctx, c, engine)
		}
		return 0, nil
	}

	desired, err := DesiredNodes(ctx, c, engine)
	if err != nil {
		return 0, fmt.Errorf("listing workload pods: %w", err)
	}
	desiredSet := make(map[string]struct{}, len(desired))
	for _, n := range desired {
		desiredSet[n] = struct{}{}
	}

	// Nodes currently carrying the label.
	labeled := map[string]*corev1.Node{}
	nodeList := &corev1.NodeList{}
	if err := c.List(ctx, nodeList, client.MatchingLabels{key: value}); err != nil {
		return 0, fmt.Errorf("listing labeled nodes: %w", err)
	}
	for i := range nodeList.Items {
		labeled[nodeList.Items[i].Name] = &nodeList.Items[i]
	}

	releaseDelay := engine.WorkloadReleaseDelay()
	lastSeen := engine.Status.NodeLastSeen
	if lastSeen == nil {
		lastSeen = map[string]metav1.Time{}
	}

	// 1) Label every desired node that does not carry it yet. Errors are
	//    returned: a missing label where the workload runs must not be
	//    silent.
	for _, node := range desired {
		if n, ok := labeled[node]; ok {
			if err := addLabel(ctx, c, n, key, value); err != nil {
				return 0, err
			}
			continue
		}
		fresh := &corev1.Node{}
		if err := c.Get(ctx, client.ObjectKey{Name: node}, fresh); err != nil {
			return 0, err
		}
		if err := addLabel(ctx, c, fresh, key, value); err != nil {
			return 0, err
		}
	}

	// 2) Labeled nodes no longer desired keep their label until
	//    releaseDelay passes since they were last desired, then lose it.
	var nextExpiry time.Time
	keep := map[string]metav1.Time{} // non-desired nodes still in the window
	for nodeName, node := range labeled {
		if _, ok := desiredSet[nodeName]; ok {
			continue
		}
		since, tracked := lastSeen[nodeName]
		if !tracked {
			// Untracked (e.g. labeled by an operator version predating the
			// bookkeeping): the label's own creation time is the best proxy
			// for when it became stale; fall back to immediate removal when
			// the node object provides nothing.
			since = metav1.Time{Time: node.CreationTimestamp.Add(0)}
			if deadline := since.Add(releaseDelay); now.Before(deadline) {
				keep[nodeName] = since
				if nextExpiry.IsZero() || deadline.Before(nextExpiry) {
					nextExpiry = deadline
				}
				continue
			}
			if err := removeLabel(ctx, c, node, key); err != nil {
				return 0, err
			}
			continue
		}
		deadline := since.Add(releaseDelay)
		if now.Before(deadline) {
			keep[nodeName] = since
			if nextExpiry.IsZero() || deadline.Before(nextExpiry) {
				nextExpiry = deadline
			}
			continue
		}
		if err := removeLabel(ctx, c, node, key); err != nil {
			return 0, err
		}
	}

	// 3) Refresh the bookkeeping: desired nodes were just seen; kept nodes
	//    retain their original timestamp so their window does not restart.
	newSeen := make(map[string]metav1.Time, len(desiredSet)+len(keep))
	for n := range desiredSet {
		newSeen[n] = metav1.Time{Time: now}
	}
	for n, ts := range keep {
		newSeen[n] = ts
	}

	// The visible set: desired plus still-kept nodes.
	visible := make([]string, 0, len(newSeen))
	for n := range newSeen {
		visible = append(visible, n)
	}
	sort.Strings(visible)

	statusChanged := false
	if !timesEqual(newSeen, engine.Status.NodeLastSeen) {
		engine.Status.NodeLastSeen = newSeen
		statusChanged = true
	}
	if !stringsEqual(visible, engine.Status.WorkloadNodes) {
		engine.Status.WorkloadNodes = visible
		statusChanged = true
	}
	if engine.Status.WorkloadLabelKey != key {
		engine.Status.WorkloadLabelKey = key
		statusChanged = true
	}
	if statusChanged {
		if err := updateStatus(ctx, c, engine); err != nil {
			return 0, err
		}
	}

	if nextExpiry.IsZero() {
		return 0, nil
	}
	return nextExpiry.Sub(now), nil
}

// StripAll removes every node label managed by this engine. Called from the
// CR finalizer: node labels are cluster-scoped, so ownership garbage
// collection cannot clean them.
func StripAll(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine) error {
	key, _ := resources.WorkloadPlacementLabel(engine.Namespace, engine.Name)
	return purge(ctx, c, key)
}

func purge(ctx context.Context, c client.Client, key string) error {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes, client.MatchingLabels{key: resources.WorkloadLabelValue}); err != nil {
		return err
	}
	for i := range nodes.Items {
		if err := removeLabel(ctx, c, &nodes.Items[i], key); err != nil {
			return err
		}
	}
	return nil
}

func addLabel(ctx context.Context, c client.Client, node *corev1.Node, key, value string) error {
	if node.Labels[key] == value {
		return nil
	}
	patch := client.MergeFrom(node.DeepCopy())
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[key] = value
	return c.Patch(ctx, node, patch)
}

func removeLabel(ctx context.Context, c client.Client, node *corev1.Node, key string) error {
	if _, ok := node.Labels[key]; !ok {
		return nil
	}
	patch := client.MergeFrom(node.DeepCopy())
	delete(node.Labels, key)
	return c.Patch(ctx, node, patch)
}

func updateStatus(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine) error {
	return c.Status().Update(ctx, engine)
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func timesEqual(a, b map[string]metav1.Time) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		ob, ok := b[k]
		if !ok || !v.Equal(&ob) {
			return false
		}
	}
	return true
}
