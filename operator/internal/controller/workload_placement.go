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
	"fmt"
	"maps"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lmcachev1alpha1 "github.com/LMCache/LMCache/api/v1alpha1"
	"github.com/LMCache/LMCache/internal/resources"
)

// Workload-aware engine placement: the controller mirrors a per-engine label
// (resources.WorkloadPlacementLabel) onto every node hosting a bound pod
// matching spec.workloadAwareScheduling.workloadSelector, and the DaemonSet
// nodeSelector (resources.EffectiveNodeSelector) requires it. Engine pods
// therefore only exist where the workload runs; a node loses its label — and
// its engine pod — releaseDelaySeconds after the node is FIRST OBSERVED
// without the workload. Anchoring the window on observed absence (rather than
// last-seen presence) means a controller interruption that misses the actual
// departure still grants the full delay, and a labeled node whose
// bookkeeping predates this operator gets a full window from its first
// handling rather than being cleaned up instantly.

// workloadSelectorFor parses spec.workloadAwareScheduling.workloadSelector.
// Malformed selectors error here; the validating webhook rejects them at
// admission, so a parse failure only surfaces if validation was bypassed.
func workloadSelectorFor(engine *lmcachev1alpha1.LMCacheEngine) (labels.Selector, error) {
	parsed, err := metav1.LabelSelectorAsSelector(engine.Spec.WorkloadAwareScheduling.WorkloadSelector)
	if err != nil {
		return nil, fmt.Errorf("parsing workloadSelector: %w", err)
	}
	return parsed, nil
}

// workloadMatches reports whether podLabels belong to the engine's workload.
// Malformed selectors never match.
func workloadMatches(engine *lmcachev1alpha1.LMCacheEngine, podLabels map[string]string) bool {
	sel, err := workloadSelectorFor(engine)
	if err != nil {
		return false
	}
	return sel.Matches(labels.Set(podLabels))
}

// desiredWorkloadNodes returns the sorted, deduplicated nodes hosting bound,
// non-terminating workload pods in the engine's namespace. "Bound"
// (spec.nodeName set) rather than "Ready" deliberately: the engine must be
// booting while the workload still loads weights, and both finish long
// before the KV connector's server handshake timeout.
func desiredWorkloadNodes(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine) ([]string, error) {
	sel, err := workloadSelectorFor(engine)
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

// reconcileWorkloadPlacement syncs the managed node labels for one engine and
// returns the wait until the next node exits its release window (zero when
// nothing is pending), which the caller turns into a RequeueAfter.
//
// Failure direction: an error adding a needed label fails loudly (the
// workload would lack an engine); a node that must lose its label but cannot
// is retried — sticky engines are the safe side of the asymmetry, and the
// only side affinity-only placement ever had.
func reconcileWorkloadPlacement(
	ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine, now time.Time,
) (time.Duration, error) {
	key, value := resources.WorkloadPlacementLabel(engine.Namespace, engine.Name)

	if !engine.WorkloadSchedulingEnabled() {
		return 0, reconcileWorkloadPlacementOff(ctx, c, engine, key)
	}

	desired, err := desiredWorkloadNodes(ctx, c, engine)
	if err != nil {
		return 0, fmt.Errorf("listing workload pods: %w", err)
	}
	desiredSet := make(map[string]struct{}, len(desired))
	for _, n := range desired {
		desiredSet[n] = struct{}{}
	}

	labeled, err := listLabeledNodes(ctx, c, key)
	if err != nil {
		return 0, err
	}

	// Copy-on-write bookkeeping: absence entries are only persisted when a
	// pass actually changed them, guarding against chronic status churn.
	absentSince := maps.Clone(engine.Status.NodeAbsentSince)
	if absentSince == nil {
		absentSince = map[string]metav1.Time{}
	}

	// The passes mutate nodes and the absentSince clone; the status write
	// inside updateWorkloadPlacementStatus is self-guarding — it compares
	// against engine.Status and persists only when something moved.
	if err := ensureDesiredWorkloadLabels(ctx, c, labeled, desired, key, value, absentSince); err != nil {
		return 0, err
	}

	nextExpiry, err := releaseExpiredWorkloadNodes(ctx, c, labeled, desiredSet, absentSince, engine.WorkloadReleaseDelay(), now, key)
	if err != nil {
		return 0, err
	}

	pruneStaleWorkloadAbsence(absentSince, labeled, desiredSet)

	if err := updateWorkloadPlacementStatus(ctx, c, engine, key, desiredSet, absentSince); err != nil {
		return 0, err
	}

	if nextExpiry.IsZero() {
		return 0, nil
	}
	return nextExpiry.Sub(now), nil
}

// reconcileWorkloadPlacementOff handles a disabled feature: strip residue
// from any previous enable and reset the bookkeeping, only writing status
// when something actually changed.
func reconcileWorkloadPlacementOff(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine, key string) error {
	if err := purgeWorkloadLabels(ctx, c, key); err != nil {
		return err
	}
	changed := false
	if engine.Status.NodeAbsentSince != nil {
		engine.Status.NodeAbsentSince = nil
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
	if !changed {
		return nil
	}
	return c.Status().Update(ctx, engine)
}

// listLabeledNodes returns the nodes currently carrying the managed label.
func listLabeledNodes(ctx context.Context, c client.Client, key string) (map[string]*corev1.Node, error) {
	labeled := map[string]*corev1.Node{}
	nodeList := &corev1.NodeList{}
	if err := c.List(ctx, nodeList, client.MatchingLabels{key: resources.WorkloadLabelValue}); err != nil {
		return nil, fmt.Errorf("listing labeled nodes: %w", err)
	}
	for i := range nodeList.Items {
		labeled[nodeList.Items[i].Name] = &nodeList.Items[i]
	}
	return labeled, nil
}

// ensureDesiredWorkloadLabels labels every desired node and clears absence
// bookkeeping the moment the workload returns to a node.
func ensureDesiredWorkloadLabels(
	ctx context.Context, c client.Client,
	labeled map[string]*corev1.Node,
	desired []string,
	key, value string,
	absentSince map[string]metav1.Time,
) error {
	for _, node := range desired {
		if n, ok := labeled[node]; ok {
			if err := addWorkloadLabel(ctx, c, n, key, value); err != nil {
				return err
			}
		} else {
			fresh := &corev1.Node{}
			if err := c.Get(ctx, client.ObjectKey{Name: node}, fresh); err != nil {
				return err
			}
			if err := addWorkloadLabel(ctx, c, fresh, key, value); err != nil {
				return err
			}
		}
		delete(absentSince, node)
	}
	return nil
}

// releaseExpiredWorkloadNodes runs the release windows for labeled nodes the
// workload has left: the window starts at the first reconcile that observes
// the absence — never at a historical last-seen time, so a controller outage
// crossing the actual departure still grants the full delay, and untracked
// labels (operator predating the bookkeeping) get a full window too. Returns
// the earliest pending expiry for the caller's requeue.
func releaseExpiredWorkloadNodes(
	ctx context.Context,
	c client.Client,
	labeled map[string]*corev1.Node,
	desiredSet map[string]struct{},
	absentSince map[string]metav1.Time,
	releaseDelay time.Duration,
	now time.Time,
	key string,
) (time.Time, error) {
	var nextExpiry time.Time
	for nodeName, node := range labeled {
		if _, ok := desiredSet[nodeName]; ok {
			continue
		}
		since, tracked := absentSince[nodeName]
		if !tracked {
			since = metav1.Time{Time: now}
			absentSince[nodeName] = since
		}
		deadline := since.Add(releaseDelay)
		if now.Before(deadline) {
			if nextExpiry.IsZero() || deadline.Before(nextExpiry) {
				nextExpiry = deadline
			}
			continue // still inside the release window
		}
		if err := removeWorkloadLabel(ctx, c, node, key); err != nil {
			return nextExpiry, err
		}
		delete(absentSince, nodeName)
	}
	return nextExpiry, nil
}

// pruneStaleWorkloadAbsence drops bookkeeping for nodes that are neither
// labeled nor desired (the label was stripped by other means).
func pruneStaleWorkloadAbsence(
	absentSince map[string]metav1.Time,
	labeled map[string]*corev1.Node,
	desiredSet map[string]struct{},
) {
	for nodeName := range absentSince {
		_, isLabeled := labeled[nodeName]
		_, isDesired := desiredSet[nodeName]
		if !isLabeled && !isDesired {
			delete(absentSince, nodeName)
		}
	}
}

// updateWorkloadPlacementStatus persists the kept bookkeeping and the visible
// node set. Empty absence maps are written as nil to avoid chronic churn.
func updateWorkloadPlacementStatus(
	ctx context.Context,
	c client.Client,
	engine *lmcachev1alpha1.LMCacheEngine,
	key string,
	desiredSet map[string]struct{},
	absentSince map[string]metav1.Time,
) error {
	changed := false
	if !timesEqual(absentSince, engine.Status.NodeAbsentSince) {
		if len(absentSince) > 0 {
			engine.Status.NodeAbsentSince = absentSince
		} else {
			engine.Status.NodeAbsentSince = nil
		}
		changed = true
	}

	visible := make([]string, 0, len(desiredSet)+len(absentSince))
	for n := range desiredSet {
		visible = append(visible, n)
	}
	for n := range absentSince {
		visible = append(visible, n)
	}
	sort.Strings(visible)
	if !stringsEqual(visible, engine.Status.WorkloadNodes) {
		engine.Status.WorkloadNodes = visible
		changed = true
	}

	if engine.Status.WorkloadLabelKey != key {
		engine.Status.WorkloadLabelKey = key
		changed = true
	}

	if !changed {
		return nil
	}
	return c.Status().Update(ctx, engine)
}

// stripWorkloadPlacementLabels removes every node label managed by this
// engine. Called from the CR finalizer: node labels are cluster-scoped, so
// ownership garbage collection cannot clean them.
func stripWorkloadPlacementLabels(ctx context.Context, c client.Client, engine *lmcachev1alpha1.LMCacheEngine) error {
	key, _ := resources.WorkloadPlacementLabel(engine.Namespace, engine.Name)
	return purgeWorkloadLabels(ctx, c, key)
}

func purgeWorkloadLabels(ctx context.Context, c client.Client, key string) error {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes, client.MatchingLabels{key: resources.WorkloadLabelValue}); err != nil {
		return err
	}
	for i := range nodes.Items {
		if err := removeWorkloadLabel(ctx, c, &nodes.Items[i], key); err != nil {
			return err
		}
	}
	return nil
}

func addWorkloadLabel(ctx context.Context, c client.Client, node *corev1.Node, key, value string) error {
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

func removeWorkloadLabel(ctx context.Context, c client.Client, node *corev1.Node, key string) error {
	if _, ok := node.Labels[key]; !ok {
		return nil
	}
	patch := client.MergeFrom(node.DeepCopy())
	delete(node.Labels, key)
	return c.Patch(ctx, node, patch)
}

// stringsEqual compares two ordered string slices; used to guard status
// writes against no-op churn.
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
