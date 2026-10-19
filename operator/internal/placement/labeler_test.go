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

package placement

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lmcachev1alpha1 "github.com/LMCache/LMCache/api/v1alpha1"
	"github.com/LMCache/LMCache/internal/resources"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sc := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sc); err != nil {
		t.Fatal(err)
	}
	if err := lmcachev1alpha1.AddToScheme(sc); err != nil {
		t.Fatal(err)
	}
	return sc
}

func newEngine(ns, name string, delaySecs int32) *lmcachev1alpha1.LMCacheEngine {
	e := &lmcachev1alpha1.LMCacheEngine{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}
	e.Spec.L1.SizeGB = 8
	e.Spec.WorkloadAwareScheduling = &lmcachev1alpha1.WorkloadAwareSchedulingSpec{
		Enabled: true,
		WorkloadSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "predictor"},
		},
		ReleaseDelaySeconds: &delaySecs,
	}
	e.SetDefaults()
	return e
}

func boundPod(ns, name, node string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func nodeNamed(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"kubernetes.io/hostname": name}},
	}
}

func labelSnapshot(ctx context.Context, t *testing.T, c client.Client, key string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes.Items {
		if v, ok := n.Labels[key]; ok && v == resources.WorkloadLabelValue {
			out[n.Name] = true
		}
	}
	return out
}

func mkClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&lmcachev1alpha1.LMCacheEngine{}).
		Build()
}

func mustReconcile(ctx context.Context, t *testing.T, c client.Client, e *lmcachev1alpha1.LMCacheEngine, now time.Time) time.Duration {
	t.Helper()
	delay, err := Reconcile(ctx, c, e, now)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return delay
}

func TestReconcile_LabelsNodesHostingBoundPods(t *testing.T) {
	ctx := context.Background()
	e := newEngine("mlservices", "engine", 300)
	c := mkClient(t,
		e,
		nodeNamed("gpu-a"), nodeNamed("gpu-b"), nodeNamed("gpu-c"),
		boundPod("mlservices", "p1", "gpu-a", map[string]string{"app": "predictor"}),
		boundPod("mlservices", "p2", "gpu-b", map[string]string{"app": "predictor"}),
		// Wrong labels and unbound pods must not trigger labels.
		boundPod("mlservices", "p3", "gpu-c", map[string]string{"app": "other"}),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p4", Namespace: "mlservices", Labels: map[string]string{"app": "predictor"}},
			Spec:   corev1.PodSpec{ /* unbound */ },
			Status: corev1.PodStatus{Phase: corev1.PodPending}},
	)
	// workloads in another namespace must not count
	other := boundPod("other-ns", "p5", "gpu-c", map[string]string{"app": "predictor"})
	if err := c.Create(ctx, other); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	mustReconcile(ctx, t, c, e, now)

	key, _ := resources.WorkloadPlacementLabel("mlservices", "engine")
	got := labelSnapshot(ctx, t, c, key)
	want := map[string]bool{"gpu-a": true, "gpu-b": true}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("labels mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"gpu-a", "gpu-b"}, e.Status.WorkloadNodes); diff != "" {
		t.Fatalf("status.workloadNodes mismatch (-want +got):\n%s", diff)
	}
	if e.Status.WorkloadLabelKey != key {
		t.Fatalf("status.workloadLabelKey = %q, want %q", e.Status.WorkloadLabelKey, key)
	}
}

func TestReconcile_KeepsLabelThroughoutReleaseWindow(t *testing.T) {
	ctx := context.Background()
	e := newEngine("mlservices", "engine", 300)
	pod := boundPod("mlservices", "p1", "gpu-a", map[string]string{"app": "predictor"})
	c := mkClient(t, e, nodeNamed("gpu-a"), pod)

	t0 := time.Now()
	mustReconcile(ctx, t, c, e, t0)

	// Workload leaves the node; still inside the 300s window.
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	delay := mustReconcile(ctx, t, c, e, t0.Add(60*time.Second))
	if delay <= 0 || delay > 240*time.Second {
		t.Fatalf("expected requeue within the remaining window, got %v", delay)
	}

	key, _ := resources.WorkloadPlacementLabel("mlservices", "engine")
	if got := labelSnapshot(ctx, t, c, key); !got["gpu-a"] {
		t.Fatalf("expected gpu-a to keep the label inside the window, got %v", got)
	}

	// After the window: label removed.
	e.Status.NodeLastSeen = map[string]metav1.Time{"gpu-a": {Time: t0}} // deterministic clock
	if err := c.Status().Update(ctx, e); err != nil {
		t.Fatal(err)
	}
	mustReconcile(ctx, t, c, e, t0.Add(301*time.Second))
	if got := labelSnapshot(ctx, t, c, key); got["gpu-a"] {
		t.Fatalf("expected gpu-a label removed after the window, got %v", got)
	}
}

func TestReconcile_RemovesLabelImmediatelyAtZeroDelay(t *testing.T) {
	ctx := context.Background()
	e := newEngine("mlservices", "engine", 0)
	pod := boundPod("mlservices", "p1", "gpu-a", map[string]string{"app": "predictor"})
	c := mkClient(t, e, nodeNamed("gpu-a"), pod)

	t0 := time.Now()
	mustReconcile(ctx, t, c, e, t0)
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	mustReconcile(ctx, t, c, e, t0.Add(time.Second))

	key, _ := resources.WorkloadPlacementLabel("mlservices", "engine")
	if got := labelSnapshot(ctx, t, c, key); got["gpu-a"] {
		t.Fatalf("expected label removed with zero delay, got %v", got)
	}
}

func TestReconcile_DisabledStripsResidue(t *testing.T) {
	ctx := context.Background()
	e := newEngine("mlservices", "engine", 300)
	c := mkClient(t, e, nodeNamed("gpu-a"), nodeNamed("gpu-b"),
		boundPod("mlservices", "p1", "gpu-a", map[string]string{"app": "predictor"}))
	now := time.Now()
	mustReconcile(ctx, t, c, e, now)

	key, _ := resources.WorkloadPlacementLabel("mlservices", "engine")
	if got := labelSnapshot(ctx, t, c, key); !got["gpu-a"] {
		t.Fatalf("precondition: gpu-a should be labeled")
	}

	// Feature turned off: residue must disappear.
	e.Spec.WorkloadAwareScheduling.Enabled = false
	if err := c.Update(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, e); err != nil {
		t.Fatal(err)
	}
	mustReconcile(ctx, t, c, e, now.Add(time.Second))
	if got := labelSnapshot(ctx, t, c, key); len(got) != 0 {
		t.Fatalf("expected all labels stripped when disabled, got %v", got)
	}
}

func TestReconcile_IgnoresTerminatingAndTerminalPods(t *testing.T) {
	ctx := context.Background()
	e := newEngine("mlservices", "engine", 0)
	terminating := boundPod("mlservices", "p1", "gpu-a", map[string]string{"app": "predictor"})
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	terminating.Finalizers = []string{"test/finalizer"} // fake client requires finalizers on terminating objects
	failed := boundPod("mlservices", "p2", "gpu-b", map[string]string{"app": "predictor"})
	failed.Status.Phase = corev1.PodFailed

	c := mkClient(t, e, nodeNamed("gpu-a"), nodeNamed("gpu-b"), terminating)
	if err := c.Create(ctx, failed); err != nil {
		t.Fatal(err)
	}
	mustReconcile(ctx, t, c, e, time.Now())

	key, _ := resources.WorkloadPlacementLabel("mlservices", "engine")
	if got := labelSnapshot(ctx, t, c, key); len(got) != 0 {
		t.Fatalf("expected no labels from terminating/terminal pods, got %v", got)
	}
}

func TestStripAll_RemovesEveryManagedLabel(t *testing.T) {
	ctx := context.Background()
	e := newEngine("mlservices", "engine", 300)
	e2 := newEngine("mlservices", "engine-2", 300)
	p1 := boundPod("mlservices", "p1", "gpu-a", map[string]string{"app": "predictor"})
	p2 := boundPod("mlservices", "q1", "gpu-b", map[string]string{"app": "predictor"})
	c := mkClient(t, e, e2, nodeNamed("gpu-a"), nodeNamed("gpu-b"), p1, p2)
	now := time.Now()
	mustReconcile(ctx, t, c, e, now)
	mustReconcile(ctx, t, c, e2, now)

	key1, _ := resources.WorkloadPlacementLabel("mlservices", "engine")
	key2, _ := resources.WorkloadPlacementLabel("mlservices", "engine-2")
	if snap := labelSnapshot(ctx, t, c, key1); !snap["gpu-a"] {
		t.Fatal("precondition: engine label on gpu-a")
	}
	if snap := labelSnapshot(ctx, t, c, key2); !snap["gpu-b"] {
		t.Fatal("precondition: engine-2 label on gpu-b")
	}

	// Stripping engine 1 must not touch engine 2's label (per-engine keys).
	if err := StripAll(ctx, c, e); err != nil {
		t.Fatal(err)
	}
	if snap := labelSnapshot(ctx, t, c, key1); len(snap) != 0 {
		t.Fatalf("engine labels should be gone, got %v", snap)
	}
	if snap := labelSnapshot(ctx, t, c, key2); !snap["gpu-b"] {
		t.Fatalf("engine-2 labels must survive, got %v", snap)
	}
}

func TestSelectorRejects(t *testing.T) {
	e := newEngine("mlservices", "engine", 300)
	e.Spec.WorkloadAwareScheduling.WorkloadSelector = &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "x", Operator: "BogusOperator", Values: []string{"y"}},
		},
	}
	if _, err := Selector(e); err == nil {
		t.Fatal("expected selector parse error")
	}
	if Matches(e, map[string]string{"x": "y"}) {
		t.Fatal("malformed selector must not match anything")
	}
}
