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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lmcachev1alpha1 "github.com/LMCache/LMCache/api/v1alpha1"
	"github.com/LMCache/LMCache/internal/placement"
	"github.com/LMCache/LMCache/internal/resources"
)

const finalizerName = "lmcache.ai/cleanup"

// workloadPlacementFinalizer guards the cluster-scoped node labels a
// workload-aware engine manages; ownership garbage collection cannot clean
// node labels, so the finalizer runs placement.StripAll before the CR goes.
const workloadPlacementFinalizer = "lmcache.ai/workload-placement-cleanup"

// LMCacheEngineReconciler reconciles a LMCacheEngine object.
type LMCacheEngineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=lmcache.lmcache.ai,resources=lmcacheengines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=lmcache.lmcache.ai,resources=lmcacheengines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=lmcache.lmcache.ai,resources=lmcacheengines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

// Reconcile reconciles the LMCacheEngine CR.
func (r *LMCacheEngineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch CR
	engine := &lmcachev1alpha1.LMCacheEngine{}
	if err := r.Get(ctx, req.NamespacedName, engine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Handle finalizer
	if err, done := r.handleFinalizer(ctx, engine); done {
		return ctrl.Result{}, err
	}

	// 3. Set defaults
	engine.SetDefaults()

	// 4. Validate
	if err := r.validateAndSetCondition(ctx, engine); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Reconcile RESP auth secret (cross-namespace copy if needed)
	if err := r.reconcileRESPAuthSecret(ctx, engine); err != nil {
		log.Error(err, "Failed to reconcile RESP auth secret")
		return ctrl.Result{}, err
	}

	// 6. Workload-aware scheduling: mirror the managed node label onto the
	//    nodes hosting matching workload pods and schedule the release of
	//    nodes the workload has left.
	var requeueAfter time.Duration
	if engine.WorkloadSchedulingEnabled() {
		if !controllerutil.ContainsFinalizer(engine, workloadPlacementFinalizer) {
			controllerutil.AddFinalizer(engine, workloadPlacementFinalizer)
			if err := r.Update(ctx, engine); err != nil {
				return ctrl.Result{}, err
			}
		}
		delay, err := placement.Reconcile(ctx, r.Client, engine, time.Now())
		if err != nil {
			log.Error(err, "Failed to reconcile workload-aware placement labels")
			return ctrl.Result{}, err
		}
		requeueAfter = max(requeueAfter, delay)
	} else if controllerutil.ContainsFinalizer(engine, workloadPlacementFinalizer) {
		// Disabled-after-enabled: one Placement pass runs on the disabled
		// path (strips leftover labels, resets status), then the finalizer
		// is dropped so CR deletion never blocks on it.
		if _, err := placement.Reconcile(ctx, r.Client, engine, time.Now()); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(engine, workloadPlacementFinalizer)
		if err := r.Update(ctx, engine); err != nil {
			return ctrl.Result{}, err
		}
		requeueAfter = 0
	}

	// 7. Reconcile DaemonSet
	if err := r.reconcileDaemonSet(ctx, engine); err != nil {
		log.Error(err, "Failed to reconcile DaemonSet")
		return ctrl.Result{}, err
	}

	// 8. Reconcile lookup Service (node-local discovery for vLLM)
	if err := r.reconcileLookupService(ctx, engine); err != nil {
		log.Error(err, "Failed to reconcile lookup Service")
		return ctrl.Result{}, err
	}

	// 9. Reconcile metrics Service
	if err := r.reconcileMetricsService(ctx, engine); err != nil {
		log.Error(err, "Failed to reconcile metrics Service")
		return ctrl.Result{}, err
	}

	// 10. Reconcile ConfigMap
	if err := r.reconcileConnectionConfigMap(ctx, engine); err != nil {
		log.Error(err, "Failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	// 11. Reconcile ServiceMonitor
	if err := r.reconcileServiceMonitor(ctx, engine); err != nil {
		log.Error(err, "Failed to reconcile ServiceMonitor")
		return ctrl.Result{}, err
	}

	// 12. Update status
	if err := r.updateStatus(ctx, engine); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// enginesForPod maps a pod event to the workload-aware engines whose workload
// selector matches it. Pod events are the trigger for label refreshes: a
// workload arriving on or leaving a node must add or (after the release
// delay) drop that node's engine label. Informer selectors are static at
// manager start, so the pod watch is broad and matching happens here per
// event against each enabled engine's selector.
func (r *LMCacheEngineReconciler) enginesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	engines := &lmcachev1alpha1.LMCacheEngineList{}
	if err := r.List(ctx, engines); err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range engines.Items {
		engine := &engines.Items[i]
		if engine.Namespace != pod.Namespace || !engine.WorkloadSchedulingEnabled() {
			continue
		}
		if !placement.Matches(engine, pod.Labels) {
			continue
		}
		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: engine.Namespace, Name: engine.Name},
		})
	}
	return out
}

// SetupWithManager sets up the controller with the Manager.
func (r *LMCacheEngineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lmcachev1alpha1.LMCacheEngine{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.enginesForPod),
		).
		Named("lmcacheengine").
		Complete(r)
}

// Ensure resources package is used (for linter).
var _ = resources.StandardLabels
