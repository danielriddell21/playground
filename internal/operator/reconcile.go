package operator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const finalizer = "links.playground.dev/link"

// Resync is how often a ready link is checked again, which is what puts it
// back if the backend loses it.
var Resync = 30 * time.Second

type Reconciler struct {
	client.Client
	Backend Backend
	// BaseURL prefixes the slug in status.url.
	BaseURL string
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sl ShortLink
	if err := r.Get(ctx, req.NamespacedName, &sl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !sl.DeletionTimestamp.IsZero() {
		if sl.Status.Slug != "" {
			if err := r.Backend.Delete(sl.Status.Slug); err != nil && !errors.Is(err, ErrNotFound) {
				return ctrl.Result{}, err
			}
		}
		controllerutil.RemoveFinalizer(&sl, finalizer)
		return ctrl.Result{}, r.Update(ctx, &sl)
	}
	if controllerutil.AddFinalizer(&sl, finalizer) {
		if err := r.Update(ctx, &sl); err != nil {
			return ctrl.Result{}, err
		}
	}

	slug, msg, err := r.ensure(&sl)
	before := sl.Status
	if err != nil {
		sl.Status.Ready, sl.Status.Message = false, err.Error()
	} else {
		sl.Status = ShortLinkStatus{Slug: slug, URL: r.BaseURL + "/" + slug, Ready: true, Message: msg}
	}
	sl.Status.ObservedGeneration = sl.Generation
	if sl.Status != before {
		if uerr := r.Status().Update(ctx, &sl); uerr != nil {
			return ctrl.Result{}, uerr
		}
	}
	if errors.Is(err, ErrInvalid) || errors.Is(err, ErrTaken) {
		return ctrl.Result{}, nil // the spec has to change, retrying won't help
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: Resync}, nil
}

// ensure makes the backend match the spec and returns the live slug.
func (r *Reconciler) ensure(sl *ShortLink) (string, string, error) {
	want := sl.Spec.Slug
	if want == "" {
		want = sl.Status.Slug // keep a generated slug stable
	}
	owner := string(sl.UID)
	if owner == "" {
		owner = sl.Namespace + "/" + sl.Name
	}
	if want != "" {
		target, by, err := r.Backend.Get(want)
		switch {
		case err == nil && by != owner:
			return "", "", fmt.Errorf("%w by %s", ErrTaken, by)
		case err == nil && target == sl.Spec.Target:
			return want, "in sync", nil
		case err == nil:
			// the backend has no update, so replace it
			if err := r.Backend.Delete(want); err != nil {
				return "", "", err
			}
		case !errors.Is(err, ErrNotFound):
			return "", "", err
		}
	}
	// a slug change in the spec leaves the old one behind
	if old := sl.Status.Slug; old != "" && old != want {
		r.Backend.Delete(old)
	}
	slug, err := r.Backend.Create(want, sl.Spec.Target, owner)
	if err != nil {
		return "", "", err
	}
	return slug, fmt.Sprintf("created at generation %d", sl.Generation), nil
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&ShortLink{}).Complete(r)
}

func NewScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}
