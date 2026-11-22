package operator

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Hand written instead of kubebuilder/controller-gen so the whole operator
// is readable in one package. crd.yaml is kept in sync by hand too.

var GroupVersion = schema.GroupVersion{Group: "links.playground.dev", Version: "v1alpha1"}

type ShortLinkSpec struct {
	Target string `json:"target"`
	// Slug is optional; empty lets the shortener pick one.
	Slug string `json:"slug,omitempty"`
}

type ShortLinkStatus struct {
	Slug               string `json:"slug,omitempty"`
	URL                string `json:"url,omitempty"`
	Ready              bool   `json:"ready"`
	Message            string `json:"message,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

type ShortLink struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShortLinkSpec   `json:"spec"`
	Status ShortLinkStatus `json:"status,omitempty"`
}

type ShortLinkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShortLink `json:"items"`
}

func (in *ShortLink) DeepCopyInto(out *ShortLink) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
}

func (in *ShortLink) DeepCopy() *ShortLink {
	if in == nil {
		return nil
	}
	out := new(ShortLink)
	in.DeepCopyInto(out)
	return out
}

func (in *ShortLink) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *ShortLinkList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ShortLinkList)
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]ShortLink, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
	return out
}

func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &ShortLink{}, &ShortLinkList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
