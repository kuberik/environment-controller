package controller

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	releasev1alpha1 "github.com/kuberik/release-controller/api/v1alpha1"
	kuberikrolloutv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// publishCatalogue writes one ReleaseSlice for source holding rels, and returns
// the lean status.candidates / status.releaseSource a Rollout would carry for
// it. The caller assigns them to the Rollout status.
func publishCatalogue(ctx context.Context, c client.Client, ns, source string, rels []kuberikrolloutv1alpha1.VersionInfo) ([]kuberikrolloutv1alpha1.ReleaseCandidate, *kuberikrolloutv1alpha1.ReleaseSourceStatusRef, error) {
	now := metav1.Now()
	slice := &releasev1alpha1.ReleaseSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: source + "-00000", Namespace: ns,
			Labels: map[string]string{releasev1alpha1.ReleaseSourceLabel: source},
		},
		Source: source,
	}
	cands := make([]kuberikrolloutv1alpha1.ReleaseCandidate, 0, len(rels))
	for i, rel := range rels {
		slice.Releases = append(slice.Releases, releasev1alpha1.Release{
			Tag: rel.Tag, Revision: rel.Revision, Sequence: int64(i), Created: now, FirstSeen: now,
		})
		cands = append(cands, kuberikrolloutv1alpha1.ReleaseCandidate{Tag: rel.Tag, Gated: true})
	}
	existing := &releasev1alpha1.ReleaseSlice{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(slice), existing); err == nil {
		existing.Releases = slice.Releases
		if err := c.Update(ctx, existing); err != nil {
			return nil, nil, err
		}
	} else if err := c.Create(ctx, slice); err != nil {
		return nil, nil, err
	}
	return cands, &kuberikrolloutv1alpha1.ReleaseSourceStatusRef{Name: source, Ready: true, Count: int32(len(rels))}, nil
}
