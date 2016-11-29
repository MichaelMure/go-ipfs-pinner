package gc

import (
	"context"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	dag "github.com/ipfs/go-ipfs/merkledag"
	pin "github.com/ipfs/go-ipfs/pin"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	cid "gx/ipfs/QmcTcsTvfaeEBRFo1TkFgT8sRmgi1n1LTZpecfVP8fzpGD/go-cid"
)

var log = logging.Logger("gc")

// GC performs a mark and sweep garbage collection of the blocks in the blockstore
// first, it creates a 'marked' set and adds to it the following:
// - all recursively pinned blocks, plus all of their descendants (recursively)
// - bestEffortRoots, plus all of its descendants (recursively)
// - all directly pinned blocks
// - all blocks utilized internally by the pinner
//
// The routine then iterates over every block in the blockstore and
// deletes any block that is not found in the marked set.
func GC(ctx context.Context, bs bstore.GCBlockstore, ls dag.LinkService, pn pin.Pinner, bestEffortRoots []*cid.Cid) (<-chan *cid.Cid, error) {
	unlocker := bs.GCLock()

	ls = ls.GetOfflineLinkService()

	gcs, err := ColoredSet(ctx, pn, ls, bestEffortRoots)
	if err != nil {
		return nil, err
	}

	keychan, err := bs.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	output := make(chan *cid.Cid)
	go func() {
		defer close(output)
		defer unlocker.Unlock()
		for {
			select {
			case k, ok := <-keychan:
				if !ok {
					return
				}
				if !gcs.Has(k) {
					err := bs.DeleteBlock(k)
					if err != nil {
						log.Debugf("Error removing key from blockstore: %s", err)
						return
					}
					select {
					case output <- k:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return output, nil
}

func Descendants(ctx context.Context, ls dag.LinkService, set *cid.Set, roots []*cid.Cid, bestEffort bool) error {
	for _, c := range roots {
		set.Add(c)

		// EnumerateChildren recursively walks the dag and adds the keys to the given set
		err := dag.EnumerateChildren(ctx, ls, c, set.Visit, bestEffort)
		if err != nil {
			return err
		}
	}

	return nil
}

func ColoredSet(ctx context.Context, pn pin.Pinner, ls dag.LinkService, bestEffortRoots []*cid.Cid) (*cid.Set, error) {
	// KeySet currently implemented in memory, in the future, may be bloom filter or
	// disk backed to conserve memory.
	gcs := cid.NewSet()
	err := Descendants(ctx, ls, gcs, pn.RecursiveKeys(), false)
	if err != nil {
		return nil, err
	}

	err = Descendants(ctx, ls, gcs, bestEffortRoots, true)
	if err != nil {
		return nil, err
	}

	for _, k := range pn.DirectKeys() {
		gcs.Add(k)
	}

	err = Descendants(ctx, ls, gcs, pn.InternalPins(), false)
	if err != nil {
		return nil, err
	}

	return gcs, nil
}
