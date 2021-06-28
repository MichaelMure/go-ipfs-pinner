package dspinner

import (
	"context"
	"testing"

	bs "github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	bds "github.com/ipfs/go-ds-badger"
	lds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
)

func makeStoreLevelDB(dir string) (ds.Datastore, ipld.DAGService) {
	ldstore, err := lds.NewDatastore(dir, nil)
	if err != nil {
		panic(err)
	}
	dstore := &batchWrap{ldstore} //XXX: What is the significance of this wrapper?
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

func makeStoreBadger(dir string) (ds.Datastore, ipld.DAGService) {
	bdstore, err := bds.NewDatastore(dir, nil)
	if err != nil {
		panic(err)
	}
	dstore := &batchWrap{bdstore}
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

func benchSyncEvery(b *testing.B, N int, every bool, dstore ds.Datastore, dserv ipld.DAGService) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pinner, err := New(ctx, dstore, dserv)
	if err != nil {
		panic(err.Error())
	}

	nodes := makeNodes(N, dserv)
	pinNodesSyncEvery(nodes, pinner, true, every)
}

func BenchmarkSyncOnceBadger(b *testing.B) {
	dstoreB1, dservB1 := makeStoreBadger("b-once")
	benchSyncEvery(b, b.N, false, dstoreB1, dservB1)
	dstoreB1.Close()
}

func BenchmarkSyncEveryBadger(b *testing.B) {
	dstoreB2, dservB2 := makeStoreBadger("b-every")
	benchSyncEvery(b, b.N, true, dstoreB2, dservB2)
	dstoreB2.Close()
}

func BenchmarkSyncOnceLevelDB(b *testing.B) {
	dstoreL1, dservL1 := makeStoreLevelDB("l-once")
	benchSyncEvery(b, b.N, false, dstoreL1, dservL1)
	dstoreL1.Close()
}

func BenchmarkSyncEveryLevelDB(b *testing.B) {
	dstoreL2, dservL2 := makeStoreLevelDB("l-every")
	benchSyncEvery(b, b.N, true, dstoreL2, dservL2)
	dstoreL2.Close()
}
