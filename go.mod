module github.com/ipfs/go-ipfs-pinner

go 1.13

require (
	github.com/gogo/protobuf v1.3.1
	github.com/ipfs/go-blockservice v0.1.2
	github.com/ipfs/go-cid v0.0.3
	github.com/ipfs/go-datastore v0.1.1
	github.com/ipfs/go-ipfs-blockstore v0.1.0
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-util v0.0.1
	github.com/ipfs/go-ipld-format v0.0.2
	github.com/ipfs/go-log v0.0.1
	github.com/ipfs/go-merkledag v0.2.4
)

replace github.com/ipfs/go-merkledag => github.com/MichaelMure/go-merkledag v0.2.1-0.20191119160700-c20b9a52f504
