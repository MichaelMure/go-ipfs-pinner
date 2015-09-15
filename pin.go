// package pin implemnts structures and methods to keep track of
// which objects a user wants to keep stored locally.
package pin

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	ds "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	nsds "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore/namespace"
	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	key "github.com/ipfs/go-ipfs/blocks/key"
	"github.com/ipfs/go-ipfs/blocks/set"
	mdag "github.com/ipfs/go-ipfs/merkledag"
	logging "github.com/ipfs/go-ipfs/vendor/go-log-v1.0.0"
)

var log = logging.Logger("pin")
var recursePinDatastoreKey = ds.NewKey("/local/pins/recursive/keys")
var directPinDatastoreKey = ds.NewKey("/local/pins/direct/keys")
var indirectPinDatastoreKey = ds.NewKey("/local/pins/indirect/keys")

type PinMode int

const (
	Recursive PinMode = iota
	Direct
	Indirect
	NotPinned
)

type Pinner interface {
	IsPinned(key.Key) bool
	Pin(context.Context, *mdag.Node, bool) error
	Unpin(context.Context, key.Key, bool) error
	Flush() error
	GetManual() ManualPinner
	DirectKeys() []key.Key
	IndirectKeys() map[key.Key]int
	RecursiveKeys() []key.Key
}

// ManualPinner is for manually editing the pin structure
// Use with care! If used improperly, garbage collection
// may not be successful
type ManualPinner interface {
	PinWithMode(key.Key, PinMode)
	RemovePinWithMode(key.Key, PinMode)
	Pinner
}

// pinner implements the Pinner interface
type pinner struct {
	lock       sync.RWMutex
	recursePin set.BlockSet
	directPin  set.BlockSet
	indirPin   *indirectPin
	dserv      mdag.DAGService
	dstore     ds.ThreadSafeDatastore
}

// NewPinner creates a new pinner using the given datastore as a backend
func NewPinner(dstore ds.ThreadSafeDatastore, serv mdag.DAGService) Pinner {

	// Load set from given datastore...
	rcds := nsds.Wrap(dstore, recursePinDatastoreKey)
	rcset := set.NewDBWrapperSet(rcds, set.NewSimpleBlockSet())

	dirds := nsds.Wrap(dstore, directPinDatastoreKey)
	dirset := set.NewDBWrapperSet(dirds, set.NewSimpleBlockSet())

	nsdstore := nsds.Wrap(dstore, indirectPinDatastoreKey)
	return &pinner{
		recursePin: rcset,
		directPin:  dirset,
		indirPin:   NewIndirectPin(nsdstore),
		dserv:      serv,
		dstore:     dstore,
	}
}

// Pin the given node, optionally recursive
func (p *pinner) Pin(ctx context.Context, node *mdag.Node, recurse bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	k, err := node.Key()
	if err != nil {
		return err
	}

	if recurse {
		if p.recursePin.HasKey(k) {
			return nil
		}

		if p.directPin.HasKey(k) {
			p.directPin.RemoveBlock(k)
		}

		err := p.pinLinks(ctx, node)
		if err != nil {
			return err
		}

		p.recursePin.AddBlock(k)
	} else {
		if _, err := p.dserv.Get(ctx, k); err != nil {
			return err
		}

		if p.recursePin.HasKey(k) {
			return fmt.Errorf("%s already pinned recursively", k.B58String())
		}

		p.directPin.AddBlock(k)
	}
	return nil
}

// Unpin a given key
func (p *pinner) Unpin(ctx context.Context, k key.Key, recursive bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.recursePin.HasKey(k) {
		if recursive {
			p.recursePin.RemoveBlock(k)
			node, err := p.dserv.Get(ctx, k)
			if err != nil {
				return err
			}

			return p.unpinLinks(ctx, node)
		} else {
			return fmt.Errorf("%s is pinned recursively", k)
		}
	} else if p.directPin.HasKey(k) {
		p.directPin.RemoveBlock(k)
		return nil
	} else if p.indirPin.HasKey(k) {
		return fmt.Errorf("%s is pinned indirectly. indirect pins cannot be removed directly", k)
	} else {
		return fmt.Errorf("%s is not pinned", k)
	}
}

func (p *pinner) unpinLinks(ctx context.Context, node *mdag.Node) error {
	for _, l := range node.Links {
		node, err := l.GetNode(ctx, p.dserv)
		if err != nil {
			return err
		}

		k, err := node.Key()
		if err != nil {
			return err
		}

		p.indirPin.Decrement(k)

		err = p.unpinLinks(ctx, node)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *pinner) pinIndirectRecurse(ctx context.Context, node *mdag.Node) error {
	k, err := node.Key()
	if err != nil {
		return err
	}

	p.indirPin.Increment(k)
	return p.pinLinks(ctx, node)
}

func (p *pinner) pinLinks(ctx context.Context, node *mdag.Node) error {
	for _, ng := range p.dserv.GetDAG(ctx, node) {
		subnode, err := ng.Get(ctx)
		if err != nil {
			// TODO: Maybe just log and continue?
			return err
		}
		err = p.pinIndirectRecurse(ctx, subnode)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsPinned returns whether or not the given key is pinned
func (p *pinner) IsPinned(key key.Key) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.recursePin.HasKey(key) ||
		p.directPin.HasKey(key) ||
		p.indirPin.HasKey(key)
}

func (p *pinner) RemovePinWithMode(key key.Key, mode PinMode) {
	p.lock.Lock()
	defer p.lock.Unlock()
	switch mode {
	case Direct:
		p.directPin.RemoveBlock(key)
	case Indirect:
		p.indirPin.Decrement(key)
	case Recursive:
		p.recursePin.RemoveBlock(key)
	default:
		// programmer error, panic OK
		panic("unrecognized pin type")
	}
}

// LoadPinner loads a pinner and its keysets from the given datastore
func LoadPinner(d ds.ThreadSafeDatastore, dserv mdag.DAGService) (Pinner, error) {
	p := new(pinner)

	{ // load recursive set
		var recurseKeys []key.Key
		if err := loadSet(d, recursePinDatastoreKey, &recurseKeys); err != nil {
			return nil, err
		}
		p.recursePin = set.SimpleSetFromKeys(recurseKeys)
	}

	{ // load direct set
		var directKeys []key.Key
		if err := loadSet(d, directPinDatastoreKey, &directKeys); err != nil {
			return nil, err
		}
		p.directPin = set.SimpleSetFromKeys(directKeys)
	}

	{ // load indirect set
		var err error
		p.indirPin, err = loadIndirPin(d, indirectPinDatastoreKey)
		if err != nil {
			return nil, err
		}
	}

	// assign services
	p.dserv = dserv
	p.dstore = d

	return p, nil
}

// DirectKeys returns a slice containing the directly pinned keys
func (p *pinner) DirectKeys() []key.Key {
	return p.directPin.GetKeys()
}

// IndirectKeys returns a slice containing the indirectly pinned keys
func (p *pinner) IndirectKeys() map[key.Key]int {
	return p.indirPin.GetRefs()
}

// RecursiveKeys returns a slice containing the recursively pinned keys
func (p *pinner) RecursiveKeys() []key.Key {
	return p.recursePin.GetKeys()
}

// Flush encodes and writes pinner keysets to the datastore
func (p *pinner) Flush() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	err := storeSet(p.dstore, directPinDatastoreKey, p.directPin.GetKeys())
	if err != nil {
		return err
	}

	err = storeSet(p.dstore, recursePinDatastoreKey, p.recursePin.GetKeys())
	if err != nil {
		return err
	}

	err = storeIndirPin(p.dstore, indirectPinDatastoreKey, p.indirPin)
	if err != nil {
		return err
	}
	return nil
}

// helpers to marshal / unmarshal a pin set
func storeSet(d ds.Datastore, k ds.Key, val interface{}) error {
	buf, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return d.Put(k, buf)
}

func loadSet(d ds.Datastore, k ds.Key, val interface{}) error {
	buf, err := d.Get(k)
	if err != nil {
		return err
	}

	bf, ok := buf.([]byte)
	if !ok {
		return errors.New("invalid pin set value in datastore")
	}
	return json.Unmarshal(bf, val)
}

// PinWithMode is a method on ManualPinners, allowing the user to have fine
// grained control over pin counts
func (p *pinner) PinWithMode(k key.Key, mode PinMode) {
	p.lock.Lock()
	defer p.lock.Unlock()
	switch mode {
	case Recursive:
		p.recursePin.AddBlock(k)
	case Direct:
		p.directPin.AddBlock(k)
	case Indirect:
		p.indirPin.Increment(k)
	}
}

func (p *pinner) GetManual() ManualPinner {
	return p
}
