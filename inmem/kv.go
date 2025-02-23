package inmem

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/influxdata/influxdb/kv"
)

// KVStore is an in memory btree backed kv.Store.
type KVStore struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
	ro      map[string]*bucket
}

// NewKVStore creates an instance of a KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		buckets: map[string]*Bucket{},
		ro:      map[string]*bucket{},
	}
}

// View opens up a transaction with a read lock.
func (s *KVStore) View(ctx context.Context, fn func(kv.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fn(&Tx{
		kv:       s,
		writable: false,
		ctx:      ctx,
	})
}

// Update opens up a transaction with a write lock.
func (s *KVStore) Update(ctx context.Context, fn func(kv.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return fn(&Tx{
		kv:       s,
		writable: true,
		ctx:      ctx,
	})
}

// Flush removes all data from the buckets.  Used for testing.
func (s *KVStore) Flush(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.buckets {
		b.btree.Clear(false)
	}
}

// Buckets returns the names of all buckets within inmem.KVStore.
func (s *KVStore) Buckets(ctx context.Context) [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buckets := make([][]byte, 0, len(s.buckets))
	for b := range s.buckets {
		buckets = append(buckets, []byte(b))
	}
	return buckets
}

// Tx is an in memory transaction.
// TODO: make transactions actually transactional
type Tx struct {
	kv       *KVStore
	writable bool
	ctx      context.Context
}

// Context returns the context for the transaction.
func (t *Tx) Context() context.Context {
	return t.ctx
}

// WithContext sets the context for the transaction.
func (t *Tx) WithContext(ctx context.Context) {
	t.ctx = ctx
}

// createBucketIfNotExists creates a btree bucket at the provided key.
func (t *Tx) createBucketIfNotExists(b []byte) (kv.Bucket, error) {
	if t.writable {
		bkt, ok := t.kv.buckets[string(b)]
		if !ok {
			bkt = &Bucket{btree.New(2)}
			t.kv.buckets[string(b)] = bkt
			t.kv.ro[string(b)] = &bucket{Bucket: bkt}
			return bkt, nil
		}

		return bkt, nil
	}

	return nil, kv.ErrTxNotWritable
}

// Bucket retrieves the bucket at the provided key.
func (t *Tx) Bucket(b []byte) (kv.Bucket, error) {
	bkt, ok := t.kv.buckets[string(b)]
	if !ok {
		return t.createBucketIfNotExists(b)
	}

	if t.writable {
		return bkt, nil
	}

	return t.kv.ro[string(b)], nil
}

// Bucket is a btree that implements kv.Bucket.
type Bucket struct {
	btree *btree.BTree
}

type bucket struct {
	kv.Bucket
}

// Put wraps the put method of a kv bucket and ensures that the
// bucket is writable.
func (b *bucket) Put(_, _ []byte) error {
	return kv.ErrTxNotWritable
}

// Delete wraps the delete method of a kv bucket and ensures that the
// bucket is writable.
func (b *bucket) Delete(_ []byte) error {
	return kv.ErrTxNotWritable
}

type item struct {
	key   []byte
	value []byte
}

// Less is used to implement btree.Item.
func (i *item) Less(b btree.Item) bool {
	j, ok := b.(*item)
	if !ok {
		return false
	}

	return bytes.Compare(i.key, j.key) < 0
}

// Get retrieves the value at the provided key.
func (b *Bucket) Get(key []byte) ([]byte, error) {
	i := b.btree.Get(&item{key: key})

	if i == nil {
		return nil, kv.ErrKeyNotFound
	}

	j, ok := i.(*item)
	if !ok {
		return nil, fmt.Errorf("error item is type %T not *item", i)
	}

	return j.value, nil
}

// Put sets the key value pair provided.
func (b *Bucket) Put(key []byte, value []byte) error {
	_ = b.btree.ReplaceOrInsert(&item{key: key, value: value})
	return nil
}

// Delete removes the key provided.
func (b *Bucket) Delete(key []byte) error {
	_ = b.btree.Delete(&item{key: key})
	return nil
}

// Cursor creates a static cursor from all entries in the database.
func (b *Bucket) Cursor(opts ...kv.CursorHint) (kv.Cursor, error) {
	var o kv.CursorHints
	for _, opt := range opts {
		opt(&o)
	}

	// TODO we should do this by using the Ascend/Descend methods that
	//  the btree provides.
	pairs, err := b.getAll(&o)
	if err != nil {
		return nil, err
	}

	return kv.NewStaticCursor(pairs), nil
}

func (b *Bucket) getAll(o *kv.CursorHints) ([]kv.Pair, error) {
	fn := o.PredicateFn

	var pairs []kv.Pair
	var err error
	b.btree.Ascend(func(i btree.Item) bool {
		j, ok := i.(*item)
		if !ok {
			err = fmt.Errorf("error item is type %T not *item", i)
			return false
		}

		if fn == nil || fn(j.key, j.value) {
			pairs = append(pairs, kv.Pair{Key: j.key, Value: j.value})
		}

		return true
	})

	if err != nil {
		return nil, err
	}

	return pairs, nil
}
