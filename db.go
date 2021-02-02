package db

import (
	"errors"
	"sync"
	"time"
)

type KV interface {
	Get(k string) ([]byte, error)
	Put(k string, v []byte) error
	Delete(k string) error
	TxBegin() KV
	TxCommit(kv KV) error
}

type TSVal struct {
	Ts    int64
	Value []byte
}

type DB struct {
	Parent    *DB
	Mutex     sync.RWMutex
	Store     map[string][]byte
	Writes    map[string][]TSVal
	Deletes   map[string][]int64
	Timestamp int64
	ActiveTx  uint
	Closed    bool
}

func (db *DB) Now() int64 {
	return time.Now().UnixNano()
}

var (
	ErrKeyNotFound = errors.New("db: key not found")
	ErrTxClosed    = errors.New("db: tx closed")
)

// Returns the latest write from db.Writes up until ts. If ts == nil, then the
// latest write is returned. If there are no writes, then (nil, 0) is returned.
// Requires lock to be held.
func (db *DB) GetLatestWriteUntilTS(k string, ts *int64) (v []byte, wts int64) {
	if wrs, ok := db.Writes[k]; ok {
		l := len(wrs)
		if l == 0 {
			return
		}
		if ts == nil {
			// Fast path, ts == nil.
			v = wrs[l-1].Value
			wts = wrs[l-1].Ts
		} else {
			// Return the last write before ts.
			for i := 0; i < len(wrs); i++ {
				if i != len(wrs)-1 && wrs[i+1].Ts < *ts {
					continue
				}
				if wrs[i].Ts < *ts {
					v = wrs[i].Value
					wts = wrs[i].Ts
					break
				}
			}
		}
	}
	return
}

// Returns the latest delete from db.Deletes up until ts. If ts == nil, then the
// latest delete is returned. If there are no deletes, then nil is returned.
// Requires lock to be held.
func (db *DB) GetLatestDeleteUntilTS(k string, ts *int64) (dts *int64) {
	if dels, ok := db.Deletes[k]; ok {
		l := len(dels)
		if l == 0 {
			return
		}
		if ts == nil {
			// Fast path, ts == nil.
			dts = &dels[l-1]
		} else {
			// Return the last delete before ts.
			for i := 0; i < len(dels); i++ {
				if i != len(dels)-1 && dels[i+1] < *ts {
					continue
				}
				if dels[i] < *ts {
					dts = &dels[i]
					break
				}
			}
		}
	}
	return
}

func (db *DB) InternalGet(k string, ts *int64) ([]byte, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	// We are within a transaction.
	if db.Closed {
		return nil, ErrTxClosed
	}

	// Get the latest write up until `ts` of this transaction.
	v, fts := db.GetLatestWriteUntilTS(k, ts)

	// Make sure the key/value pair hasn't been deleted within this
	// transaction. If it was deleted at a later timestamp than the
	// latest write and before ts, then we can safely return.
	if dts := db.GetLatestDeleteUntilTS(k, ts); dts != nil {
		if *dts > fts {
			return nil, ErrKeyNotFound
		}
	}

	// If we have written to this key during this transaction, then this
	// is the most up to date version and thus we must return it.
	if v != nil {
		return v, nil
	}

	// If we are in the parent object, then we try to fetch the key from the
	// store as a last resort. Otherwise, we must fetch the key from the next
	// level up using a recursive call. Note that we fetch the key at the
	// timestamp of the start of this transaction, so writes after the
	// transaction won't be counted.
	if db.Parent == nil {
		v, ok := db.Store[k]
		if !ok {
			return nil, ErrKeyNotFound
		}
		return v, nil
	}
	return db.Parent.InternalGet(k, &db.Timestamp)
}

func (db *DB) Get(k string) ([]byte, error) {
	return db.InternalGet(k, nil)
}

// Requires lock to be held.
func (db *DB) InternalPut(k string, v []byte, ts *int64) error {
	if db.Closed {
		return ErrTxClosed
	}
	if ts == nil {
		now := db.Now()
		ts = &now
	}
	// If this is the root and there are no active transactions, then
	// we don't need to use the db.Writes array and we can just go
	// straight to the underlying storage mechanism.
	if db.Parent == nil && db.ActiveTx == 0 {
		db.Store[k] = v
		return nil
	}
	// Record the new insertion at the timestamp.
	db.Writes[k] = append(db.Writes[k], TSVal{Ts: *ts, Value: v})
	return nil
}

func (db *DB) Put(k string, v []byte) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	return db.InternalPut(k, v, nil)
}

// Requires lock to be held.
func (db *DB) InternalDelete(k string, ts *int64) error {
	if db.Closed {
		return ErrTxClosed
	}
	if ts == nil {
		now := db.Now()
		ts = &now
	}
	// If this is the root and there are no active transactions, then
	// we don't need to use the db.Deletes array and we can just go
	// straight to the underlying storage mechanism.
	if db.Parent == nil && db.ActiveTx == 0 {
		delete(db.Store, k)
		return nil
	}
	// Record the new deletion at timestamp.
	db.Deletes[k] = append(db.Deletes[k], *ts)
	return nil
}

func (db *DB) Delete(k string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	return db.InternalDelete(k, nil)
}

func (db *DB) TxBegin() KV {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	db.ActiveTx++
	return &DB{
		Parent:    db,
		Writes:    make(map[string][]TSVal),
		Deletes:   make(map[string][]int64),
		Timestamp: db.Now(),
		Closed:    false,
	}
}

var (
	ErrInvalidTxType = errors.New("db: invalid tx type")
	ErrInvalidParent = errors.New("db: invalid parent tx")
	ErrConflict      = errors.New("db: conflicting timestamps")
)

// Creates a hashset of all the keys activally mutates as part of
// db.Writes & db.Deletes. Requires an RLock to be held by the caller.
func (db *DB) MutatedKeys() map[string]struct{} {
	keys := make(map[string]struct{})
	for k := range db.Writes {
		keys[k] = struct{}{}
	}
	for k := range db.Deletes {
		keys[k] = struct{}{}
	}
	return keys
}

// Fetches the last mutated state for a key. Requires that the k was mutated
// and thus is a key in db.Writes or db.Deletes (or both). Returns the key value
// and the timestamps of both the last write and last delete. Requires an RLock
// to be held by the caller.
func (db *DB) LatestMutatedState(k string) (v []byte, wts int64, dts int64) {
	v, wts = db.GetLatestWriteUntilTS(k, nil)
	if delts := db.GetLatestDeleteUntilTS(k, nil); delts != nil {
		dts = *delts
	}
	return
}

// ClearMutations clears all the mutations from db.Writes and db.Deletes.
// Requires the lock to be held by the caller.
func (db *DB) ClearMutations() {
	for k := range db.Writes {
		delete(db.Writes, k)
	}
	for k := range db.Deletes {
		delete(db.Deletes, k)
	}
}

// Flushes the latest mutations from db.Writes & db.Deletes into db.Store.
// This operation can only be performed as the parent. Requires lock to be held.
func (db *DB) Flush() error {
	// Flush only makes sense as a parent.
	if db.Parent != nil {
		return nil
	}
	// Fetch all the mutated keys.
	keys := db.MutatedKeys()
	// For each key, flush it to storage.
	for k := range keys {
		v, wts, dts := db.LatestMutatedState(k)
		if dts > wts {
			delete(db.Store, k)
		} else if dts < wts {
			db.Store[k] = v
		} else {
			return ErrConflict
		}
	}
	db.ClearMutations()
	return nil
}

func (db *DB) TxCommit(kv KV) error {
	// Make sure that the type of the transaction matches ourselves.
	txdb, ok := kv.(*DB)
	if !ok {
		return ErrInvalidTxType
	}

	// This shouldn't be required, but for safety, we also acquire a lock
	// on the transaction being committed to make sure that no one else is going
	// to write to it while we are committing it.
	txdb.Mutex.Lock()
	defer txdb.Mutex.Unlock()

	// Make sure the transaction is a child of ours.
	if txdb.Parent != db {
		return ErrInvalidParent
	}

	// Make sure the transaction hasn't been previously committed.
	if txdb.Closed {
		return ErrTxClosed
	}

	// Construct a hashset of all the keys that were mutated.
	keys := txdb.MutatedKeys()

	// At the point of committing a transaction, we essentially fast-forward all
	// the writes to the current point in time, call it `ts`. This means that
	// any mutations within the transaction being committed will appear to have
	// been executed at ts to the current transaction and/or db state.
	ts := db.Now()

	// Since we are now actually writing the data, need to acquire the lock.
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	// For each key that has been mutated, do the required comparisons and then
	// commit that mutation to our store. Note that this is dependent on whether
	// or not this object is itself a transaction or if it's the root.
	for k := range keys {
		v, wts, dts := txdb.LatestMutatedState(k)
		if dts > wts {
			// This is a delete.
			db.InternalDelete(k, &ts)
		} else if dts < wts {
			// This is a write.
			db.InternalPut(k, v, &ts)
		} else {
			return ErrConflict
		}
	}

	// Now that the transaction has been committed, we mark it as closed
	// and thus it cannot be used for any subsequent operations. This ensures
	// that our ActiveTx count remains accurate (since it would be messed up
	// if a single transaction was committed twice).
	txdb.Closed = true
	if db.Parent == nil {
		db.ActiveTx--
		// If at this point there are no active transactions, take this
		// opportunity to flush all the writes to storage. At the end of this
		// operation, the db.Store will contain all the latest writes and the
		// db.Writes & db.Deletes will be empty. Remember, this is only for the
		// root node. Transaction nodes don't have a db.Store.
		if db.ActiveTx == 0 {
			db.Flush()
		}
	}

	return nil
}

func NewInMemoryKV() KV {
	return &DB{
		Store:    make(map[string][]byte),
		Writes:   make(map[string][]TSVal),
		Deletes:  make(map[string][]int64),
		ActiveTx: 0,
	}
}
