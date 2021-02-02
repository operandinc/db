package db

import (
	"errors"
	"sync"
	"time"
)

type KV interface {
	Get(k string, ts *int64) ([]byte, error)
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
	Parent    KV
	Mutex     sync.RWMutex
	Store     map[string][]byte
	Writes    map[string][]TSVal
	Deletes   map[string][]int64
	Timestamp int64
}

func (db *DB) Now() int64 {
	return time.Now().UnixNano()
}

var ErrKeyNotFound = errors.New("db: key not found")

func (db *DB) Get(k string, ts *int64) ([]byte, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()
	// Base case - we are the root object.
	if db.Parent == nil {
		v, ok := db.Store[k]
		if !ok {
			return nil, ErrKeyNotFound
		}
		return v, nil
	}
	// We are within a transaction.
	var (
		v   []byte // Value of the key from this transaction.
		fts int64  // Timestamp of latest write (from current view).
	)
	// Get the latest write up until ts of this transaction.
	if tsvs, ok := db.Writes[k]; ok && len(tsvs) > 0 {
		// FIXME: Could iterate backward to speed this up.
		for _, tsv := range tsvs {
			if ts == nil || tsv.Ts >= *ts {
				v = tsv.Value
				fts = tsv.Ts
			}
		}
	}
	// Make sure the key/value pair hasn't been deleted within this
	// transaction. If it was deleted at a later timestamp than the
	// latest write and before ts, then we can safely return.
	if dts, ok := db.Deletes[k]; ok && len(dts) > 0 {
		for _, dt := range dts {
			if dt > fts {
				if ts == nil || dt <= *ts {
					return nil, ErrKeyNotFound
				}
				break
			}
		}
	}
	// If we have written to this key during this transaction, then this
	// is the most up to date version and thus we must return it.
	if v != nil {
		return v, nil
	}
	// If key wasn't mutated inside this transaction, must fetch from parent.
	return db.Parent.Get(k, &db.Timestamp)
}

func (db *DB) Put(k string, v []byte) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	// If we are in the root object, then we just need to update the base.
	if db.Parent == nil {
		db.Store[k] = v
		return nil
	}
	// We need to update our transaction state to include the new update
	// for the key/value pair. This mutation gets appended to any existing
	// mutations already executed within this transaction.
	db.Writes[k] = append(db.Writes[k], TSVal{Ts: db.Now(), Value: v})
	return nil
}

func (db *DB) Delete(k string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	// If we are in the root object, then we just need to update the base.
	if db.Parent == nil {
		delete(db.Store, k)
		return nil
	}
	// We need to update our transaction state to include the new deletion.
	db.Deletes[k] = append(db.Deletes[k], db.Now())
	return nil
}

func (db *DB) TxBegin() KV {
	return &DB{
		Parent:    db,
		Writes:    make(map[string][]TSVal),
		Deletes:   make(map[string][]int64),
		Timestamp: db.Now(),
	}
}

var (
	ErrInvalidTxType = errors.New("db: invalid tx type")
	ErrInvalidParent = errors.New("db: invalid parent tx")
	ErrConflict      = errors.New("db: conflicting timestamps")
)

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

	// Construct a hashset of all the keys that were mutated.
	keys := make(map[string]struct{})
	for k := range txdb.Writes {
		keys[k] = struct{}{}
	}
	for k := range txdb.Deletes {
		keys[k] = struct{}{}
	}

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
		var (
			v       []byte // The value that the key was mutated to, can be nil.
			writets int64  // The timestamp of latest write for key.
			delts   int64  // The timestamp of latest delete for key.
		)
		if tsv, ok := txdb.Writes[k]; ok {
			l := len(tsv)
			if l > 0 {
				v = tsv[l-1].Value
				writets = tsv[l-1].Ts
			}
		}
		if dts, ok := txdb.Deletes[k]; ok {
			l := len(dts)
			if l > 0 {
				delts = dts[l-1]
			}
		}
		if delts > writets {
			// This is a delete.
			if db.Parent == nil {
				delete(db.Store, k)
			} else {
				db.Deletes[k] = append(db.Deletes[k], ts)
			}
		} else if delts < writets {
			// This is a write.
			if db.Parent == nil {
				db.Store[k] = v
			} else {
				db.Writes[k] = append(db.Writes[k], TSVal{Ts: ts, Value: v})
			}
		} else {
			// delts = writets meaning the key was written to and deleted
			// at the same time, we cannot resolve this thus should return
			// an error to the caller.
			return ErrConflict
		}
	}
	return nil
}

func NewInMemoryKV() KV {
	return &DB{
		Store: make(map[string][]byte),
	}
}
