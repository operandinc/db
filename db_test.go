package db

import (
	"reflect"
	"testing"
)

func BasicKVOps(db KV, t *testing.T) {
	v, err := db.Get("foo", nil)
	if err != ErrKeyNotFound || v != nil {
		t.Fatalf("expecting (nil, %v), got (%v, %v)", ErrKeyNotFound, v, err)
	}
	if err := db.Put("foo", []byte("bar")); err != nil {
		t.Fatalf("expected nil err for put, got %v", err)
	}
	v, err = db.Get("foo", nil)
	if err != nil || !reflect.DeepEqual(v, []byte("bar")) {
		t.Fatalf("expecting (%v, nil), got (%v, %v)", []byte("bar"), v, err)
	}
	if err := db.Delete("foo"); err != nil {
		t.Fatalf("expected nil err for delete, got %v", err)
	}
	v, err = db.Get("foo", nil)
	if err != ErrKeyNotFound || v != nil {
		t.Fatalf("expecting (nil, %v), got (%v, %v)", ErrKeyNotFound, v, err)
	}
}

func TestBasicKV(t *testing.T) {
	db := NewInMemoryKV()
	BasicKVOps(db, t)
}

func TestBasicKVWithinTx(t *testing.T) {
	db := NewInMemoryKV()
	tx := db.TxBegin()
	// The transaction itself should pass all the basic KV tests.
	BasicKVOps(tx, t)
	if err := db.TxCommit(tx); err != nil {
		t.Fatalf("expected nil err for tx commit, got %v", err)
	}
	// At this point, the database should still be empty since every write
	// within the transaction was deleted.
	BasicKVOps(db, t)
}
