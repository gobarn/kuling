package kuling

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/boltdb/bolt"
)

// IterStore stores iterators and receives them
type IterStore interface {
	Commit(iter string, offset int64) error
	GetAll(group, topic string) (map[string]int64, error)
}

const (
	itersBucket = "iters"
)

// BoltIterStore stores iters in a bolt DB
type BoltIterStore struct {
	db *bolt.DB
}

// OpenBoltIterStore creates a new bolt db backed iter store
func OpenBoltIterStore(path string, c *Config) *BoltIterStore {
	db, err := bolt.Open(path, c.PermData, nil)
	if err != nil {
		log.Fatal("boltiterstore:", err)
	}

	return &BoltIterStore{
		db: db,
	}
}

// Commit persists an iterator and it's offset
func (bs *BoltIterStore) Commit(iter string, offset int64) error {
	err := bs.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(itersBucket))
		if err != nil {
			return fmt.Errorf("boltiterstore: create iter bucket: %s", err)
		}

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, offset); err != nil {
			return fmt.Errorf("boltiterstore: store offset: %s", err)
		}

		// Persist iterator with iter ID as key (group, topic, shard) which is
		// the unique part of the iterator. Persist the offset as the value
		// connected to it.
		return b.Put([]byte(iter), buf.Bytes())
	})

	if err != nil {
		return fmt.Errorf("boltiterstore: commit: %s", err)
	}

	return nil
}

// GetAll iterators for a group and topic
func (bs *BoltIterStore) GetAll(group, topic string) (map[string]int64, error) {
	iters := make(map[string]int64)
	err := bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(itersBucket))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		// Range Scan Bolt DB bucket with the group and topic, exclude the shard
		// from the key as that is the part we are looking for.
		prefix := []byte(group + "/" + topic)
		for iterID, v := c.Seek(prefix); bytes.HasPrefix(iterID, prefix); iterID, v = c.Next() {
			var offset int64
			if err := binary.Read(bytes.NewReader(v), binary.BigEndian, &offset); err != nil {
				return fmt.Errorf("boltiterstore: iterator %s offset not parseable: %s", iterID, err)
			}

			iters[string(iterID)] = offset
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("boltiterstore: unable to get all iters: %s", err)
	}

	return iters, nil
}
