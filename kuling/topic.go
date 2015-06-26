package kuling

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

var (
	// ErrUnknownShard returned when the shard requested
	// is unknown to the topic
	ErrUnknownShard = errors.New("topic: Unknown shard")
)

// Topic store data in a topic
type Topic interface {
	// Write data with key and payload
	Append(shard string, key, payload []byte) error
	// Read data from specific shard starting form sequenceID and reading
	// max number of messages
	Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error)
	// Copy data from specific shard starting form sequenceID and reading
	// max number of messages into the io writer
	Copy(shard string, startSequenceID, maxMessages int64, w io.Writer) (int64, error)
}

// FSTopic handles an entire topic with it's segments and indexex
type FSTopic struct {
	// topics directory
	dir string
	// map of shard name to shard
	shards map[string]Shard
	// sharding strategy that determine when a new shard is created
	shardingStrategy ShardingStrategy
}

// OpenFSTopicWithFixedShardingStrategy opens or creates a new file system topic
func OpenFSTopicWithFixedShardingStrategy(dir string, numShards int) (*FSTopic, error) {
	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		// The directory does not exist, lets create it
		err := os.Mkdir(dir, 0600)

		if err != nil {
			return nil, fmt.Errorf("topic: Could not create topic directory %s", dir)
		}
	}

	var shards map[string]Shard
	// // Loop over all directories and loadup the shards
	// // Load segment files, important that we load them in correct order
	// // such that the first segment file is loaded first.
	// err = filepath.Walk(dir, func(shardDir string, f os.FileInfo, err error) error {
	// 	// Shards are in directories, skip files
	// 	if !f.IsDir() {
	// 		return nil
	// 	}
	//
	// 	// Load shard from directory
	// 	shard, err := NewFSShard(f.Name())
	// 	if err != nil {
	// 		log.Printf("topic: Could not load shard %s\n", f.Name())
	// 		return err
	// 	}
	//
	// 	shards[f.Name()] = shard
	//
	// 	return nil
	// })
	// if err != nil {
	// 	return nil, err
	// }

	// Create shard factory method for the sharding strategy
	factory := func(name string) (Shard, error) {
		return NewFSShard(path.Join(dir, name))
	}

	strategy, err := NewFixedShardsShardingStrategy(numShards, factory)
	if err != nil {
		log.Println("topic: Could not create fixed sharding strategy")
		return nil, err
	}

	return &FSTopic{
			dir,
			shards,
			strategy,
		},
		nil
}

// Append key and payload to topic
func (t *FSTopic) Append(shard string, key, payload []byte) error {
	if s, err := t.shardingStrategy.Get(shard); err == nil {
		return s.Append(key, payload)
	}

	return ErrUnknownShard
}

// Read from topic shard from start sequence id and max messages
func (t *FSTopic) Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if s, err := t.shardingStrategy.Get(shard); err == nil {
		return s.Read(startSequenceID, maxMessages)
	}

	return nil, ErrUnknownShard
}

// Copy from topic shard from start sequence id and max messages into io writer
func (t *FSTopic) Copy(shard string, startSequenceID, maxMessages int64, w io.Writer) (int64, error) {
	if s, err := t.shardingStrategy.Get(shard); err == nil {
		return s.Copy(startSequenceID, maxMessages, w)
	}

	return 0, ErrUnknownShard
}
