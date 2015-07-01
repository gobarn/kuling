package kuling

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

var ()

// Topic store data in a topic
type Topic interface {
	// Write data with key and payload
	Append(shard string, key, payload []byte) error
	// Read data from specific shard starting form sequenceID and reading
	// max number of messages
	Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error)
	// Copy data from specific shard starting form sequenceID and reading
	// max number of messages into the io writer
	Copy(shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error)
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
func OpenFSTopicWithFixedShardingStrategy(dir string, numShards int, config *FSConfig) (*FSTopic, error) {
	if config.PermDirectories < 0700 {
		panic("topic: Directories must have execute right for running user")
	}
	if config.PermData < 0600 {
		panic("topic: Files must have read and write permissions for running user")
	}

	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		log.Printf("topic: Creating topic directory %s", dir)
		// The directory does not exist, lets create it
		err := os.Mkdir(dir, config.PermDirectories)

		if err != nil {
			return nil, fmt.Errorf("topic: Could not create topic directory %s", dir)
		}
	}

	shards := make(map[string]Shard)

	// Create shard factory method for the sharding strategy
	factory := func(name string) (Shard, error) {
		return OpenFSShard(path.Join(dir, name), config.SegmentMaxBytes, config.PermDirectories, config.PermData)
	}

	strategy, err := NewFixedShardsShardingStrategy(numShards, factory)
	if err != nil {
		log.Println("topic: Could not create fixed sharding strategy")
		return nil, err
	}

	// Create struct and return in
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

	return fmt.Errorf("topic: Unknown shard %s", shard)
}

// Read from topic shard from start sequence id and max messages
func (t *FSTopic) Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if s, err := t.shardingStrategy.Get(shard); err == nil {
		return s.Read(startSequenceID, maxMessages)
	}

	return nil, fmt.Errorf("topic: Unknown shard %s", shard)
}

// Copy from topic shard from start sequence id and max messages into io writer
func (t *FSTopic) Copy(shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error) {
	if s, err := t.shardingStrategy.Get(shard); err == nil {
		return s.Copy(startSequenceID, maxMessages, w, preC, postC)
	}

	return 0, fmt.Errorf("topic: Unknown shard %s", shard)
}

// String from stringer interface
func (t *FSTopic) String() string {
	return fmt.Sprintf("path: %s shards: %d", t.dir, len(t.shards))
}
