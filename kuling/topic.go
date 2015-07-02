package kuling

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
)

// Topic store data in a topic
type Topic interface {
	// CreatePartition with the given name
	CreatePartition(partitionName string) error
	// Write data with key and payload
	Append(shard string, key, payload []byte) error
	// Read data from specific shard starting form sequenceID and reading
	// max number of messages
	Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error)
	// Copy data from specific shard starting form sequenceID and reading
	// max number of messages into the io writer
	Copy(shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error)
	// Delete the Topic
	Delete() error
}

// FSTopic handles an entire topic with it's segments and indexex
type FSTopic struct {
	config *FSConfig
	// topics directory
	dir string
	// map of shard name to shard
	shards map[string]Shard
}

// OpenFSTopic opens or creates a new file system topic
func OpenFSTopic(dir string, config *FSConfig) (Topic, error) {
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

	topic := &FSTopic{
		config,
		dir,
		make(map[string]Shard),
	}

	// Load all existing partitions
	err = filepath.Walk(dir, func(topicDir string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			// Shards are directories, continue
			return nil
		}

		partition, err := OpenFSShard(path.Join(dir, f.Name()), config.SegmentMaxBytes, config.PermDirectories, config.PermData)
		if err != nil {
			return err
		}

		topic.shards[f.Name()] = partition

		return nil
	})
	if err != nil {
		log.Printf("topic: Could not load shard: %s\n", err)
		return nil, err
	}

	return topic, nil
}

// CreatePartition adds a folder under the topic directory with the name
// of the partition and adds it to the topic
func (t *FSTopic) CreatePartition(partitionName string) error {
	partition, err := OpenFSShard(path.Join(t.dir, partitionName), t.config.SegmentMaxBytes, t.config.PermDirectories, t.config.PermData)
	if err != nil {
		return err
	}

	t.shards[partitionName] = partition

	return nil
}

// Delete the topic and all the partitions in it
func (t *FSTopic) Delete() error {
	return os.RemoveAll(t.dir)
}

// Append key and payload to topic
func (t *FSTopic) Append(shard string, key, payload []byte) error {
	if s, ok := t.shards[shard]; ok {
		return s.Append(key, payload)
	}

	return fmt.Errorf("topic: Unknown shard %s", shard)
}

// Read from topic shard from start sequence id and max messages
func (t *FSTopic) Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if s, ok := t.shards[shard]; ok {
		return s.Read(startSequenceID, maxMessages)
	}

	return nil, fmt.Errorf("topic: Unknown shard %s", shard)
}

// Copy from topic shard from start sequence id and max messages into io writer
func (t *FSTopic) Copy(shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error) {
	if s, ok := t.shards[shard]; ok {
		return s.Copy(startSequenceID, maxMessages, w, preC, postC)
	}

	return 0, fmt.Errorf("topic: Unknown shard %s", shard)
}

// String from stringer interface
func (t *FSTopic) String() string {
	return fmt.Sprintf("path: %s shards: %d", t.dir, len(t.shards))
}
