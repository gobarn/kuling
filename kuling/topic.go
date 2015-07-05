package kuling

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
)

// Topic store data in a topic
type Topic interface {
	// Createshard with the given name
	CreateShard(shardName string) error
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
	// Close the topic
	Close() error
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

	// Load all existing shards
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("topic: Could not open topic dir %s: %s", dir, err)
	}

	for _, f := range files {
		if !f.IsDir() {
			// We only want dirs!
			continue
		}

		shard, err := OpenFSShard(path.Join(dir, f.Name()), config.SegmentMaxBytes, config.PermDirectories, config.PermData)
		if err != nil {
			return nil, fmt.Errorf("topic: Could not load shard: %s\n", err)
		}

		topic.shards[f.Name()] = shard
	}

	return topic, nil
}

// CreateShard adds a folder under the topic directory with the name
// of the shard and adds it to the topic
func (t *FSTopic) CreateShard(shardName string) error {
	shard, err := OpenFSShard(path.Join(t.dir, shardName), t.config.SegmentMaxBytes, t.config.PermDirectories, t.config.PermData)
	if err != nil {
		return err
	}

	t.shards[shardName] = shard

	return nil
}

// Delete the topic and all the shards in it
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

// Close down the file system topic by closing all shards
func (t *FSTopic) Close() error {
	for _, p := range t.shards {
		p.Close()
	}

	return nil
}

// String from stringer interface
func (t *FSTopic) String() string {
	return fmt.Sprintf("path: %s shards: %d", t.dir, len(t.shards))
}
