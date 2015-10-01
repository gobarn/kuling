package kuling

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
)

// Topic handles an entire topic
type Topic struct {
	config *Config
	// topics directory
	dir string
	// map of shard name to shard
	shards map[string]*Shard
}

// OpenTopic opens or creates a new file system topic
func OpenTopic(dir string, config *Config) (*Topic, error) {
	if config.PermDirectories < 0700 {
		panic("topic: directories must have execute right for running user")
	}
	if config.PermData < 0600 {
		panic("topic: files must have read and write permissions for running user")
	}

	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		log.Printf("topic: creating topic directory %s", dir)
		// The directory does not exist, lets create it
		err := os.Mkdir(dir, config.PermDirectories)

		if err != nil {
			return nil, fmt.Errorf("topic: could not create topic directory %s", dir)
		}
	}

	topic := &Topic{
		config,
		dir,
		make(map[string]*Shard),
	}

	// Load all existing shards
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("topic: could not open topic dir %s: %s", dir, err)
	}

	for _, f := range files {
		if !f.IsDir() {
			// We only want dirs!
			continue
		}

		shard, err := OpenShard(path.Join(dir, f.Name()), config.SegmentMaxBytes, config.PermDirectories, config.PermData)
		if err != nil {
			return nil, fmt.Errorf("topic: could not load shard: %s\n", err)
		}

		topic.shards[f.Name()] = shard
	}

	return topic, nil
}

// CreateShard adds a folder under the topic directory with the name
// of the shard and adds it to the topic
func (t *Topic) CreateShard(shardName string) error {
	shard, err := OpenShard(path.Join(t.dir, shardName), t.config.SegmentMaxBytes, t.config.PermDirectories, t.config.PermData)
	if err != nil {
		return err
	}

	t.shards[shardName] = shard

	return nil
}

// Shards gets a all shards for the topic
func (t *Topic) Shards() map[string]*Shard {
	return t.shards
}

// Delete the topic and all the shards in it
func (t *Topic) Delete() error {
	return os.RemoveAll(t.dir)
}

// Append key and payload to topic
func (t *Topic) Append(shard string, key, payload []byte) error {
	if s, ok := t.shards[shard]; ok {
		return s.Append(key, payload)
	}

	return fmt.Errorf("topic: unknown shard %s", shard)
}

// Read from topic shard from start sequence id and max messages
func (t *Topic) Read(shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if s, ok := t.shards[shard]; ok {
		return s.Read(startSequenceID, maxMessages)
	}

	return nil, fmt.Errorf("topic: unknown shard %s", shard)
}

// Copy from topic shard from start sequence id and max messages into io writer
func (t *Topic) Copy(shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error) {
	if s, ok := t.shards[shard]; ok {
		return s.Copy(startSequenceID, maxMessages, w, preC, postC)
	}

	return 0, fmt.Errorf("topic: unknown shard %s", shard)
}

// Close down the file system topic by closing all shards
func (t *Topic) Close() error {
	for _, p := range t.shards {
		p.Close()
	}

	return nil
}

// String from stringer interface
func (t *Topic) String() string {
	return fmt.Sprintf("path: %s shards: %d", t.dir, len(t.shards))
}
