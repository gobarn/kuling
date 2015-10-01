package kuling

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
)

// PreCopy function that will be called before a copy action is carried
// out. It tells the caller the number of bytes to be read.
type PreCopy func(totalBytesToCopy int64)

// PostCopy function that will be called after a copy action is carried
// out. It tells the caller the number of bytes that were actually read
type PostCopy func(totalBytesCopied int64)

// FSConfig contains configurations that are used during runtime and
// init of the file system based topic store. For instance segment max size
// and other configurations
type FSConfig struct {
	// Permissions for data and files in the file system topic store
	PermDirectories, PermData os.FileMode
	// Maximum bytes that will get squezed into one segment file before
	// a new one is created
	SegmentMaxBytes int64
}

// LogStore is a file system based log store.
type LogStore struct {
	// Global configuration for all topics
	config *FSConfig
	// Root directory of log store
	dir string
	// Map of topic names to file system topics structs
	topics map[string]*Topic
	// Channel that will broadcast when the log store has closed down
	closed chan struct{}
}

// OpenLogStore opens or create ile system topic log store
func OpenLogStore(dir string, c *FSConfig) (*LogStore, error) {
	if c.PermDirectories < 0700 {
		return nil, fmt.Errorf("logstore: directories must have execute right for running user")
	}
	if c.PermData < 0600 {
		return nil, fmt.Errorf("logstore: files must have read and write permissions for running user")
	}

	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		log.Println("logstore: path is not a directory")
		return nil, err
	}

	logStore := &LogStore{
		c,
		dir,
		make(map[string]*Topic),
		make(chan (struct{})),
	}

	// Load all existing topics from the file system
	// This might need improvement as it just loads all
	// directories and treats them as topic
	// Load segment files, important that we load them in correct order
	// such that the first segment file is loaded first.
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("logstore: could not open logstore dir %s: %s", dir, err)
	}

	for _, f := range files {
		if !f.IsDir() {
			// We only want dirs!
			continue
		}

		log.Println("logstore: found existing topic", f.Name())

		topic, err := OpenTopic(path.Join(dir, f.Name()), c)
		if err != nil {
			return nil, fmt.Errorf("logstore: could not load topic: %s\n", err)
		}

		logStore.topics[f.Name()] = topic
	}

	return logStore, nil
}

// CreateTopic a new topic with given name. Name must not contain
// spaces or non file system ok chars
func (ls *LogStore) CreateTopic(topicName string, numShards int) (*Topic, error) {
	topic, err := OpenTopic(path.Join(ls.dir, topicName), ls.config)
	if err != nil {
		return nil, err
	}

	for i := 0; i < numShards; i++ {
		err := topic.CreateShard(fmt.Sprintf("%d", i))
		if err != nil {
			// Try to delete the created topic as it could not be correctly
			// created
			err := ls.DeleteTopic(topicName)
			if err != nil {
				// Could not delete it, notify client that the topic has been created but
				// could not be removed after issue. The topic is not added to the list
				// of available topics so it will not be reachable until restart but
				// then it may not work
				return nil, fmt.Errorf("logstore: unable to create topic %s, cleanup failed", topicName)
			}
			return nil, fmt.Errorf("logstore: unable to create topic %s, cleanup success", topicName)
		}
	}

	ls.topics[topicName] = topic
	return topic, nil
}

// Topics returns a map of topic names to topics
func (ls *LogStore) Topics() map[string]*Topic {
	return ls.topics
}

// DeleteTopic deletes topic with given name
func (ls *LogStore) DeleteTopic(topic string) error {
	if t, ok := ls.topics[topic]; ok {
		return t.Delete()
	}

	return fmt.Errorf("topic: unknown topic %s", topic)
}

// Shards get a list of shards for a topic
func (ls *LogStore) Shards(topic string) (map[string]*Shard, error) {
	if t, ok := ls.topics[topic]; ok {
		return t.Shards(), nil
	}

	return nil, fmt.Errorf("topic: unknown topic %s", topic)
}

// Append data to log store in given topic and shard
func (ls *LogStore) Append(topic, shard string, key, payload []byte) error {
	if t, ok := ls.topics[topic]; ok {
		return t.Append(shard, key, payload)
	}

	return fmt.Errorf("topic: unknown topic %s", topic)
}

// Read messages into message array
func (ls *LogStore) Read(topic, shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if t, ok := ls.topics[topic]; ok {
		return t.Read(shard, startSequenceID, maxMessages)
	}

	return nil, fmt.Errorf("topic: unknown topic %s", topic)
}

// Copy data from the topic, shard into the io writer
func (ls *LogStore) Copy(topic, shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error) {
	if t, ok := ls.topics[topic]; ok {
		return t.Copy(shard, startSequenceID, maxMessages, w, preC, postC)
	}

	return 0, fmt.Errorf("topic: unknown topic %s", topic)
}

// Closed returns closing channel that will broadcast when the log store
// has shut down
func (ls *LogStore) Closed() <-chan struct{} {
	return ls.closed
}

// Close the file system topics down
func (ls *LogStore) Close() error {
	// Close the closed channel
	for _, t := range ls.topics {
		t.Close()
	}

	close(ls.closed)

	return nil
}
