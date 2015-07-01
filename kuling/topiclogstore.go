package kuling

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

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

// FSTopicLogStore is a file system based log store.
type FSTopicLogStore struct {
	// Root directory of log store
	dir string
	// Map of topic names to file system topics structs
	topics map[string]*FSTopic
	// Channel that will broadcast when the log store has closed down
	closed chan struct{}
	// Global configuration for all topics
	config *FSConfig
}

// OpenFSTopicLogStore opens or create ile system topic log store
func OpenFSTopicLogStore(dir string, c *FSConfig) *FSTopicLogStore {
	if c.PermDirectories < 0700 {
		panic("logstore: Directories must have execute right for running user")
	}
	if c.PermData < 0600 {
		panic("logstore: Files must have read and write permissions for running user")
	}

	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		log.Fatalln("logstore: Path is not a directory")
	}

	return &FSTopicLogStore{
		dir,
		make(map[string]*FSTopic),
		make(chan (struct{})),
		c,
	}
}

// CreateTopic a new topic with given name. Name must not contain
// spaces or non file system ok chars
func (ls *FSTopicLogStore) CreateTopic(topic string, numShards int) error {
	topicDir := path.Join(ls.dir, topic)

	fsTopic, err := OpenFSTopicWithFixedShardingStrategy(topicDir, numShards, ls.config)
	if err != nil {
		return err
	}

	// Save the file system topic to map of topics
	ls.topics[topic] = fsTopic
	return nil
}

// Append data to log store in given topic and shard
func (ls *FSTopicLogStore) Append(topic, shard string, key, payload []byte) error {
	if fsTopic, ok := ls.topics[topic]; ok {
		if err := fsTopic.Append(shard, key, payload); err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("topic: Unknown topic %s", topic)
}

// Read messages into message array
func (ls *FSTopicLogStore) Read(topic, shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if fsTopic, ok := ls.topics[topic]; ok {
		return fsTopic.Read(shard, startSequenceID, maxMessages)
	}

	return nil, fmt.Errorf("topic: Unknown topic %s", topic)
}

// Copy data from the topic, shard into the io writer
func (ls *FSTopicLogStore) Copy(topic, shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error) {
	if fsTopic, ok := ls.topics[topic]; ok {
		return fsTopic.Copy(shard, startSequenceID, maxMessages, w, preC, postC)
	}

	return 0, fmt.Errorf("topic: Unknown topic %s", topic)
}

// Closed returns closing channel that will broadcast when the log store
// has shut down
func (ls *FSTopicLogStore) Closed() <-chan struct{} {
	return ls.closed
}

// Close the file system topics down
func (ls *FSTopicLogStore) Close() {
	// Close all File system topic stores

	// Close the closed channel
	close(ls.closed)
}
