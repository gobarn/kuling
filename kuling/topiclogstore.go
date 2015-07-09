package kuling

import (
	"fmt"
	"io"
	"io/ioutil"
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
	// Global configuration for all topics
	config *FSConfig
	// Root directory of log store
	dir string
	// Map of topic names to file system topics structs
	topics map[string]Topic
	// Channel that will broadcast when the log store has closed down
	closed chan struct{}
}

// OpenFSTopicLogStore opens or create ile system topic log store
func OpenFSTopicLogStore(dir string, c *FSConfig) (LogStore, error) {
	if c.PermDirectories < 0700 {
		return nil, fmt.Errorf("logstore: Directories must have execute right for running user")
	}
	if c.PermData < 0600 {
		return nil, fmt.Errorf("logstore: Files must have read and write permissions for running user")
	}

	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		log.Println("logstore: Path is not a directory")
		return nil, err
	}

	logStore := &FSTopicLogStore{
		c,
		dir,
		make(map[string]Topic),
		make(chan (struct{})),
	}

	// Load all existing topics from the file system
	// This might need improvement as it just loads all
	// directories and treats them as topic
	// Load segment files, important that we load them in correct order
	// such that the first segment file is loaded first.
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("logstore: Could not open logstore dir %s: %s", dir, err)
	}

	for _, f := range files {
		if !f.IsDir() {
			// We only want dirs!
			continue
		}

		log.Println("logstore: Found existing topic", f.Name())

		topic, err := OpenFSTopic(path.Join(dir, f.Name()), c)
		if err != nil {
			return nil, fmt.Errorf("logstore: Could not load topic: %s\n", err)
		}

		logStore.topics[f.Name()] = topic
	}

	return logStore, nil
}

// CreateTopic a new topic with given name. Name must not contain
// spaces or non file system ok chars
func (ls *FSTopicLogStore) CreateTopic(topicName string, numShards int) (Topic, error) {
	topic, err := OpenFSTopic(path.Join(ls.dir, topicName), ls.config)
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
				return nil, fmt.Errorf("logstore: Unable to create topic %s, cleanup failed", topicName)
			}
			return nil, fmt.Errorf("logstore: Unable to create topic %s, cleanup success", topicName)
		}
	}

	ls.topics[topicName] = topic
	return topic, nil
}

// Topics returns a map of topic names to topics
func (ls *FSTopicLogStore) Topics() map[string]Topic {
	return ls.topics
}

// DeleteTopic deletes topic with given name
func (ls *FSTopicLogStore) DeleteTopic(topicName string) error {
	if t, ok := ls.topics[topicName]; ok {
		return t.Delete()
	}

	return fmt.Errorf("topic: Unknown topic %s", topicName)
}

// Append data to log store in given topic and shard
func (ls *FSTopicLogStore) Append(topic, shard string, key, payload []byte) error {
	if fsTopic, ok := ls.topics[topic]; ok {
		return fsTopic.Append(shard, key, payload)
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
func (ls *FSTopicLogStore) Close() error {
	// Close the closed channel
	for _, t := range ls.topics {
		t.Close()
	}

	close(ls.closed)

	return nil
}
