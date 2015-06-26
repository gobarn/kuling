package kuling

import (
	"errors"
	"io"
	"log"
	"os"
)

var (
	// ErrUnknownTopic returned when trying to access non exising topic
	ErrUnknownTopic = errors.New("logstore: Unknown Topic")
)

// FSTopicLogStore is a file system based log store.
type FSTopicLogStore struct {
	// Root directory of log store
	dir string
	// Map of topic names to file system topics structs
	topics map[string]*FSTopic
	// Channel that will broadcast when the log store has closed down
	closed chan struct{}
}

// OpenFSTopicLogStore opens or create ile system topic log store
func OpenFSTopicLogStore(dir string, permDirectories, permData os.FileMode) *FSTopicLogStore {
	if permDirectories < 0700 {
		panic("Directories must have execute right for running user")
	}
	if permData < 0600 {
		panic("Files must have read and write permissions for running user")
	}

	stat, err := os.Stat(dir)

	if err != nil || !stat.IsDir() {
		log.Fatalln("logstore: Path is not a directory")
	}

	return &FSTopicLogStore{
		dir,
		make(map[string]*FSTopic),
		make(chan (struct{})),
	}
}

// CreateTopic a new topic with given name. Name must not contain
// spaces or non file system ok chars
func (ls *FSTopicLogStore) CreateTopic(topic string) error {
	// TODO make sure topic is correct and change to file system functions
	// top create the directory
	topicDir := ls.dir + "/" + topic

	fsTopic, err := OpenFSTopicWithFixedShardingStrategy(topicDir, 10)
	if err != nil {
		return err
	}

	log.Printf("logstore: Topic %s created", topic)

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

	return ErrUnknownTopic
}

// Read messages into message array
func (ls *FSTopicLogStore) Read(topic, shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if fsTopic, ok := ls.topics[topic]; ok {
		return fsTopic.Read(shard, startSequenceID, maxMessages)
	}

	return nil, ErrUnknownTopic
}

// Copy data from the topic, shard into the io writer
func (ls *FSTopicLogStore) Copy(topic, shard string, startSequenceID, maxMessages int64, w io.Writer) (int64, error) {
	if fsTopic, ok := ls.topics[topic]; ok {
		return fsTopic.Copy(shard, startSequenceID, maxMessages, w)
	}

	return 0, ErrUnknownTopic
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
