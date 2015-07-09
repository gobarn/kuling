package kuling

import "io"

// PreCopy function that will be called before a copy action is carried
// out. It tells the caller the number of bytes to be read.
type PreCopy func(totalBytesToCopy int64)

// PostCopy function that will be called after a copy action is carried
// out. It tells the caller the number of bytes that were actually read
type PostCopy func(totalBytesCopied int64)

// LogStore interface for log stores
type LogStore interface {
	// CreateTopic creates a new topic. If the topic exists it returns
	// a topic already exists error
	CreateTopic(topic string, numShards int) (Topic, error)
	// Topics returns a slice of topics
	Topics() map[string]Topic
	// Append inserts the paylooad into the topic and shard
	Append(topic, shard string, key, payload []byte) error
	// Read will take a collection of messages and return that collection as
	// parsed messages. It reads from the topic.
	Read(topic, shard string, startSequenceID, maxMessages int64) ([]*Message, error)
	// Copy will copy a collection of messages from the topic and shard
	// from the store into the provied writer
	Copy(topic, shard string, startSequenceID, maxMessages int64, w io.Writer, preC PreCopy, postC PostCopy) (int64, error)
	// Closed returns a channel that is closed when the Log is closed
	// or times out.
	Closed() <-chan struct{}
	// Close down the log store. Calls Done channel when finished
	Close() error
}
