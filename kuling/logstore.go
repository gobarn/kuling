package kuling

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// ErrClosed signals that the LogStore is closed
var ErrClosed = errors.New("LogStore Closed")

// ErrRootNotExists when the root path the user sends in is not created
// before running.
var ErrRootNotExists = errors.New("Root path needs to be created")

// ErrTopicNotExist signals that the requested topic does not exist
var ErrTopicNotExist = errors.New("Topic does not exist")

// ErrShardKeyNotProvided signals that there was no shard key provieded
var ErrShardKeyNotProvided = errors.New("Shard key not provided")

// ErrKeyNotProvided key was not provided
var ErrKeyNotProvided = errors.New("Payload Key not provided")

// ErrPayloadNotProvided was not provieded
var ErrPayloadNotProvided = errors.New("Payload not provided")

// LogStore interface for log stores
type LogStore interface {
	// CreateTopic creates a new topic. If the topic exists it returns
	// a topic already exists error
	CreateTopic(topic string) error
	// Write inserts the paylooad into the topic and partition
	Write(topic, shard string, key, payload []byte) error
	// Read will take a collection of messages and return that collection as
	// parsed messages. It reads from the topic.
	Read(topic, shard string, startSequenceID, maxMessages int64) ([]*Message, error)
	// Copy will copy a collection of messages from the topic and partition
	// from the store into the provied writer
	Copy(topic, shard string, startSequenceID, maxMessages int64, w io.Writer) (int64, error)
	// Closed returns a channel that is closed when the Log is closed
	// or times out.
	Closed() <-chan struct{}
	// Close down the log store. Calls Done channel when finished
	Close()
}

// TopicLogStore struct
//  root/
//      meta.db
//      topic1/
//            shard_1/
//					             partition.idx
//          						 segment_01.data
//                       segment_02.data
//            shard_2/
//					             index.db
//          						 000000001.data
//                       000000002.data
//
//
type TopicLogStore struct {
	// Flag indicating if the log is open or closed
	running bool
	// Permissions for directories and files
	permDir, permFile os.FileMode
	// The root path of the database, this is where segments and such
	// are positioned
	rootPath string
	// Current offset
	offsets map[string]int64
	// Map of topic to file handle, we start with one file for each topics
	// and expand from there.
	logs map[string]*os.File
	// Each topic has a lock that needs to be acquired for each operation on
	// it
	locks map[string]sync.Locker
	// Index for each topic
	indexes map[string]*LogIndex
	// Closed channel, the log broadcasts on this when it has closed down
	closed chan struct{}
}

// OpenLogStore from given root path. The root path must be a directory
// perm directories must have execute right for the user creating
// the database, perm data needs to have read write access
func OpenLogStore(root string, permDirectories, permData os.FileMode) LogStore {
	if permDirectories < 0700 {
		panic("Directories must have execute right for running user")
	}
	if permData < 0600 {
		panic("Files must have read and write permissions for running user")
	}

	info, err := os.Stat(root)

	if err != nil {
		panic(err)
	}

	if !info.IsDir() {
		panic("Root is not a directory")
	}

	// arrays of logs
	logs := make(map[string]*os.File)
	locks := make(map[string]sync.Locker)
	offsets := make(map[string]int64)
	indexes := make(map[string]*LogIndex)

	// Create log store
	ls := &TopicLogStore{
		true,
		permDirectories,
		permData,
		root,
		offsets,
		logs,
		locks,
		indexes,
		make(chan (struct{})),
	}

	// Load all the topics from the directory structure
	ls.loadFromRootPath(root)

	return ls
}

// Load a log store from the root directory by traversing the folders
// and files
func (ls *TopicLogStore) loadFromRootPath(root string) error {
	// load the existing files into the log store. The strucutre from the
	// root contain one folder for each topic and in each of those
	var topic string
	err := filepath.Walk(root, func(topicDir string, f os.FileInfo, err error) error {
		// Skip loose files
		if !f.IsDir() {
			return nil
		}

		if topicDir == root {
			// Walking includes the parent directory which we do not want to
			// take into account
			return nil
		}

		// The directory is the topic name
		topic = f.Name()
		ls.locks[topic] = &sync.RWMutex{}

		// Found topic dir, walk it and load index and data file
		err = filepath.Walk(topicDir, func(path string, f os.FileInfo, err error) error {
			// We don't want dirs in the topic folder
			if f.IsDir() {
				return nil
			}

			if strings.HasSuffix(f.Name(), "idx") {
				// Found index file, load it up into the log store
				index, err := OpenIndex(path)

				if err != nil {
					return err
				}

				ls.indexes[topic] = index
			}

			// Found data file
			if strings.HasSuffix(f.Name(), "data") {
				// Open the file with write only permissions and append mode that moves the
				// seek handle to the end of the file for each write. Ask the OS to
				// create the file if not present.
				dataFile, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, ls.permFile)

				if err != nil {
					return err
				}

				ls.logs[topic] = dataFile
			}

			return nil
		})

		return err
	})

	return err
}

// CreateTopic creates a topic if it does not already exist.
func (ls *TopicLogStore) CreateTopic(topic string) error {
	_, err := ls.createTopicIfNotExists(topic)

	return err
}

// Close closes the log store, you need to handle this as the last
// thing
func (ls *TopicLogStore) Close() {
	// notify others that the DB is now closed
	ls.running = false
	// Close all file handles by first acquireing their respective write locks
	for _, f := range ls.logs {
		f.Close()
	}

	for _, index := range ls.indexes {
		index.Close()
	}

	// Close the closed channel
	close(ls.closed)
}

// Closed returns a channel that is called is sent on when the log store closes
func (ls *TopicLogStore) Closed() <-chan (struct{}) {
	return ls.closed
}

func (ls *TopicLogStore) createTopicIfNotExists(topic string) (*os.File, error) {
	if log, ok := ls.logs[topic]; ok {
		// Topic already exists
		return log, nil
	}

	err := os.Mkdir(path.Join(ls.rootPath, topic), ls.permDir)

	if err != nil {
		// Could not create topic directory
		return nil, err
	}

	// Open the file with write only permissions and append mode that moves the
	// seek handle to the end of the file for each write. Ask the OS to
	// create the file if not present.
	dataFile, err := os.OpenFile(path.Join(ls.rootPath, topic, topic+".data"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, ls.permFile)

	if err != nil {
		return nil, err
	}

	ls.logs[topic] = dataFile
	ls.locks[topic] = &sync.RWMutex{}

	// Open Index from file
	index, err := OpenIndex(path.Join(ls.rootPath, topic, topic+".idx"))

	if err != nil {
		// Could not open index!
		panic(err)
		// return nil, err
	}

	// Store the index for the topic
	ls.indexes[topic] = index

	return dataFile, nil
}

// Write the keyed message payload into the DB onto the topics partition
func (ls *TopicLogStore) Write(topic, shard string, key, payload []byte) error {
	// Guards
	if !ls.running {
		return ErrClosed
	}
	if len(topic) == 0 {
		return ErrTopicNotExist
	}
	if len(shard) == 0 {
		return ErrShardKeyNotProvided
	}
	if len(key) == 0 {
		return ErrKeyNotProvided
	}
	if len(payload) == 0 {
		return ErrPayloadNotProvided
	}

	// Get topic log file
	log, err := ls.createTopicIfNotExists(topic)

	if err != nil {
		// Could not create topic
		return err
	}

	// Get Index and store a new value in it
	if index, ok := ls.indexes[topic]; ok {
		// Write to the index store and save the current offset of the just
		// written message
		sequenceID, err := index.Next(ls.offsets[topic], CalculateMessageSize(key, payload))
		if err != nil {
			// Could not get index, not good!
			panic(err)
		}

		// Create message
		m := NewMessage(sequenceID, key, payload)

		// Write message
		messageWriter := NewMessageWriter(log)

		numBytesWritten, err := messageWriter.WriteMessage(m)
		if err != nil {
			panic(err)
		}

		ls.offsets[topic] += numBytesWritten
		return nil
	}

	return ErrTopicNotExist
}

// Read from start sequence and max number of messages forward and convert the
// binary messages into deserialized messages
func (ls *TopicLogStore) Read(topic, shard string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if !ls.running {
		return nil, ErrClosed
	}
	// Check that the topic requested to read from exists
	if _, ok := ls.logs[topic]; ok {
		// Create a read file handle for the file
		filePath := path.Join(ls.rootPath, topic+".data")
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)

		if err != nil {
			return nil, err
		}

		defer f.Close()

		// Create message reader
		messageReader := NewMessageReader(f)

		messages, err := messageReader.ReadMessages()

		if err != nil {
			return nil, err
		}

		return messages, nil
	}

	return nil, ErrTopicNotExist
}

// OffsetOf returns the offset of a sequence ID in a a topic
func (ls *TopicLogStore) offsetOf(topic string, sequenceID int64) (int64, error) {
	// Grab the index of the topic to get the offsets from the startSequenceID
	index, ok := ls.indexes[topic]
	if !ok {
		return 0, errors.New("Index not found")
	}

	offset, _, err := index.GetOffset(sequenceID)
	if err != nil {
		// Could not get offset for sequence ID
		return 0, err
	}

	return offset, nil
}

// Copy from start sequence and max number of messages forward into the writer
func (ls *TopicLogStore) Copy(topic, shard string, startSequenceID, maxMessages int64, w io.Writer) (int64, error) {
	if !ls.running {
		return 0, ErrClosed
	}

	fmt.Println(startSequenceID, maxMessages)

	// Check that the topic requested to read from exists
	if _, ok := ls.logs[topic]; ok {
		// Create a read file handle for the file
		filePath := path.Join(ls.rootPath, topic, topic+".data")
		// Open read only file handle that will copy data
		f, err := os.OpenFile(filePath, os.O_RDONLY, ls.permFile)

		if err != nil {
			// Could not get read handle for file. Too many open files?
			return 0, err
		}

		// Make sure to close the read file handle
		defer f.Close()

		// Seek to the start position of the startSequenceID in the file
		offset, err := ls.offsetOf(topic, startSequenceID)
		if err != nil {
			// this means that the start ID is higher than the last written ID
			return 0, errors.New("Start ID does not exist")
		}

		endOffset, err := ls.offsetOf(topic, startSequenceID+maxMessages)

		if err != nil {
			// This means that the offset of the max message is greater than
			// the file size, so just copy the entire file from the seek position
			// and then the rest of the file
			return io.Copy(w, f)
		}

		// The number of bytes to copy is the end offset minus start offset
		bytesToRead := endOffset - offset

		// Copy the message file from the topic file to the writer from the client
		// read X number of messages forward
		return io.CopyN(w, f, bytesToRead)
	}

	return 0, ErrTopicNotExist
}
