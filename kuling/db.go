package kuling

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/boltdb/bolt"
)

// ErrClosed signals that the LogStore is closed
var ErrClosed = errors.New("LogStore Closed")

// ErrTopicNotExist signals that the requested topic does not exist
var ErrTopicNotExist = errors.New("Topic does not exist")

var topics = []byte("topics")

// LogStore struct
//  root/
//      meta.db
//      topic1/
//            partition_1/
//					             partition.idx
//          						 000000001.data
//                       000000002.data
//            partition_2/
//					             index.db
//          						 000000001.data
//                       000000002.data
//
type LogStore struct {
	running bool
	// The root path of the database, this is where segments and such
	// are positioned
	rootPath string
	// Current offset
	currentOffset int64
	// Meta data database that contain all metadata that is global for this
	// store
	metaDB *bolt.DB
	// Map of topic to file handle, we start with one file for each topic
	// and expand from there.
	logs map[string]*os.File
	// Each topic has a lock that needs to be acquired for each operation on
	// it
	locks map[string]sync.Locker
	// Index for each topic
	indexes map[string]*LogIndex
}

// OpenLogStore from given root path. The root path must be a directory
func OpenLogStore(root string) *LogStore {
	info, err := os.Stat(root)

	if err != nil {
		panic(err)
	}

	if !info.IsDir() {
		panic("Root is not a directory")
	}

	// Open bolt database
	metaDB, err := bolt.Open(path.Join(root, "meta.db"), 0644, nil)
	if err != nil {
		panic(err)
	}

	// arrays of logs
	logs := make(map[string]*os.File)
	locks := make(map[string]sync.Locker)
	indexes := make(map[string]*LogIndex)

	// Create log store
	logStore := &LogStore{true, root, info.Size(), metaDB, logs, locks, indexes}
	//
	// rootDir, err := os.Open(root)
	// dirs, err := rootDir.Readdirnames(-1)

	// Get all known topics from the meta db and create log entries for each
	// in the log store
	err = metaDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(topics)
		if err != nil {
			panic(err)
		}

		// For each topic create a log entry
		b.ForEach(func(k, v []byte) error {
			// Create the log from the root and join the name of the topic
			fmt.Println("Existing topic" + string(k))
			_, err := logStore.createTopicIfNotExists(string(k))

			return err
		})

		return nil
	})

	return logStore
}

// Close closes the log store, you need to handle this as the last
// thing
func (d *LogStore) Close() {
	// notify others that the DB is now closed
	d.running = false
	// Close meta data DB
	d.metaDB.Close()
	// Close all file handles by first acquireing their respective write locks
	for _, f := range d.logs {
		f.Close()
	}

	for _, index := range d.indexes {
		index.Close()
	}
}

func (d *LogStore) createTopicIfNotExists(topic string) (*os.File, error) {
	if log, ok := d.logs[topic]; ok {
		// Topic already exists
		return log, nil
	}

	// store the topic in the meta db
	err := d.metaDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(topics)
		if err != nil {
			panic(err)
		}

		b.Put([]byte(topic), []byte(topic))

		return nil
	})

	// Open the file with write only permissions and append mode that moves the
	// seek handle to the end of the file for each write. Ask the OS to
	// create the file if not present.
	dataFile, err := os.OpenFile(path.Join(d.rootPath, topic+".data"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	d.logs[topic] = dataFile
	d.locks[topic] = &sync.RWMutex{}

	// Open or create new Index based on BoltDB
	index, err := OpenIndex(path.Join(d.rootPath, topic+".idx2"))

	if err != nil {
		// Could not open index!
		panic(err)
		// return nil, err
	}

	// Store the index for the topic
	d.indexes[topic] = index

	return dataFile, nil
}

// Write the keyed message payload into the DB onto the topics partition
func (d *LogStore) Write(topic, partition string, key, payload []byte) error {
	if !d.running {
		return ErrClosed
	}

	// Get topic log file
	log, err := d.createTopicIfNotExists(topic)

	if err != nil {
		// Could not create topic
		return err
	}

	// Get Index and store a new value in it
	if index, ok := d.indexes[topic]; ok {
		// TODO all of this need to be in TX.
		// Create message
		m := NewMessage(key, payload)
		// Write message
		numBytesWritten, err := WriteMessage(log, m)
		if err != nil {
			panic(err)
		}
		// Write to the index store and save the current offset of the just
		// written message
		_, err = index.Next(d.currentOffset)
		if err != nil {
			panic(err)
		}

		d.currentOffset += numBytesWritten
		return nil
	}

	return errors.New("Topic index does not exist")
}

// Read from start sequence and max number of messages forward and convert the
// binary messages into deserialized messages
func (d *LogStore) Read(topic string, startSequenceID, maxMessages int64) ([]*Message, error) {
	if !d.running {
		return nil, ErrClosed
	}
	// Check that the topic requested to read from exists
	if _, ok := d.logs[topic]; ok {
		// Create a read file handle for the file
		filePath := path.Join(d.rootPath, topic+".data")
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)

		if err != nil {
			return nil, err
		}

		defer f.Close()

		messages, _, err := ReadMessages(f)

		if err != nil {
			return nil, err
		}

		return messages, nil
	}

	return nil, ErrTopicNotExist
}

func (d *LogStore) offsetOf(topic string, sequenceID int64) (int64, error) {
	// Grab the index of the topic to get the offsets from the startSequenceID
	index, ok := d.indexes[topic]
	if !ok {
		return 0, errors.New("Index not found")
	}

	offset, err := index.GetOffset(sequenceID)
	if err != nil {
		// Could not get offset for sequence ID
		return 0, err
	}

	return offset, nil
}

// Copy from start sequence and max number of messages forward into the writer
func (d *LogStore) Copy(topic string, startSequenceID, maxMessages int64, w io.Writer) (int64, error) {
	if !d.running {
		return 0, ErrClosed
	}

	// Check that the topic requested to read from exists
	if _, ok := d.logs[topic]; ok {
		// Create a read file handle for the file
		filePath := path.Join(d.rootPath, topic+".data")
		// Open read only file handle that will copy data
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)

		if err != nil {
			// Could not get read handle for file. Too many open files?
			return 0, err
		}

		// Make sure to close the read file handle
		defer f.Close()

		// Seek to the start position of the startSequenceID in the file
		offset, err := d.offsetOf(topic, startSequenceID)
		fmt.Printf("Offset %d\n", offset)
		if err != nil {
			// this means that the start ID is higher than the last written ID
			return 0, errors.New("Start ID does not exist")
		}

		endOffset, err := d.offsetOf(topic, startSequenceID+maxMessages)
		fmt.Printf("End Offset %d\n", endOffset)

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
