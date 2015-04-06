package kuling

import (
	"bufio"
	"bytes"
	"encoding/binary"
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
	// Meta data database that contain all metadata that is global for this
	// store
	metaDB *bolt.DB
	// Map of topic to file handle, we start with one file for each topic
	// and expand from there.
	logs map[string]*os.File
	// Each topic has a lock that needs to be acquired for each operation on
	// it
	locks   map[string]sync.Locker
	indexes map[string]*bolt.DB
	// For each log, the offset of the next message
	logsOfsset map[string]int64
}

// Open LogStore from given root path. The root path must be a directory
func Open(root string) *LogStore {
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

	// Slice of logs
	logs := make(map[string]*os.File)
	locks := make(map[string]sync.Locker)
	indexes := make(map[string]*bolt.DB)
	logsOffset := make(map[string]int64)

	// Create log store
	logStore := &LogStore{true, root, metaDB, logs, locks, indexes, logsOffset}

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
			fmt.Println("Creating topic " + string(k))
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
}

func (d *LogStore) createTopicIfNotExists(topic string) (*os.File, error) {
	if log, ok := d.logs[topic]; ok {
		// Topic already exists
		return log, nil
	}

	dataFile, err := os.OpenFile(path.Join(d.rootPath, topic+".data"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	d.logs[topic] = dataFile
	d.locks[topic] = &sync.RWMutex{}
	d.logsOfsset[topic] = 0

	// Open or create new Index based on BoltDB
	index, err := bolt.Open(path.Join(d.rootPath, topic+".idx"), 0600, nil)

	if err != nil {
		return nil, err
	}

	d.indexes[topic] = index
	// Add 0 sequence id and offset to the index
	err = index.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(topic))

		if err != nil {
			return err
		}

		// Set the next ID:s offset in the index
		nextSequenceIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(nextSequenceIDBytes, 0)
		nextOffsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(nextOffsetBytes, 0)

		// Set the index value
		err = b.Put(nextSequenceIDBytes, nextOffsetBytes)

		if err != nil {
			panic(err)
		}

		return nil
	})

	if err != nil {
		panic(err)
	}

	return dataFile, nil
}

// Write the keyed message payload into the DB onto the topics partition
func (d *LogStore) Write(topic, partition string, key, payload []byte) error {
	if !d.running {
		return ErrClosed
	}

	log, err := d.createTopicIfNotExists(topic)

	if err != nil {
		return err
	}

	// Get Index for topics
	if index, ok := d.indexes[topic]; ok {
		err := index.Update(func(tx *bolt.Tx) error {
			// Acquire lock for given topic
			d.locks[topic].Lock()
			defer d.locks[topic].Unlock()
			// Create message
			m := NewMessage(key, payload)

			// Create buffered writer from the log file
			w := bufio.NewWriter(log)
			totalMessageLen, err := WriteMessage(w, m)

			if err != nil {
				// Could not write to file
				return err
			}

			// Increment the sequence ID and the offset of the next sequence ID
			return d.incrementTopic(topic, totalMessageLen, tx)
		})

		return err
	}

	return errors.New("Topic index does not exist")
}

func (d *LogStore) incrementTopic(topic string, offsetIncrement int64, tx *bolt.Tx) error {
	// Get the topics bucket
	b, err := tx.CreateBucketIfNotExists([]byte(topic))
	if err != nil {
		return err
	}

	// Get sequence number and offset of last message stored
	k, v := b.Cursor().Last()

	if k == nil {
		return errors.New("No sequence ID:s for topic, this is bad")
	}

	var currentSequenceID int64
	buf := bytes.NewBuffer(k)
	binary.Read(buf, binary.BigEndian, &currentSequenceID)

	var currentOffset int64
	buf = bytes.NewBuffer(v)
	binary.Read(buf, binary.BigEndian, &currentOffset)

	//Increment sequence for this message
	nextSequenceID := currentSequenceID + 1
	// Increment offset for the next message
	nextOffset := currentOffset + offsetIncrement
	// Set the next ID:s offset in the index
	buf = bytes.NewBuffer(make([]byte, 0))
	binary.Write(buf, binary.BigEndian, &nextSequenceID)
	nextSequenceIDBytes := buf.Bytes()
	buf = bytes.NewBuffer(make([]byte, 0))
	binary.Write(buf, binary.BigEndian, &nextOffset)
	nextOffsetBytes := buf.Bytes()

	b.Put(nextSequenceIDBytes, nextOffsetBytes)

	return nil
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

		r := bufio.NewReader(f)
		messages, err := ReadMessages(r)

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

	var startOffset int64
	err := index.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(topic))

		buf := bytes.NewBuffer(make([]byte, 0))
		binary.Write(buf, binary.BigEndian, &sequenceID)
		startSequenceID := buf.Bytes()

		// Get the offset value from the sequence ID in the index
		v := b.Get(startSequenceID)

		if v == nil {
			return errors.New("Sequence ID does not exist")
		}

		buf = bytes.NewBuffer(v)
		binary.Read(buf, binary.BigEndian, &startOffset)

		return nil
	})

	if err != nil {
		// Could not get offset for sequence ID
		return 0, err
	}

	return startOffset, nil
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

		defer f.Close()

		// Seek to the start position of the startSequenceID in the file
		offset, err := d.offsetOf(topic, startSequenceID)
		if err != nil {
			// this means that the start ID is higher than the last written ID
			return 0, errors.New("Start ID does not exist")
		}

		f.Seek(offset, 0)

		endOffset, err := d.offsetOf(topic, startSequenceID+maxMessages)

		if err != nil {
			// This means that the offset of the max message is greater than
			// the file size, so just copy the entire file from the seek position
			// and then the rest of the file
			return io.Copy(w, f)
		}

		// Copy the message file from the topic file to the writer from the client
		// read X number of messages forward
		return io.CopyN(w, f, endOffset)
	}

	return 0, ErrTopicNotExist
}
