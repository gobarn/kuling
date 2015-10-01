package kuling

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// The byte length of a key
const keyLen = 8

// The byte length of a value
const valueLen = 8

// The byte length of a value size
const valueSizeLen = 8

// Index is closed
var ErrIndexClosed = errors.New("index: index has been closed")

// The index write could not be made
var ErrIndexWriteFailed = errors.New("index: index write failed")

// ErrSequenceIDNotFound tells the user that the sequecne ID is to high or low
var ErrSequenceIDNotFound = errors.New("index: sequence ID not found")

// ErrIndexFileCouldNotBeOpened the index file could not be opened
var ErrIndexFileCouldNotBeOpened = errors.New("index: index file could not be opened")

// ErrNegativeOffset offset cannot be negative
var ErrNegativeOffset = errors.New("index: negative offset")

// ErrPathNotSet the path was not set correctly
var ErrPathNotSet = errors.New("index: path not set")

// LogIndex struct that knows how a index for a log file is
type LogIndex struct {
	// boolean to check that we are running
	running bool
	// The next sequence ID to use for writing
	nextSequenceID int64
	// Path to the file
	path string
	// The file we are writing index changes to.
	writeFile *os.File
	// Index size
	size int64
	// Lock for the index
	lock sync.RWMutex
	// readers wait group
	readWaitGroup sync.WaitGroup
}

// OpenIndex opens or creates a index from the file if it does not exist
// and opens the index if it already exists
func OpenIndex(path string, permData os.FileMode) (*LogIndex, error) {
	if len(path) == 0 {
		return nil, ErrPathNotSet
	}

	// IMPORTANT:
	// Open the file with create, append and read write.
	// Permissions set to R/W for the user executing
	var err error
	var writeFile *os.File
	if writeFile, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, permData); err != nil {
		// This should not happen, may be that the file has wrong permissions.
		return nil, err
	}

	// Lock file so that other processes cannot use the same file as that may
	// cause corruption. If we cannot lock the file after the timouet then
	// we return an error
	if err = flock(writeFile, 1000*time.Millisecond); err != nil {
		return nil, err
	}

	// Get file stat to check if it contains any previous entry
	fd, err := writeFile.Stat()
	if err != nil {
		return nil, err
	}

	// The next sequence ID can be calculated from the size which gives
	// the number of messages in the log and then adding 1 for it to represent
	// the next id
	nextSequenceID := fd.Size()/(keyLen+valueLen) + 1

	// Create log
	log := &LogIndex{
		true,
		nextSequenceID,
		path,
		writeFile,
		0,
		sync.RWMutex{},
		sync.WaitGroup{},
	}

	return log, err
}

// Close the index
func (idx *LogIndex) Close() {
	// Write lock the index and defer the unlock
	idx.lock.Lock()
	defer idx.lock.Unlock()
	defer funlock(idx.writeFile)

	idx.running = false
	// Close the write file for writing
	idx.writeFile.Close()
	// Readers do not acqurie read lock as they can read without checking in
	// with the writer. However we should wait for all the readers to finish
	idx.readWaitGroup.Wait()
}

// Next writes the offset value to the next sequence ID and the segment where
// that sequence ID belongs
func (idx *LogIndex) Next(segmentNumber, offsetValue int64) (int64, error) {
	if offsetValue < 0 {
		return 0, ErrNegativeOffset
	}
	if segmentNumber < 0 {
		return 0, fmt.Errorf("index: segment number negative")
	}

	// Writelock the index and defer the unlock
	idx.lock.Lock()
	defer idx.lock.Unlock()
	if !idx.running {
		return 0, ErrIndexClosed
	}

	// Buffered io for writing the entire index row in one flush to the
	// writer.
	buf := bufio.NewWriter(idx.writeFile)

	// Create a entry by combining the next sequence ID with the offset value
	// The file is opened in append mode thus we need not seek to the end
	currentSequenceID := idx.nextSequenceID
	err := binary.Write(buf, binary.BigEndian, &currentSequenceID)
	if err != nil {
		panic(err)
	}

	err = binary.Write(buf, binary.BigEndian, &offsetValue)
	if err != nil {
		panic(err)
	}

	err = binary.Write(buf, binary.BigEndian, &segmentNumber)
	if err != nil {
		panic(err)
	}

	// err = binary.Write(buf, binary.BigEndian, &messageSize)
	// if err != nil {
	// 	panic(err)
	// }

	// Flush the row to disk
	err = buf.Flush()

	if err != nil {
		// The log entry could not be written.
		return 0, ErrIndexWriteFailed
	}

	// Increase the sequence ID for this entry
	idx.nextSequenceID++
	// Increase the file size with the size of writing one index entry
	idx.size += (keyLen + valueLen)

	// return sequcenID for the entry
	return currentSequenceID, nil
}

// SegmentAndOffset finds the offset value stored under the sequenceID key. If
// The sequence ID cannot be found then ErrSequenceIDNotFound is returned.
// If any trouble during file operations ErrIndexFileCouldNotBeOpened is
// returned
// On success the segment number and the offset is returned, on failure an
// error is returned
func (idx *LogIndex) SegmentAndOffset(sequenceID int64) (int64, int64, error) {
	if !idx.running {
		return 0, 0, ErrIndexClosed
	}
	if sequenceID < 0 {
		return 0, 0, ErrSequenceIDNotFound
	}
	if sequenceID > (idx.nextSequenceID - 1) {
		return 0, 0, ErrSequenceIDNotFound
	}

	// Seek the write file to the write location of the sequenceID
	// Each "row" in the index stores two int64 numbers so calculating
	// the offset for a key can be done by multiplying int64 byte length
	// times two for the row length and then times the sequenceID to
	// get the next offset for the next sequence, if we then add one
	// int64 we get the value of the offset for the given sequence ID.
	seekOffset := (keyLen+valueLen+valueSizeLen)*sequenceID + keyLen

	// Add reader to wait group
	idx.readWaitGroup.Add(1)
	defer idx.readWaitGroup.Done()

	// Create read file handle
	readFile, err := os.Open(idx.path)

	if err != nil {
		return 0, 0, ErrIndexFileCouldNotBeOpened
	}

	// Seek to the seek offset of the sequence ID. As clients are most likely
	// to be up to speed it's better to seek from the end of the file
	_, err = readFile.Seek(seekOffset, os.SEEK_SET)

	if err == io.EOF {
		// Searched to the end of the file and could not find the sequence
		return 0, 0, ErrSequenceIDNotFound
	} else if err != nil {
		panic(err)
	}

	var value int64
	err = binary.Read(readFile, binary.BigEndian, &value) // Reads 8
	if err == io.EOF {
		// Searched to the end of the file and could not find the sequence
		return 0, 0, ErrSequenceIDNotFound
	} else if err != nil {
		panic(err)
	}

	var segmentNumber int64
	err = binary.Read(readFile, binary.BigEndian, &segmentNumber) // Reads 8
	if err == io.EOF {
		// Searched to the end of the file and could not find the sequence
		return 0, 0, ErrSequenceIDNotFound
	} else if err != nil {
		panic(err)
	}

	return segmentNumber, value, nil
}
