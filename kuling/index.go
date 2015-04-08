package kuling

import (
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

// Index is closed
var ErrIndexClosed = errors.New("Index has been closed")

// ErrSequenceIDNotFound tells the user that the sequecne ID is to high or low
var ErrSequenceIDNotFound = errors.New("Sequence ID not found")

// ErrIndexFileCouldNotBeOpened the index file could not be opened
var ErrIndexFileCouldNotBeOpened = errors.New("Index file could not be opened")

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
	lock *sync.RWMutex
	// readers wait group
	readWaitGroup *sync.WaitGroup
}

// OpenIndex creates a index from the file if it does not exist
// and opens the index if it already exists
func OpenIndex(path string) (*LogIndex, error) {
	log := &LogIndex{true, 0, path, nil, 0, &sync.RWMutex{}, &sync.WaitGroup{}}
	// IMPORTANT:
	// Open the file with create, append and read write.
	// Permissions set to R/W for the user executing
	var err error
	if log.writeFile, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600); err != nil {
		log.Close()
		// This should not happen, may be that the file has wrong permissions.
		return nil, err
	}

	// Lock file so that other processes cannot use the same file as that may
	// cause corruption. If we cannot lock the file after the timouet then
	// we return an error
	if err = flock(log.writeFile, 1000*time.Millisecond); err != nil {
		log.Close()
		return nil, err
	}

	// Get file stat to check if it contains any previous entry
	fd, err := log.writeFile.Stat()
	if err != nil {
		return nil, err
	}

	// The next sequence ID can be calculated from the size which gives
	// the number of messages in the log and then adding 1 for it to represent
	// the next id
	log.nextSequenceID = fd.Size()/(keyLen+valueLen) + 1

	return log, nil
}

// Close the index
func (idx *LogIndex) Close() {
	// Write lock the index and defer the unlock
	idx.lock.Lock()
	defer idx.lock.Unlock()
	idx.running = false
	// Close the write file for writing
	idx.writeFile.Close()
	// Readers do not acqurie read lock as they can read without checking in
	// with the writer. However we should wait for all the readers to finish
	idx.readWaitGroup.Wait()
}

// Next writes the offset value to the next sequence ID key
func (idx *LogIndex) Next(offsetValue int64) (int64, error) {
	// Write lock the index and defer the unlock
	idx.lock.Lock()
	defer idx.lock.Unlock()
	if !idx.running {
		return 0, ErrIndexClosed
	}

	// Create a entry by combining the next sequence ID with the offset value
	// The file is opened in append mode thus we need not seek to the end
	// TODO this should be made in one write to get better TX handling
	currentSequenceID := idx.nextSequenceID
	err := binary.Write(idx.writeFile, binary.BigEndian, &currentSequenceID)
	if err != nil {
		// Could not write sequence ID
		panic(err)
	}

	err = binary.Write(idx.writeFile, binary.BigEndian, &offsetValue)
	if err != nil {
		// Could not write offsetValue but we wrote the sequence ID
		// TODO we have made the index corrupt!
		panic(err)
	}

	// Increase the sequence ID for this entry
	idx.nextSequenceID++
	// Increase the file size with the size of writing one index entry
	idx.size += (keyLen + valueLen)

	// return sequcenID for the entry
	return currentSequenceID, nil
}

// GetOffset finds the offset value stored under the sequenceID key. If
// The sequence ID cannot be found then ErrSequenceIDNotFound is returned.
// If any trouble during file operations ErrIndexFileCouldNotBeOpened is
// returned
// On success the offset value is returned and nil error
func (idx *LogIndex) GetOffset(sequenceID int64) (int64, error) {
	if !idx.running {
		return 0, ErrIndexClosed
	}
	if sequenceID < 0 {
		return 0, ErrSequenceIDNotFound
	}
	// Seek the write file to the write location of the sequenceID
	// Each "row" in the index stores two int64 numbers so calculating
	// the offset for a key can be done by multiplying int64 byte length
	// times two for the row length and then times the sequenceID to
	// get the next offset for the next sequence, if we then add one
	// int64 we get the value of the offset for the given sequence ID.
	seekOffset := (keyLen+valueLen)*sequenceID + keyLen
	if seekOffset > idx.size {
		return 0, ErrSequenceIDNotFound
	}

	// Add reader to wait group
	idx.readWaitGroup.Add(1)
	defer idx.readWaitGroup.Done()

	// Create read file handle
	readFile, err := os.Open(idx.path)

	if err != nil {
		return 0, ErrIndexFileCouldNotBeOpened
	}

	// Seek to the seek offset
	_, err = readFile.Seek(seekOffset, os.SEEK_SET)

	fmt.Println("Seek offset", seekOffset)

	if err == io.EOF {
		// Searched to the end of the file and could not find the sequence
		return 0, ErrSequenceIDNotFound
	} else if err != nil {
		panic(err)
	}

	var value int64
	err = binary.Read(readFile, binary.BigEndian, &value) // Reads 8
	if err != nil {
		// Read error that is not due to file not being opened or seek error.
		// The file may be corrupt
		panic(err)
	}

	return value, nil
}
