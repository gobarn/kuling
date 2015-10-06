package kuling

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

var (
	// ErrShardIllegalKey returned when the key is not set
	ErrShardIllegalKey = errors.New("shard: illegal key")
	// ErrShardIllegalPayload returned when the payload is not set
	ErrShardIllegalPayload = errors.New("shard: illegal key")
	// ErrShardIllegalStartSequenceID returned when start sequence is negative
	ErrShardIllegalStartSequenceID = errors.New("shard: illegal start sequence ID")
	// ErrShardStartSequenceIDNotFound returned when start sequence is not found
	// which could be that the sequence ID is bigger than the shard
	ErrShardStartSequenceIDNotFound = errors.New("shard: start sequence ID not found")
	// ErrShardIllegalMaxMessages returned when max messages is negative
	ErrShardIllegalMaxMessages = errors.New("shard: illegal start max messages")
)

// Shard file system shards. Keeps a zero based index for the shard that
// spans all segments. Knows the active segment where writes are going
type Shard struct {
	dir string
	// index that spans all segments with sequence ID to offset mapping
	index *LogIndex
	// array of segments
	segments []*Segment
	// active segment
	activeSegment *Segment
	// segment max size
	segmentMaxByteSize int64
	// data files and directories permissions
	permDirectories, permData os.FileMode
	// mutex for writes, reads do not use this mutex
	wlock *sync.Mutex
}

// OpenShard opens or creates a shard from the file path
func OpenShard(dir string, segmentMaxByteSize int64, permDirectories, permData os.FileMode) (*Shard, error) {
	// Check that the shard directory exist, if not then create the directory
	stat, err := os.Stat(dir)
	if err != nil || !stat.IsDir() {
		log.Printf("shard: creating shard directory %s", dir)
		err := os.Mkdir(dir, permDirectories)

		if err != nil {
			return nil, fmt.Errorf("shard: could not create shard directory %s: %s", dir, err)
		}
	}

	index, err := OpenIndex(path.Join(dir, "shard.idx"), permData)
	if err != nil {
		return nil, fmt.Errorf("shard: could not open shard index file: %s", err)
	}

	var segments []*Segment

	// Load segment files, important that we load them in correct order
	// such that the first segment file is loaded first.
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("shard: could not open shard dir %s: %s", dir, err)
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if !strings.HasSuffix(f.Name(), ".seg") {
			continue
		}

		segment, err := OpenSegment(path.Join(dir, f.Name()), permData)
		if err != nil {
			return nil, fmt.Errorf("shard: could not load segment file(s): %s\n", err)
		}

		segments = append(segments, segment)
	}

	if len(segments) == 0 {
		// If no segments found then this is a new shard, create the initial
		// segment file
		segment, err := OpenSegment(path.Join(dir, createSegmentName(1)), permData)
		if err != nil {
			log.Printf("shard: could not load segment file(s): %s", err)
			return nil, err
		}

		segments = append(segments, segment)
	}

	return &Shard{
			dir,
			index,
			segments,
			segments[len(segments)-1],
			segmentMaxByteSize,
			permDirectories,
			permData,
			&sync.Mutex{},
		},
		nil
}

// Append key and payload message
func (s *Shard) Append(key, payload []byte) error {
	if len(key) == 0 {
		return ErrShardIllegalKey
	}
	if len(key) == 0 {
		return ErrShardIllegalPayload
	}
	// Acquire and release lock after append is done
	s.wlock.Lock()
	defer s.wlock.Unlock()

	if s.activeSegment.Size() > s.segmentMaxByteSize {
		segmentName := path.Join(s.dir, createSegmentName(len(s.segments)+1))
		newSegment, err := OpenSegment(segmentName, s.permData)
		if err != nil {
			// Could not create shard, most likely due to out of disk or permissions
			// in segment directory has changed from the outside
			return fmt.Errorf("shard: %s", err)
		}
		s.segments = append(s.segments, newSegment)
		s.activeSegment = newSegment
	}

	// Get next sequenceID from index
	sequenceID, err := s.index.Next(int64(len(s.segments)-1), s.activeSegment.Size())
	if err != nil {
		return err
	}

	// Create message from key and payload
	m := NewMessage(sequenceID, key, payload)
	// Append the message to the active segment
	err = s.activeSegment.Append(m)
	if err != nil {
		// TODO: Not an ideal situation where we could not append the message to the
		// active segment and have already commited the next sequenceID meaning that
		// the id will not be used and may be confusing or cause errors down the line
		return fmt.Errorf("shard: warn: could not append message to active segment. Index id %d will be empty: %s", sequenceID, err)
	}

	return nil
}

// Read messages starting from start sequence ID and max number of messages
// forwards
func (s *Shard) readAction(startSequenceID, maxMessages int64, action func(startOffset, endOffset int64, segment *Segment) error) error {
	if startSequenceID < 0 {
		return ErrShardIllegalStartSequenceID
	}
	if maxMessages < 0 {
		return ErrShardIllegalMaxMessages
	}

	// Get offset from index
	segmentNumber, startOffset, err := s.index.SegmentAndOffset(startSequenceID)
	if err == ErrSequenceIDNotFound {
		// Could not find start offset
		return ErrShardStartSequenceIDNotFound
	} else if err != nil {
		return err
	}

	segment := s.segments[segmentNumber]

	if err != nil {
		return errors.New("shard: could not find segment for start sequence ID, have the file been removed?")
	}

	_, endOffset, err := s.index.SegmentAndOffset(startSequenceID + maxMessages)
	if err == ErrSequenceIDNotFound {
		// Fewer messages than max messages in shard, take the whole shard
		err = nil
		// grab the entire segment
		endOffset = segment.Size()
	} else if err != nil {
		return err
	}

	return action(startOffset, endOffset, segment)
}

// Read messages starting from start sequence ID and max number of messages
// forwards
func (s *Shard) Read(startSequenceID, maxMessages int64) ([]*Message, error) {
	var messages []*Message

	err := s.readAction(startSequenceID, maxMessages, func(startOffset, endOffset int64, segment *Segment) error {
		// read and pars into messages
		var err error
		messages, err = segment.Read(startOffset, endOffset)
		return err
	})

	return messages, err
}

// Copy copies from the segment that owns the sequence ID and then takes
// max number of messages forward
func (s *Shard) Copy(startSequenceID, maxMessages int64, w io.Writer, pre PreCopy, post PostCopy) (int64, error) {
	var copied int64
	err := s.readAction(startSequenceID, maxMessages, func(startOffset, endOffset int64, segment *Segment) error {
		// Call pre copy function with the number of bytes that we should read
		pre(endOffset - startOffset)
		// read and parse into messages
		var err error
		copied, err = segment.Copy(startOffset, endOffset, w)
		// Call post copy function with the actual number of bytes copied
		post(copied)
		return err
	})

	return copied, err
}

// Size returns the total size of all segments
func (s *Shard) Size() int64 {
	var total int64
	for _, segment := range s.segments {
		total += segment.Size()
	}

	return total
}

// Close down the shard
func (s *Shard) Close() error {
	for _, p := range s.segments {
		p.Close()
	}

	return nil
}

// String from stringer interface
func (s *Shard) String() string {
	return fmt.Sprintf("path: %s segments: %d size: %d", s.dir, len(s.segments), s.Size())
}

// Create segment name from the
func createSegmentName(segmentNumber int) string {
	return fmt.Sprintf("%011d.seg", segmentNumber)
}
