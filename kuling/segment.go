package kuling

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// SegmentError error with more information about the segment
type SegmentError struct {
	Cause   error
	Segment Segment
}

func (se SegmentError) Error() string {
	return se.Cause.Error()
}

// Segment stores messages
type Segment interface {
	// Append
	Append(m *Message) error
	// Read messages from offset of file until end offset
	Read(offset, endOffset int64) ([]*Message, error)
	// Copy messages from offset of file until end offset
	Copy(offset, endOffset int64, w io.Writer) (int64, error)
	// Size returns the size in bytes of the segment
	Size() int64
}

var (
	// ErrSegmentStartOffset returned when start offset larger than file
	// or when negative
	ErrSegmentStartOffset = errors.New("segment: Start offset illegal")
	// ErrSegmentEndOffset returned when end offset larger than file
	// of when negative
	ErrSegmentEndOffset = errors.New("segment: End offset illegal")
)

// FSSegment s
type FSSegment struct {
	// File Path of the segment
	FilePath string
	// segment file handle used for writing
	whandle *os.File
	// size of segment in bytes
	size int64
}

// OpenFSSegment opens or creates a new file system segment
func OpenFSSegment(fileName string, perm os.FileMode) (*FSSegment, error) {
	// Check if the file exists, if not log that it will be created
	_, err := os.Stat(fileName)
	if err != nil {
		log.Printf("segment: Creating segment file %s", fileName)
	}
	// Open or create the segment file.
	segmentFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, perm)
	if err != nil {
		return nil, fmt.Errorf("segment: Could not open or create segment file %v", fileName)
	}

	// Lock file so that other processes cannot use the same file as that may
	// cause corruption. If we cannot lock the file after the timouet then
	// we return an error
	if err = flock(segmentFile, 1000*time.Millisecond); err != nil {
		return nil, fmt.Errorf("segment: Could not acquire file lock on segment file %v", fileName)
	}

	// Get file stat to calcualte size of segment
	fd, err := segmentFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("segment: Could not get file stats for segment file %v", fileName)
	}

	return &FSSegment{
			fileName,
			segmentFile,
			fd.Size(),
		},
		nil
}

// Append message to segment.
func (ss *FSSegment) Append(m *Message) error {
	mw := NewMessageWriter(ss.whandle)

	bytesWritten, err := mw.WriteMessage(m)
	if err != nil {
		log.Fatalf("segment: Could not write message to segment %v", ss.whandle.Name())
		return SegmentError{err, ss}
	}

	// Increment the segments size with the number of bytes written
	ss.size += bytesWritten

	return nil
}

func (ss *FSSegment) readAction(offset, endOffset int64, action func(readHandle *os.File) error) error {
	if offset > ss.size || offset < 0 {
		return SegmentError{ErrSegmentStartOffset, ss}
	}
	if endOffset > ss.size || endOffset < 0 {
		return SegmentError{ErrSegmentEndOffset, ss}
	}

	readHandle, err := os.OpenFile(ss.FilePath, os.O_RDONLY, 0500)
	if err != nil {
		log.Fatalf("segment: Could not get read file handle %v", ss.whandle.Name())
		return SegmentError{err, ss}
	}
	defer readHandle.Close()

	// Seek to the offset position
	_, err = readHandle.Seek(offset, os.SEEK_SET)
	if err != nil {
		log.Fatalf("segment: Could not seek to offset in segment %v", ss.whandle.Name())
		return SegmentError{err, ss}
	}

	return action(readHandle)
}

// Read messages from segment and parse into messages
func (ss *FSSegment) Read(offset, endOffset int64) ([]*Message, error) {
	var messages []*Message

	err := ss.readAction(offset, endOffset, func(readHandle *os.File) error {
		var err error
		mr := NewMessageReader(readHandle)
		messages, err = mr.ReadMessages()
		if err != nil {
			log.Fatalf("segment: Could not read message from segment %v", ss.whandle.Name())
			return SegmentError{err, ss}
		}

		return nil
	})

	return messages, err
}

// Copy part of segment into io writer
func (ss *FSSegment) Copy(offset, endOffset int64, w io.Writer) (int64, error) {
	var copied int64

	err := ss.readAction(offset, endOffset, func(readHandle *os.File) error {
		var err error
		copied, err = io.CopyN(w, readHandle, endOffset-offset)
		return err
	})

	return copied, err
}

// Size returns the size in bytes of the segment
func (ss *FSSegment) Size() int64 {
	return ss.size
}

// String from stringer interface
func (ss *FSSegment) String() string {
	return fmt.Sprintf("path: %s size: %d", ss.FilePath, ss.size)
}
