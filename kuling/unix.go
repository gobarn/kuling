package kuling

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const maxMapSize = 0xFFFFFFFFFFFF // 256TB

// Taken from the magnificant project BoltDB:
// https://github.com/boltdb/bolt
// flock acquires an advisory lock on a file descriptor.
func flock(f *os.File, timeout time.Duration) error {
	var t time.Time
	for {
		// If we're beyond our timeout then return an error.
		// This can only occur after we've attempted a flock once.
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}

		// Otherwise attempt to obtain an exclusive lock.
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// Wait for a bit and try again.
		time.Sleep(50 * time.Millisecond)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// MemoryMapped A memory mapped struct that contain the memory mapped file
// information that's needed to do operations on the mmaped byte slice and also
// to unmap data.
type memoryMapped struct {
	dataref []byte
	data    *[maxMapSize]byte
	datasz  int
	*sync.Mutex
}

// mmapFromFile returns a mmap struct with data that is the mmaped version of
// the file
func mmapFromFile(file *os.File, sz int) (*memoryMapped, error) {
	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if err := file.Truncate(int64(sz)); err != nil {
		return nil, fmt.Errorf("file resize error: %s", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("file sync error: %s", err)
	}

	// Map the data file to memory with read only perm
	b, err := syscall.Mmap(int(file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Save the original byte slice and convert to a byte array pointer.
	return &memoryMapped{b, (*[maxMapSize]byte)(unsafe.Pointer(&b[0])), sz, &sync.Mutex{}}, nil
}

// munmap unmaps a MM's data file from memory.
func (mm *memoryMapped) munmap() error {
	// Ignore the unmap if we have no mapped data.
	if mm.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := syscall.Munmap(mm.dataref)
	mm.dataref = nil
	mm.data = nil
	mm.datasz = 0
	return err
}
