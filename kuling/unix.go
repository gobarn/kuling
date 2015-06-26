package kuling

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// The max mmap size, depends on OS
const maxMapSize = 0xFFFFFFFFFFFF // 256TB

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB

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
	file    *os.File
	dataref []byte // mmap'ed readonly, write throws SEGV
	data    *[maxMapSize]byte
	*sync.Mutex
}

// Creata new memory mapped from file and of max Size
func newMemoryMapped(f *os.File) *memoryMapped {
	return &memoryMapped{f, nil, nil, &sync.Mutex{}}
}

func (mm *memoryMapped) mmap() error {
	// Lock mmap lock
	mm.Lock()
	defer mm.Unlock()

	info, err := mm.file.Stat()
	if err != nil {
		fmt.Println("mmap stat error: ", err)
		return nil
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())

	if size <= 0 {
		return nil
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	if err := mm.file.Truncate(int64(size)); err != nil {
		return fmt.Errorf("file resize error: %s", err)
	}
	if err := mm.file.Sync(); err != nil {
		return fmt.Errorf("file sync error: %s", err)
	}

	// munmap any existing data first.
	err = mm.munmap()

	if err != nil {
		// Could not munmap
		fmt.Errorf("failed to munmap: %s", err)
		return err
	}

	// Map the data file to memory with read only perm
	b, err := syscall.Mmap(int(mm.file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	// Set mmaped reference data
	mm.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	mm.dataref = b

	return nil
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
	return err
}
