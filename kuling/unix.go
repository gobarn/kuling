package kuling

import (
	"os"
	"syscall"
	"time"
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
