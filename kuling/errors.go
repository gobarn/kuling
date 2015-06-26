package kuling

import "errors"

var (
	// ErrChecksum when a message checksum is not correct
	ErrChecksum = errors.New("checksum error")

	// ErrTimeout when a timeout occurs
	ErrTimeout = errors.New("timeout")
)
