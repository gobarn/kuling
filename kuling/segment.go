package kuling

import "os"

// Segment a
type Segment struct {
	dataFile *os.File
	index    *LogIndex
}
