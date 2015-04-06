package kuling

import (
	"os"
	"sync"

	"github.com/boltdb/bolt"
)

// Segment a
type Segment struct {
	lock  *sync.Locker
	index *bolt.DB
	file  *os.File
}

//
// // NewSegment loads a segment from a path.
// func NewSegment(rootPath string, sequenceID int64) *Segment {
// 	f, err := os.OpenFile(path.Join(rootPath, string(sequenceID)+".data"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	index := bolt.Open(path.Join(rootPath, string(sequenceID)+".idx"), 0600, nil)
//
// 	return &Segment{sync.Mutex{}, index, f}
// }
//
// // Close closes all references to file handles and indexes
// func (s *Segment) Close() {
// 	s.file.Close()
// }
