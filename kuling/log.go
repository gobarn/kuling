package kuling

//
// // Log a
// type Log struct {
// 	Segments          []Segment
// 	CurrentSequenceID int64
// }
//
// // NewLog creates a Log struct
// func NewLog(root string) *Log {
// 	return nil
// }
//
// // NextSequence ID generates the next sequence ID
// func (l *Log) generateSequenceID() int64 {
// 	return l.CurrentSequenceID + 1
// }
//
// // getActiveSegments returns the currently active segments where writes are
// // going
// func (l *Log) getActiveSegment() (*Segment, error) {
// 	return &l.Segments[len(l.Segments)-1], nil
// }
//
// // returns the segment that contin the sequence id, if the sequence id is
// // less than zero then error or if the sequence id is greater than the
// // last sequence id of the log then error.
// func (l *Log) segmentForSequenceID(sequenceID int64) (*Segment, error) {
// 	if sequenceID < 0 {
// 		return nil, errors.New("SequenceID negative")
// 	}
//
// 	var containingSegment Segment
//
// 	for i, s := range l.Segments {
// 		if s.StartSequenceID > sequenceID && s.EndSequenceID < sequenceID {
// 			return &s, nil
// 		}
// 	}
//
// 	return &containingSegment, errors.New("SequenceID not contained in any Segment")
// }
//
// // Write writes the key and payload to the active segment and returns
// // back the sequence id for the message
// func (l *Log) Write(m *Message) (int64, error) {
// 	// 1 Get File handle for the file
// 	nextSequenceID := l.generateSequenceID()
// 	s, err := l.segmentForSequenceID(nextSequenceID)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	// Write the message to the segment file
// 	err = s.Write(nextSequenceID, m)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	// Store the offset of the sequence ID in the index
// 	return nextSequenceID, nil
// }
