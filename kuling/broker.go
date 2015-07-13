package kuling

// Store for broker key persistence
type Store interface {
	Put(k, v []byte) error
	Get(k []byte) ([]byte, error)
}

// Broker s
type Broker struct {
	s Store
}

// Peers returns a list of network addresses where peer servers are running
func (b *Broker) Peers() ([]string, error) {
	return nil, nil
}

//
// // CheckOwner
// func (b *Broker) CheckOwner(topic, shard, iter string) (bool, error) {
//
// 	return true, nil
// }
//
// func (b *Broker) Validate(client, iter)
//
// // PutIterator persists a groups position for a shard of a topic.
// // the poistion shall be the sequence id of the last record read for the
// // group and topic.
// func (b *Broker) PutIterator(iter string, p int64) error {
// 	buf := new(bytes.Buffer)
// 	if err := binary.Write(buf, binary.LittleEndian, p); err != nil {
// 		return fmt.Errorf("broker: unable to convert position to persited format. %s", err)
// 	}
//
// 	k := []byte(fmt.Sprint(group, topic, shard))
// 	if err := b.s.Put(k, buf.Bytes()); err != nil {
// 		return fmt.Errorf("broker: unable to persist position in store. %s", err)
// 	}
//
// 	return nil
// }
//
// // GetIterator s
// func (b *Broker) GetIterator(group, topic, shard string) (iter string, err error) {
// 	k := []byte(fmt.Sprint(group, topic, shard))
//
// 	var v []byte
// 	if v, err = b.s.Get(k); err != nil {
// 		return 0, fmt.Errorf("broker: unable to get position for key %s. %s", string(k), err)
// 	}
//
// 	if err := binary.Read(bytes.NewReader(v), binary.BigEndian, &p); err != nil {
// 		return 0, fmt.Errorf("broker: unable to convert persisted poistion to int64. %s", err)
// 	}
//
// 	return p, nil
// }
