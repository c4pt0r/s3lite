package store

type StoreNodeDelegate struct {
	Meta string
}

func (d *StoreNodeDelegate) NodeMeta(limit int) []byte {
	if len(d.Meta) > limit {
		panic("node meta is too large")
	}
	return []byte(d.Meta)
}

func (d *StoreNodeDelegate) NotifyMsg([]byte) {
}

func (d *StoreNodeDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *StoreNodeDelegate) LocalState(join bool) []byte {
	return nil
}

func (d *StoreNodeDelegate) MergeRemoteState(buf []byte, join bool) {
}
