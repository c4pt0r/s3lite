package store

import (
	"encoding/json"

	"github.com/c4pt0r/s3lite/meta"
	log "github.com/sirupsen/logrus"
)

type StorageNodeInfo struct {
	meta.NodeInfo

	IsReadOnly bool `json:"readonly"`
}

type StoreNodeDelegate struct {
	Info *StorageNodeInfo
}

func (d *StoreNodeDelegate) NodeMeta(limit int) []byte {
	buf, _ := json.Marshal(d.Info)
	if len(buf) > limit {
		panic("node meta is too large")
	}
	log.Info("Update NodeMeta: ", string(buf))
	return buf
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
