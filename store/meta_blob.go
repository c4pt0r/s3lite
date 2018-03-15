package store

import (
	"encoding/binary"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	STORE_FLAG_READ_ONLY = 1 << iota
)

// fixed size meta
type MetaBlob struct {
	Version uint16
	MaxSize uint64
	Flags   uint32
	StoreID [64]byte
}

func (m *MetaBlob) Bytes() []byte {
	buf := make([]byte, MetaBlobSize)
	binary.LittleEndian.PutUint16(buf, m.Version)
	binary.LittleEndian.PutUint64(buf[2:10], m.MaxSize)
	binary.LittleEndian.PutUint32(buf[10:14], m.Flags)
	copy(buf[14:], m.StoreID[:])
	return buf
}

func (m *MetaBlob) SetFlag(flag int) {
	m.Flags |= uint32(flag)
}

func (m *MetaBlob) FromBytes(buf []byte) error {
	if len(buf) != MetaBlobSize {
		return errors.New("invalid meta blob")
	}
	m.Version = binary.LittleEndian.Uint16(buf[0:2])
	m.MaxSize = binary.LittleEndian.Uint64(buf[2:10])
	m.Flags = binary.LittleEndian.Uint32(buf[10:14])
	copy(m.StoreID[:], buf[14:])
	return nil
}

func (m *MetaBlob) SetID(ID string) {
	// update meta blob
	if len(ID) > 64 {
		log.Error("store name should not exceed 64 bytes")
		return
	}
	copy(m.StoreID[0:], []byte(ID))
}

func (m MetaBlob) ID() string {
	var buf []byte
	for _, c := range m.StoreID {
		if c == 0x0 {
			break
		}
		buf = append(buf, c)
	}
	return string(buf)
}
