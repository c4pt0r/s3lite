package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

type Needle struct {
	ID       uint64
	Flags    uint32
	Data     []byte
	CheckSum uint32 // crc32
}

func NewNeedle(ID uint64, data []byte) *Needle {
	n := &Needle{
		ID:   ID,
		Data: data,
	}
	h := crc32.NewIEEE()
	h.Write(data)
	n.CheckSum = h.Sum32()
	return n
}

//payload:  | id (8bytes) | data size (4 bytes) | data | checksum (4bytes)
func (n *Needle) FromPayload(b []byte) error {
	if len(b) < 16 {
		return errors.New("invalid needle header")
	}
	// read 64 bits ID
	ID := binary.LittleEndian.Uint64(b[0:8])
	// read payload size
	flags := binary.LittleEndian.Uint32(b[8:12])
	dataSize := binary.LittleEndian.Uint32(b[12:16])
	if dataSize+16 > uint32(len(b)) {
		fmt.Println(dataSize+16, len(b))
		return errors.New("invalid needle payload")
	}

	n.ID = ID
	n.Flags = flags
	// should we copy here?
	n.Data = b[16 : 16+dataSize]
	n.CheckSum = binary.LittleEndian.Uint32(b[16+dataSize:])

	h := crc32.NewIEEE()
	h.Write(n.Data)
	if n.CheckSum != h.Sum32() {
		return errors.New("invalid data blob, checksum mismatch")
	}
	return nil
}

func (n *Needle) Bytes() []byte {
	buf := bytes.NewBuffer(nil)

	buf.Write(Uint64ToBytes(n.ID))
	buf.Write(Uint32ToBytes(n.Flags))
	buf.Write(Uint32ToBytes(uint32(len(n.Data))))
	buf.Write(n.Data)

	h := crc32.NewIEEE()
	h.Write(n.Data)
	buf.Write(Uint32ToBytes(h.Sum32()))

	return buf.Bytes()
}
