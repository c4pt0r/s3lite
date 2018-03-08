package store

import "bytes"

type Needle struct {
	ID         uint64
	NeedleSize uint32

	DataSize uint32
	Data     []byte
	CheckSum uint32 // crc32

	Padding []byte // padding to 8 bytes
}

func (n *Needle) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	buf.Write(Uint32ToBytes(n.DataSize))
	buf.Write(n.Data)
	buf.Write(Uint32ToBytes(n.CheckSum))
	dataBytes := buf.Bytes()

	needleBuf := bytes.NewBuffer(nil)
	needleBuf.Write(Uint64ToBytes(n.ID))
	needleBuf.Write(Uint32ToBytes(uint32(len(dataBytes))))
	needleBuf.Write(dataBytes)

	return needleBuf.Bytes()
}
