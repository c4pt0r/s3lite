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
	buf := bytes.NewBuffer()
	buf.Write(Uint32ToBytes(n.DataSize))
	buf.Write(Uint32ToBytes(n.Data))
	buf.Write(Uint32ToBytes(n.CheckSum))
}
