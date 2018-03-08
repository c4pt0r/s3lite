package store

import (
	"encoding/binary"
	"os"

	"github.com/juju/errors"
)

type Payload struct {
	offset int64
	size   uint32
}

type Index struct {
	m map[uint64]*Payload
}

func (i *Index) Put(ID uint64, offset int64, sz uint32) {
	i.m[ID] = &Payload{offset, sz}
}

func (i *Index) Get(ID uint64) (*Payload, bool) {
	v, ok := i.m[ID]
	return v, ok
}

func (i *Index) Delete(ID uint64) {
	delete(i.m, ID)
}

func NewIndex() *Index {
	return &Index{
		m: make(map[uint64]*Payload),
	}
}

func (i *Index) Dump(filename string) error {
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return errors.Trace(err)
	}

	buf := make([]byte, 8+8+4)

	for k, v := range i.m {
		binary.LittleEndian.PutUint64(buf[0:], k)
		binary.LittleEndian.PutUint64(buf[8:], uint64(v.offset))
		binary.LittleEndian.PutUint32(buf[16:], v.size)

		// write kv pairs
		_, err = fp.Write(buf)
		if err != nil {
			return errors.Trace(err)
		}
	}
	fp.Sync()
	fp.Close()
	return nil
}

func (i *Index) Open(filepath string) error {
	return nil
}
