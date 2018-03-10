package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaBlob(t *testing.T) {
	m := new(MetaBlob)
	copy(m.StoreID[0:], []byte("hello"))
	m.MaxSize = DefaultStoreSize
	m.Version = 1

	buf := m.Bytes()
	assert.Equal(t, len(buf), MetaBlobSize)

	m1 := new(MetaBlob)
	m1.FromBytes(buf)
	assert.EqualValues(t, m1.Version, 1)
	assert.EqualValues(t, m1.ID(), "hello")
}

func TestCreateStore(t *testing.T) {
	s := new(Store)

	copy(s.MetaBlob.StoreID[0:], []byte("hello"))
	s.MetaBlob.MaxSize = DefaultStoreSize
	s.MetaBlob.Version = 1

	err := s.Open("hello.dat", true)
	assert.Nil(t, err)

	defer func() {
		os.Remove("hello.dat")
	}()

	s1 := new(Store)
	err = s1.Open("hello.dat", false)
	assert.Nil(t, err)

	assert.EqualValues(t, s1.MetaBlob.ID(), "hello")
	assert.EqualValues(t, s1.MetaBlob.MaxSize, DefaultStoreSize)
	assert.EqualValues(t, s1.MetaBlob.Version, 1)
}

func TestWriteNeedle(t *testing.T) {
	n := NewNeedle(100, []byte("foobar"))

	s := new(Store)
	s.Open("hello-readonly.dat", true)

	defer func() {
		os.Remove("hello-readonly.dat")
	}()

	offset, sz, _ := s.WriteNeedle(n, false)
	nn, err := s.ReadNeedleWithOffsetAndSize(offset, sz)
	assert.Nil(t, err)
	assert.EqualValues(t, n, nn)

	nn1, err := s.ReadNeedleAt(offset)
	assert.Nil(t, err)
	assert.EqualValues(t, n, nn1)

	err = s.SetReadOnly()
	assert.Nil(t, err)

	_, _, err = s.WriteNeedle(n, false)
	assert.NotNil(t, err)
	fmt.Println(err.Error())
}
