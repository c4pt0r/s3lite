package store

import (
	"log"
	"testing"
)

func TestWriteBlob(t *testing.T) {
	f := NewBlobFile("./test.blob")
	f.writeBlob(123, 456, 0, []byte("aaaaa"))

	n, err := f.readNeedleAt(0)
	if err != nil {
		panic(err)
	}
	log.Println(n)

}
