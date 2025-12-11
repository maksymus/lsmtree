package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Footer struct {
	meta  BlockHandle
	index BlockHandle
}

func (f *Footer) Encode() ([]byte, error) {
	buffer := pool.Get()
	defer pool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, f.meta.offset),  // Meta block offset
		binary.Write(buffer, binary.BigEndian, f.meta.length),  // Meta block length
		binary.Write(buffer, binary.BigEndian, f.index.offset), // Index block offset
		binary.Write(buffer, binary.BigEndian, f.index.length), // Index block length
	); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (f *Footer) Decode(data []byte) error {
	buffer := pool.Get()
	defer pool.Put(buffer)

	reader := bytes.NewReader(data)

	if err := errors.Join(
		binary.Read(reader, binary.BigEndian, &f.meta.offset),
		binary.Read(reader, binary.BigEndian, &f.meta.length),
		binary.Read(reader, binary.BigEndian, &f.index.offset),
		binary.Read(reader, binary.BigEndian, &f.index.length),
	); err != nil {
		return err
	}
	return nil
}
