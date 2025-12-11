package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// MetaBlock contains metadata about the SSTable, such as creation time and level in the LSM tree.
type MetaBlock struct {
	createdAt int64
	level     int
}

func (mb *MetaBlock) Encode() ([]byte, error) {
	buffer := pool.Get()
	defer pool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, mb.createdAt),    // Creation timestamp
		binary.Write(buffer, binary.BigEndian, int32(mb.level)), // Level in LSM tree
	); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (mb *MetaBlock) Decode(data []byte) error {
	buffer := pool.Get()
	defer pool.Put(buffer)

	reader := bytes.NewReader(data)

	var level int32
	if err := errors.Join(
		binary.Read(reader, binary.BigEndian, &mb.createdAt),
		binary.Read(reader, binary.BigEndian, &level),
	); err != nil {
		return err
	}
	mb.level = int(level)
	return nil
}
