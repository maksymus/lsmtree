package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// MetaBlock contains metadata about the SSTable, such as creation time, level, and a bloom filter.
type MetaBlock struct {
	createdAt int64
	level     int
	bloom     []byte // serialized BloomFilter; empty when not present
}

// Encode serializes the MetaBlock.
// Format: createdAt (8) | level (4) | bloomLen (4) | bloom (bloomLen bytes)
func (mb *MetaBlock) Encode() ([]byte, error) {
	buffer := bytesBufPool.Get()
	defer bytesBufPool.Put(buffer)

	if err := errors.Join(
		binary.Write(buffer, binary.BigEndian, mb.createdAt),
		binary.Write(buffer, binary.BigEndian, int32(mb.level)),
		binary.Write(buffer, binary.BigEndian, uint32(len(mb.bloom))),
	); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(mb.bloom); err != nil {
		return nil, err
	}
	return bytes.Clone(buffer.Bytes()), nil
}

func (mb *MetaBlock) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	var level int32
	var bloomLen uint32
	if err := errors.Join(
		binary.Read(reader, binary.BigEndian, &mb.createdAt),
		binary.Read(reader, binary.BigEndian, &level),
		binary.Read(reader, binary.BigEndian, &bloomLen),
	); err != nil {
		return err
	}
	mb.level = int(level)
	if bloomLen > 0 {
		mb.bloom = make([]byte, bloomLen)
		if _, err := reader.Read(mb.bloom); err != nil {
			return err
		}
	}
	return nil
}
