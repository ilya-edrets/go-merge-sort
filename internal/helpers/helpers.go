package helpers

import "encoding/binary"

func ToBytes(buf []byte, i int) {
	binary.BigEndian.PutUint32(buf, uint32(i))
}

func ToInt(buf []byte) int {
	return int(binary.BigEndian.Uint32(buf))
}
