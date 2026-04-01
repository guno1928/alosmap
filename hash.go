package alosmap

import (
	"encoding/binary"
	"math/bits"
	"unsafe"
)

const (
	hashSeed0 = 0xa0761d6478bd642f
	hashSeed1 = 0xe7037ed1a0b428db
	hashSeed2 = 0x8ebc6af09c88c6e3
	hashSeed3 = 0x589965cc75374cc3
)

func hashString(seed uint64, input string) uint64 {
	length := len(input)
	if length == 0 {
		return avalanche(seed ^ hashSeed0)
	}

	base := unsafe.Pointer(unsafe.StringData(input))
	acc := seed ^ (uint64(length) * hashSeed0)
	index := 0

	for length-index >= 16 {
		acc ^= mix(readUint64(base, index)^hashSeed1, readUint64(base, index+8)^hashSeed2)
		acc = bits.RotateLeft64(acc, 27)*hashSeed0 + hashSeed3
		index += 16
	}

	if length-index >= 8 {
		acc ^= mix(readUint64(base, index)^hashSeed2, uint64(length)^hashSeed3)
		index += 8
	}

	var tail uint64
	shift := 0
	for ; index < length; index++ {
		tail |= uint64(readByte(base, index)) << shift
		shift += 8
	}

	acc ^= mix(tail^hashSeed1, uint64(length)^hashSeed0)
	return avalanche(acc)
}

func mix(left uint64, right uint64) uint64 {
	high, low := bits.Mul64(left, right)
	return high ^ low
}

func avalanche(value uint64) uint64 {
	value ^= value >> 32
	value *= hashSeed0
	value ^= value >> 29
	value *= hashSeed1
	value ^= value >> 32
	return value
}

func readUint64(base unsafe.Pointer, offset int) uint64 {
	bytes := unsafe.Slice((*byte)(unsafe.Add(base, offset)), 8)
	return binary.LittleEndian.Uint64(bytes)
}

func readByte(base unsafe.Pointer, offset int) byte {
	return *(*byte)(unsafe.Add(base, offset))
}
