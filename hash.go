package alosmap

import (
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
		return seed ^ hashSeed0
	}

	base := unsafe.Pointer(unsafe.StringData(input))
	acc := seed ^ (uint64(length) * hashSeed0)

	if length <= 8 {
		var lo, hi uint32
		if length >= 4 {
			lo = readUint32(base, 0)
			hi = readUint32(base, length-4)
		} else {
			lo = uint32(*(*byte)(base))
			lo |= uint32(*(*byte)(unsafe.Add(base, length>>1))) << 8
			lo |= uint32(*(*byte)(unsafe.Add(base, length-1))) << 16
			hi = 0
		}
		return mix(acc^uint64(lo)^hashSeed1, uint64(hi)^hashSeed2)
	}

	if length <= 16 {
		return mix(acc^readUint64(base, 0)^hashSeed1, readUint64(base, length-8)^hashSeed2)
	}

	index := 0
	for length-index >= 16 {
		acc ^= mix(readUint64(base, index)^hashSeed1, readUint64(base, index+8)^hashSeed2)
		acc = bits.RotateLeft64(acc, 27)*hashSeed0 + hashSeed3
		index += 16
	}

	acc ^= mix(readUint64(base, length-16)^hashSeed2, readUint64(base, length-8)^hashSeed3)
	return avalanche(acc)
}

func readUint32(base unsafe.Pointer, offset int) uint32 {
	return *(*uint32)(unsafe.Add(base, offset))
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
	return *(*uint64)(unsafe.Add(base, offset))
}

func hashInt64(seed uint64, key int64) uint64 {
	return mix(seed^uint64(key)^hashSeed0, uint64(key)^hashSeed1)
}

func hashKey(seed uint64, k Key) uint64 {
	if k.isInt {
		return hashInt64(seed, k.i)
	}
	return hashString(seed, k.s)
}
