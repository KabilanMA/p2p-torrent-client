package bitfield

// Bitfield is a peer's availability map: bit N set means the peer has piece N.
type Bitfield []byte

// HasPiece reports whether the peer has piece at the given index.
func (bf Bitfield) HasPiece(index int) bool {
	byteIndex := index / 8
	offset := index % 8
	if byteIndex >= len(bf) {
		return false
	}
	return bf[byteIndex]>>(7-offset)&1 != 0
}

// SetPiece marks piece at index as available.
func (bf Bitfield) SetPiece(index int) {
	byteIndex := index / 8
	offset := index % 8
	if byteIndex >= len(bf) {
		return
	}
	bf[byteIndex] |= 1 << (7 - offset)
}

// Count returns how many pieces are set in the bitfield.
func (bf Bitfield) Count() int {
	n := 0
	for _, b := range bf {
		for b != 0 {
			n += int(b & 1)
			b >>= 1
		}
	}
	return n
}
