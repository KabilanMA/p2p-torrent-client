// Package coding provides GF(2^8) arithmetic and Random Linear Network Coding
// (RLNC) primitives for use in the BitTorrent download engine.
//
// GF(2^8) uses the Rijndael irreducible polynomial x^8+x^4+x^3+x+1 (0x11b),
// the same field as AES. Addition is XOR; multiplication uses a precomputed
// 256×256 lookup table built via Russian-peasant multiplication.
//
// Reference: Fragouli & Soljanin, "Network Coding Fundamentals", 2007.
package coding

// mulTable[a][b] = a ⊗ b  in GF(2^8)
var mulTable [256][256]byte

// logTable / expTable for inversion: a^{-1} = exp[(255 - log[a]) % 255]
var logTable [256]byte
var expTable [512]byte // doubled to avoid modular wrap in hot paths

func init() {
	// Build exp/log tables using the primitive element g=2 (0x02) under 0x11b.
	x := byte(1)
	for i := 0; i < 255; i++ {
		expTable[i] = x
		logTable[x] = byte(i)
		// multiply x by 2 in GF(2^8): shift left, reduce by 0x1b if bit 7 was set
		high := x & 0x80
		x <<= 1
		if high != 0 {
			x ^= 0x1b // reduction polynomial (low 8 bits of 0x11b)
		}
	}
	// Mirror into second half so we can index without mod
	for i := 0; i < 255; i++ {
		expTable[255+i] = expTable[i]
	}

	// Populate multiplication table.
	mulTable[0] = [256]byte{} // 0 * anything = 0
	for a := 1; a < 256; a++ {
		for b := 1; b < 256; b++ {
			mulTable[a][b] = expTable[int(logTable[a])+int(logTable[b])]
		}
	}
}

// Mul returns a * b in GF(2^8).
func Mul(a, b byte) byte { return mulTable[a][b] }

// Add returns a + b in GF(2^8) (identical to XOR).
func Add(a, b byte) byte { return a ^ b }

// Inv returns the multiplicative inverse of a in GF(2^8).
// Inv(0) is defined as 0 (caller must avoid division by zero).
func Inv(a byte) byte {
	if a == 0 {
		return 0
	}
	return expTable[255-int(logTable[a])]
}

// Div returns a / b = a * b^{-1} in GF(2^8).
// Div(a, 0) returns 0.
func Div(a, b byte) byte { return Mul(a, Inv(b)) }

// VecMulAdd computes dst[i] ^= scalar * src[i] for each byte in-place.
// This is the inner loop of Gaussian elimination and RLNC encoding.
func VecMulAdd(dst, src []byte, scalar byte) {
	if scalar == 0 {
		return
	}
	row := mulTable[scalar]
	for i, s := range src {
		dst[i] ^= row[s]
	}
}
