package coding

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// Generation groups G consecutive pieces for RLNC encoding / decoding.
// A coded block is a random linear combination of the G source pieces over
// GF(2^8): coded = Σ coeff[i] * piece[i].
//
// Any G linearly independent coded blocks suffice to recover all G originals
// via Gaussian elimination, making the scheme MDS-optimal.
//
// Reference: Ho et al. "A Random Linear Network Coding Approach to Multicast",
// IEEE Trans. Inf. Theory 2006; survey Esposito et al. JNCA 2024.
type Generation struct {
	G         int // number of source pieces
	BlockSize int // bytes per piece
}

// CodedBlock is one linear combination of the G source pieces.
type CodedBlock struct {
	Coeffs []byte // G coefficients over GF(2^8)
	Data   []byte // BlockSize payload bytes
}

// Encoder produces random coded blocks from a set of G source pieces.
type Encoder struct {
	gen    *Generation
	pieces [][]byte // G source pieces, each BlockSize bytes
}

// NewEncoder creates an Encoder from g source pieces (all must be BlockSize bytes).
func NewEncoder(gen *Generation, pieces [][]byte) (*Encoder, error) {
	if len(pieces) != gen.G {
		return nil, fmt.Errorf("coding: expected %d pieces, got %d", gen.G, len(pieces))
	}
	for i, p := range pieces {
		if len(p) != gen.BlockSize {
			return nil, fmt.Errorf("coding: piece %d has length %d, want %d", i, len(p), gen.BlockSize)
		}
	}
	return &Encoder{gen: gen, pieces: pieces}, nil
}

// Encode returns a new random coded block. Each call draws fresh random
// coefficients, so successive blocks are independent with overwhelming
// probability (failure prob ≤ G/256 per block ≈ negligible for G ≤ 64).
func (enc *Encoder) Encode() (*CodedBlock, error) {
	cb := &CodedBlock{
		Coeffs: make([]byte, enc.gen.G),
		Data:   make([]byte, enc.gen.BlockSize),
	}
	if _, err := rand.Read(cb.Coeffs); err != nil {
		return nil, fmt.Errorf("coding: random coefficients: %w", err)
	}
	for i, coeff := range cb.Coeffs {
		VecMulAdd(cb.Data, enc.pieces[i], coeff)
	}
	return cb, nil
}

// Decoder accumulates coded blocks and recovers the G source pieces once
// G linearly independent blocks have been received. It uses in-place
// Gaussian elimination over GF(2^8) (systematic, with partial pivoting).
type Decoder struct {
	gen      *Generation
	matrix   [][]byte // G rows of (G coefficients | BlockSize data), row-reduced
	pivotCol []int    // pivotCol[r] = leading column of row r (-1 if not yet set)
	rank     int      // number of linearly independent rows so far
}

// NewDecoder creates a Decoder for the given generation.
func NewDecoder(gen *Generation) *Decoder {
	rowLen := gen.G + gen.BlockSize
	matrix := make([][]byte, gen.G)
	for i := range matrix {
		matrix[i] = make([]byte, rowLen)
	}
	pivotCol := make([]int, gen.G)
	for i := range pivotCol {
		pivotCol[i] = -1
	}
	return &Decoder{gen: gen, matrix: matrix, pivotCol: pivotCol}
}

// Add incorporates one coded block. Returns true if it was linearly independent
// (rank increased). Ignores duplicates / linearly dependent blocks.
func (d *Decoder) Add(cb *CodedBlock) (bool, error) {
	if len(cb.Coeffs) != d.gen.G || len(cb.Data) != d.gen.BlockSize {
		return false, errors.New("coding: block dimensions mismatch")
	}
	if d.rank == d.gen.G {
		return false, nil // already fully decoded
	}

	// Copy block into a working row [coeffs | data].
	G := d.gen.G
	row := make([]byte, G+d.gen.BlockSize)
	copy(row[:G], cb.Coeffs)
	copy(row[G:], cb.Data)

	// Forward reduce: eliminate columns that already have pivot rows.
	for r := 0; r < d.rank; r++ {
		col := d.pivotCol[r]
		if row[col] == 0 {
			continue
		}
		scalar := row[col]
		VecMulAdd(row, d.matrix[r], scalar)
	}

	// Find the first non-zero coefficient in the reduced row.
	pivot := -1
	for c := 0; c < G; c++ {
		if row[c] != 0 {
			pivot = c
			break
		}
	}
	if pivot == -1 {
		return false, nil // linearly dependent
	}

	// Normalise the pivot to 1.
	scale := Inv(row[pivot])
	for i := range row {
		row[i] = Mul(row[i], scale)
	}

	// Back-substitute into existing pivot rows.
	for r := 0; r < d.rank; r++ {
		if d.matrix[r][pivot] == 0 {
			continue
		}
		VecMulAdd(d.matrix[r], row, d.matrix[r][pivot])
	}

	// Store the new pivot row.
	d.matrix[d.rank] = row
	d.pivotCol[d.rank] = pivot
	d.rank++
	return true, nil
}

// Done reports whether G independent blocks have been collected.
func (d *Decoder) Done() bool { return d.rank == d.gen.G }

// Decode recovers the G source pieces. Returns an error if fewer than G
// independent blocks have been received.
func (d *Decoder) Decode() ([][]byte, error) {
	if d.rank < d.gen.G {
		return nil, fmt.Errorf("coding: need %d blocks, have %d", d.gen.G, d.rank)
	}
	G := d.gen.G
	// The matrix is in reduced row-echelon form. Row r has its pivot at
	// pivotCol[r]; that column index is the original piece index.
	pieces := make([][]byte, G)
	for r := 0; r < G; r++ {
		col := d.pivotCol[r]
		if col < 0 || col >= G {
			return nil, errors.New("coding: internal decoder error: bad pivot")
		}
		pieces[col] = make([]byte, d.gen.BlockSize)
		copy(pieces[col], d.matrix[r][G:])
	}
	return pieces, nil
}
