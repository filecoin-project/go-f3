package merkle

import (
	"io"
	"math"
	"math/bits"

	"golang.org/x/crypto/sha3"
)

// DigestLength is the length of a Digest in number of bytes.
const DigestLength = 32

// Digest is a 32-byte hash digest.
type Digest = [DigestLength]byte

// TreeWithProofs returns a the root of the merkle-tree of the given values, along with merkle-proofs for
// each leaf.
func TreeWithProofs(values [][]byte) (Digest, [][]Digest) {
	depth := depth(values)
	proofs := make([][]Digest, len(values))
	for i := range proofs {
		proofs[i] = make([]Digest, 0, depth)
	}
	return buildTree(depth, values, proofs), proofs
}

// Tree returns a the root of the merkle-tree of the given values.
func Tree(values [][]byte) Digest {
	return buildTree(bits.Len(uint(len(values))-1), values, nil)
}

// VerifyProof verifies that the given value maps to the given index in the merkle-tree with the
// given root. It returns "more" if the value is not the last value in the merkle-tree.
func VerifyProof(root Digest, index int, value []byte, proof []Digest) (valid bool, more bool) {
	// We only allow int32 items, assert that.
	if index >= math.MaxInt32 || len(proof) >= 32 {
		return false, false
	}

	// Make sure the index is in-range for the proof.
	if index > (1<<len(proof))-1 {
		return false, false
	}

	digest := leafHash(value)
	for i, uncle := range proof {
		if index&(1<<i) == 0 {
			// if we go left and the right-hand value is non-zero, we're there are
			// more values in the tree beyond the current index.
			more = more || uncle != (Digest{})
			digest = internalHash(digest, uncle)
		} else {
			digest = internalHash(uncle, digest)
		}
	}
	return root == digest, more
}

var internalMarker = []byte{0}
var leafMarker = []byte{1}

// returns the depth (path length) of a merkle-tree with the given values.
func depth(values [][]byte) int {
	return bits.Len(uint(len(values)) - 1)
}

// returns the keccak256 hash of the given values concatenated.
func hash(values ...[]byte) (out Digest) {
	hash := sha3.NewLegacyKeccak256()
	for _, value := range values {
		_, _ = hash.Write(value)
	}
	// Call `Read` instead of `Sum` to avoid some copying and allocations. Idea borrowed from
	// go-ethereum.
	_, _ = hash.(io.Reader).Read(out[:])
	return out
}

func internalHash(left Digest, right Digest) Digest {
	return hash(internalMarker, left[:], right[:])
}

func leafHash(value []byte) Digest {
	return hash(leafMarker, value)
}

// recursively builds a tree at the given depth with the given values.
//
//   - panics if there are too many values at the given depth.
//   - safely handles cases where there are too few values (unbalanced trees).
//   - if proofs is passed, it will be filled with values (must be the same length as the values
//     slice).
func buildTree(depth int, values [][]byte, proofs [][]Digest) Digest {
	if len(values) == 0 {
		return Digest{}
	} else if depth == 0 {
		if len(values) != 1 {
			panic("expected one value at the leaf")
		}
		return leafHash(values[0])
	}

	split := min(1<<(depth-1), len(values))

	var leftProofs, rightProofs [][]Digest
	if len(proofs) > 0 {
		leftProofs = proofs[:split]
		rightProofs = proofs[split:]
	}

	leftHash := buildTree(depth-1, values[:split], leftProofs)
	rightHash := buildTree(depth-1, values[split:], rightProofs)

	for i, proof := range leftProofs {
		leftProofs[i] = append(proof, rightHash)
	}
	for i, proof := range rightProofs {
		rightProofs[i] = append(proof, leftHash)
	}

	return internalHash(leftHash, rightHash)
}
