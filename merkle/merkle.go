package merkle

import (
	"fmt"
	"io"
	"math"
	"math/bits"

	"golang.org/x/crypto/sha3"
)

// DigestLength is the length of a Digest in number of bytes.
const DigestLength = 32

// Digest is a 32-byte hash digest.
type Digest = [DigestLength]byte

var ZeroDigest Digest

// TreeWithProofs returns a the root of the merkle-tree of the given values, along with merkle-proofs for
// each leaf.
func TreeWithProofs(values [][]byte) (Digest, [][]Digest) {
	depth := depth(len(values))
	proofs := make([][]Digest, len(values))
	for i := range proofs {
		proofs[i] = make([]Digest, 0, depth)
	}
	return buildTree(depth, values, proofs), proofs
}

// Tree returns a the root of the merkle-tree of the given values.
func Tree(values [][]byte) Digest {
	return buildTree(depth(len(values)), values, nil)
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
func depth(length int) int {
	return bits.Len(uint(length) - 1)
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

// Key for memoization map
type memoKey struct {
	depth      int
	startIndex int
	endIndex   int // endIndex is needed because padding depends on it
}

// BatchTree creates a batch of prefixes of values: [[0], [0, 1], [0, 1, 2], ...]
// and provides digests for all of them.
func BatchTree(values [][]byte) []Digest {
	// this implementation uses memoization to optimize the computation
	n := len(values)
	if n == 0 {
		return []Digest{}
	}
	roots := make([]Digest, n+1) // roots[0] is unused (or holds zeroDigest)

	memo := make(map[memoKey]Digest)

	// buildTreeMemoized computes the Merkle root for values[startIndex:endIndex]
	// at the specified targetDepth
	var buildTreeMemoized func(targetDepth int, startIndex int, endIndex int) Digest
	buildTreeMemoized = func(targetDepth int, startIndex int, endIndex int) Digest {
		numValues := endIndex - startIndex
		if numValues == 0 {
			// Base case: No values for this branch, return zero digest (padding)
			return Digest{}
		}
		if targetDepth == 0 {
			// Base case: Leaf node depth
			if numValues != 1 {
				// this should not happen if initial depth calculation is correct
				panic(fmt.Sprintf("buildTreeMemoized: targetDepth 0 but values count %d != 1", numValues))
			}
			// compute leaf hash directly (memoizing leaves adds overhead with little benefit)
			return leafHash(values[startIndex])
		}

		key := memoKey{targetDepth, startIndex, endIndex}
		if cachedDigest, ok := memo[key]; ok {
			return cachedDigest
		}

		// Calculate split point based on the capacity of the left subtree at targetDepth-1
		leftCapacity := 1 << (targetDepth - 1)
		// Determine the actual index where the split occurs within values[startIndex:endIndex]
		splitIndex := startIndex + leftCapacity
		if splitIndex > endIndex {
			splitIndex = endIndex // Don't split beyond available values
		}

		leftHash := buildTreeMemoized(targetDepth-1, startIndex, splitIndex)
		rightHash := buildTreeMemoized(targetDepth-1, splitIndex, endIndex) // Handles padding

		result := internalHash(leftHash, rightHash)
		memo[key] = result
		return result
	}

	for k := 1; k <= n; k++ {
		depthForPrefixK := 0
		if k > 1 {
			depthForPrefixK = depth(k)
		} else {
			depthForPrefixK = 0 // Single leaf tree has depth 0
		}

		if k == 1 {
			roots[k] = leafHash(values[0])
			continue
		}

		// determine the split point (size of the left subtree) based on the depth
		// the left subtree corresponds to a perfect tree of depth `depthForPrefixK - 1`
		splitSize := 1 << (depthForPrefixK - 1) // size = 2^(depth-1)

		// reuse the root of the left subtree (size `splitSize`) computed earlier.
		// this root `roots[splitSize]` was computed to the correct depth.
		leftRoot := roots[splitSize]

		// compute the root of the right subtree using the memoized function.
		// values: values[splitSize:k], Target Depth: depthForPrefixK - 1
		rightRoot := buildTreeMemoized(depthForPrefixK-1, splitSize, k)

		roots[k] = internalHash(leftRoot, rightRoot)
	}

	return roots[1:]
}
