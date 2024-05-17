package merkle

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashTree(t *testing.T) {
	for i := 1; i < 256; i++ {
		t.Run(fmt.Sprintf("Length/%d", i), func(t *testing.T) {
			test := make([][]byte, i)
			for j := range test {
				test[j] = []byte{byte(j)}

			}
			root, paths := TreeWithProofs(test)
			root2 := Tree(test)
			require.Equal(t, root, root2)
			require.Equal(t, len(test), len(paths))
			assert.Equal(t, len(paths[0]), depth(test))

			for i, path := range paths {
				valid, more := VerifyProof(root, i, test[i], path)
				assert.True(t, valid, "proof was not valid for index %d", i)
				assert.Equal(t, i < len(paths)-1, more, "incorrect value for 'more' for index %d", i)

				if len(path) > 0 {
					// Mutating any part of the path breaks the verification.
					for j := range path {
						path[j][5] += 1
						valid, _ := VerifyProof(root, i, test[i], path)
						assert.False(t, valid)
						path[j][5] -= 1
					}

					// Test with a right-truncated path
					valid, _ = VerifyProof(root, i, test[i], path[:len(path)-1])
					assert.False(t, valid)

					// Test with a left-truncated path
					valid, _ = VerifyProof(root, i, test[i], path[1:])
					assert.False(t, valid)
				}

				// Test with an extended path.
				valid, _ = VerifyProof(root, i, test[i], append(path, Digest{}))
				assert.False(t, valid)
			}
		})
	}
}

func TestHashZero(t *testing.T) {
	test := [][]byte{}
	root, paths := TreeWithProofs(test)
	assert.Empty(t, paths)
	assert.Equal(t, root, Digest{})

	// Bad proof
	valid, _ := VerifyProof(root, 0, nil, nil)
	assert.False(t, valid)
}
