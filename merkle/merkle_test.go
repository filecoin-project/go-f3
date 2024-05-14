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
			}
		})
	}
}

func TestHashZero(t *testing.T) {
	test := [][]byte{}
	root, paths := TreeWithProofs(test)
	assert.Empty(t, paths)
	assert.Equal(t, root, Digest{})
}
