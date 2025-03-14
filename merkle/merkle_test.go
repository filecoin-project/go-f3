package merkle

import (
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashTree(t *testing.T) {
	for i := 1; i < 256; i++ {
		t.Run(fmt.Sprintf("Length/%d", i), func(t *testing.T) {
			t.Parallel()

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

func generateInputs(dst string, N int) [][]byte {
	var res [][]byte
	for i := 0; i < N; i++ {
		res = append(res, []byte(dst+fmt.Sprintf("%09d", i)))
	}
	return res
}

func TestHashBatch(t *testing.T) {
	for N := 1; N < 300; {
		inputs := generateInputs(fmt.Sprintf("batch-%d", N), N)
		expected := make([]Digest, 0, N-1)

		for i := 0; i < N; i++ {
			expected = append(expected, Tree(inputs[0:i+1]))
		}

		batchRes := BatchTree(inputs)
		require.Equal(t, expected, batchRes, "failed at size %d", N)
		if N < 32 {
			N++
		} else {
			N += 13
		}
	}
}

func TestHashTreeGolden(t *testing.T) {
	expectedHex := []string{
		"3d4395573ce4d2acbce4fe8a4be67ca5e7cdfb8ee2e85b2f6733c16b24c3b175",
		"91b7c899421ca7f3228e10265c6970a03bc2ccba44367b1d44a9d8597b20a32e",
		"69abe78dc2390b4666b60d0582e1799e73e48766f6e502c515e79d6cd2ae3c45",
		"bc4ce8dbf993eb2e87c02bbf19cd4faeb3a0672188bc6be6c8d867cef9b08917",
		"538cfd0c1f6b7ab4c3d20466d4e01b438972212fe5257eae213ae0a040da977f",
		"e28aa108b0263820dfe2c7f051ddc8794ab48ebd3c1813db28bf9f06bedc52f3",
		"875cb1d5027522b344b8adc62cd6bd110d97eaedd40a35bcb2fe142a9cb4612b",
		"63804e8b6cb16993d5d43d9d7faf17ba967365dac141a4afbce1d794157a1b8e",
		"07105bd8716bebc90036c8ebfe23a92bd775c09664b076ffa1d9a29d30647f91",
		"960b7eb6440789f76f5d53965e8b208e34777bc4aab78edf6827d71c7eea4933",
		"d55e07222c786722e1ad1b5bcc2ebaf04b2b4e92c07f3f7b61b0fbf0fd78fb9b",
		"ee5a34dfae748e088a1b99386274158266f44ceeb2c5190f4e9bbc39cd8a4d26",
		"15def4fc077ccfb0e48b32bc07ea3b91acecc5b73ed9caf13b10adf17052c371",
		"07cfe4ec2efa9075763f921e9f29794ec6b945694e41cc19911101270d8b1087",
		"84cdf541cbb3b9b3f26dbdeb9ca3a2721d15447a8b47074c3b08b560f79e5d85",
		"af8e9fc2f15aaedadb96da1afb339b93e3174661327dcc6aad70ea67e183369d",
	}
	var results []string
	N := 16
	for i := 0; i < N; i++ {
		inputs := generateInputs("golden", i+1)
		res := Tree(inputs)
		resHex := hex.EncodeToString(res[:])
		assert.Equal(t, expectedHex[i], resHex)
		results = append(results, resHex)
	}
	t.Logf("results: %#v", results)

	batchRes := BatchTree(generateInputs("golden", N))
	batchResHash := make([]string, N)
	for i := 0; i < N; i++ {
		batchResHash[i] = hex.EncodeToString(batchRes[i][:])
	}
	assert.Equal(t, expectedHex, batchResHash)
}

func BenchmarkIndividualPrefix(b *testing.B) {
	K := 128
	inputs := generateInputs("golden", K)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 1; i < len(inputs); i++ {
			runtime.KeepAlive(Tree(inputs[:i+1]))
		}
	}
}

func BenchmarkBatchPrefix(b *testing.B) {
	K := 128
	inputs := generateInputs("golden", K)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.KeepAlive(BatchTree(inputs))
	}
}
