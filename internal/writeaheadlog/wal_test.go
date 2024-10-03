package writeaheadlog

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	cbgtesting "github.com/whyrusleeping/cbor-gen/testing"
)

type testPayload cbgtesting.SimpleTypeOne

func (tp *testPayload) WALEpoch() uint64 {
	return tp.Value
}

func (tp *testPayload) MarshalCBOR(w io.Writer) error {
	return (*cbgtesting.SimpleTypeOne)(tp).MarshalCBOR(w)
}

func (tp *testPayload) UnmarshalCBOR(r io.Reader) error {
	return (*cbgtesting.SimpleTypeOne)(tp).UnmarshalCBOR(r)
}

var _ Entry = (*testPayload)(nil)

func TestWALSimple(t *testing.T) {
	path := t.TempDir()
	t.Logf("tempdir: %v", path)
	wal, err := Open[testPayload](path)
	require.NoError(t, err)

	entries := []testPayload{
		{Value: 0, Foo: "Foo0"},
		{Value: 1, Foo: "Foo1"},
		{Value: 1, Foo: "Foo1.1"},
		{Value: 2, Foo: "Foo2"},
	}
	for _, e := range entries {
		err = wal.Append(e)
		require.NoError(t, err)
	}
	res, err := wal.All()
	require.NoError(t, err)
	require.Equal(t, entries, res)

	err = wal.Close()
	require.NoError(t, err)
	res, err = wal.All()
	require.NoError(t, err)
	require.Equal(t, entries, res)

	wal = nil

	wal, err = Open[testPayload](path)
	require.NoError(t, err)

	res, err = wal.All()
	require.NoError(t, err)
	require.Equal(t, entries, res)

	err = wal.Purge(1) // one file, should keep all
	require.NoError(t, err)

	res, err = wal.All()
	require.NoError(t, err)
	require.Equal(t, entries, res)
}
func TestWALRecovery(t *testing.T) {
	path := t.TempDir()
	t.Logf("tempdir: %v", path)
	wal, err := Open[testPayload](path)
	require.NoError(t, err)

	entries := []testPayload{
		{Value: 0, Foo: "Foo0"},
		{Value: 1, Foo: "Foo1"},
	}
	for _, e := range entries {
		err = wal.Append(e)
		require.NoError(t, err)
	}

	// Simulate a crash before finalizing
	wal = nil

	wal, err = Open[testPayload](path)
	require.NoError(t, err)

	res, err := wal.All()
	require.NoError(t, err)
	require.Equal(t, entries, res)
}

func TestWALPartialWrite(t *testing.T) {
	path := t.TempDir()
	t.Logf("tempdir: %v", path)
	wal, err := Open[testPayload](path)
	require.NoError(t, err)

	entries := []testPayload{
		{Value: 0, Foo: "Foo0"},
		{Value: 1, Foo: "Foo1"},
	}
	for _, e := range entries {
		err = wal.Append(e)
		require.NoError(t, err)
	}

	// Simulate a partial write
	stat, err := wal.active.file.Stat()
	require.NoError(t, err)
	err = wal.active.file.Truncate(stat.Size() - 8)
	require.NoError(t, err)
	err = wal.active.file.Close()
	require.NoError(t, err)
	wal = nil

	wal, err = Open[testPayload](path)
	require.NoError(t, err)
	all, err := wal.All()
	require.NoError(t, err)
	require.Equal(t, []testPayload{entries[0]}, all)
}

func TestWALEmpty(t *testing.T) {
	path := t.TempDir()
	t.Logf("tempdir: %v", path)
	wal, err := Open[testPayload](path)
	require.NoError(t, err)

	res, err := wal.All()
	require.NoError(t, err)
	require.Empty(t, res)

	err = wal.Close()
	require.NoError(t, err)

	res, err = wal.All()
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestWALPurge(t *testing.T) {
	path := t.TempDir()
	t.Logf("tempdir: %v", path)
	wal, err := Open[testPayload](path)
	require.NoError(t, err)

	foo0 := testPayload{Value: 0, Foo: "Foo0"}
	entries := []testPayload{
		{Value: 1, Foo: "Foo1"},
		{Value: 2, Foo: "Foo2"},
		{Value: 3, Foo: "Foo3"},
	}
	for _, e := range entries {
		err = wal.Append(foo0)
		require.NoError(t, err)
		err = wal.Append(e)
		require.NoError(t, err)
		err = wal.Close()
		require.NoError(t, err)
	}

	err = wal.Purge(2)
	require.NoError(t, err)

	expected := []testPayload{
		foo0,
		{Value: 2, Foo: "Foo2"},
		foo0,
		{Value: 3, Foo: "Foo3"},
	}
	res, err := wal.All()
	require.NoError(t, err)
	require.Equal(t, expected, res)
}
