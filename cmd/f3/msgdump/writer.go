package msgdump

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/filecoin-project/go-f3/gpbft"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type Writer[T cbg.CBORMarshaler] struct {
	underlying io.WriteSeeker
}

func (w *Writer[T]) Write(item T) error {
	// optimize it:
	//  keep cbg.Writer around
	// write in for example 1MiB padded chunks to be able to read it in parallel
	return item.MarshalCBOR(w.underlying)
}

func DirForNetwork(dir string, nn gpbft.NetworkName) string {
	basename := fmt.Sprintf("msgs-%s", strings.ReplaceAll(string(nn), "/", "-"))
	return filepath.Join(dir, basename)
}
