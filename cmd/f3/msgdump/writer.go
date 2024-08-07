package msgdump

import (
	"io"

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
