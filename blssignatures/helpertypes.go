package blssignatures

type indexedSig struct {
	index int
	sig   []byte
}

type aggSigData struct {
	Sig  []byte
	Mask []byte
}
