package granite

import "fmt"

// Computes VRF tickets for use in CONVERGE phase.
// A VRF ticket is produced by signing a payload which digests a beacon randomness value and
// the instance and round numbers.
type VRFTicketSource interface {
	MakeTicket(beacon []byte, instance int, round int, signer string) []byte
}

type VRFTicketVerifier interface {
	VerifyTicket(beacon []byte, instance int, round int, signer string, ticket []byte) bool
}

type FakeVRF struct {
}

func NewFakeVRF() *FakeVRF {
	return &FakeVRF{}
}

func (f *FakeVRF) MakeTicket(beacon []byte, instance int, round int, signer string) []byte {
	return []byte(fmt.Sprintf("FakeTicket(%x, %d, %d, %s)", beacon, instance, round, signer))
}

func (f *FakeVRF) VerifyTicket(beacon []byte, instance int, round int, signer string, ticket []byte) bool {
	return string(ticket) == fmt.Sprintf("FakeTicket(%x, %d, %d, %s)", beacon, instance, round, signer)
}
