package gpbft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
)

type ActorID uint64

type StoragePower = big.Int

type PubKey []byte

// NetworkName provides separation between different networks
// it is implicitly included in all signatures and VRFs
type NetworkName string

// Creates a new StoragePower struct with a specific value and returns the result
func NewStoragePower(value int64) StoragePower {
	return big.NewInt(value)
}

type Phase uint8

const (
	INITIAL_PHASE Phase = iota
	QUALITY_PHASE
	CONVERGE_PHASE
	PREPARE_PHASE
	COMMIT_PHASE
	DECIDE_PHASE
	TERMINATED_PHASE
)

func (p Phase) String() string {
	switch p {
	case INITIAL_PHASE:
		return "INITIAL"
	case QUALITY_PHASE:
		return "QUALITY"
	case CONVERGE_PHASE:
		return "CONVERGE"
	case PREPARE_PHASE:
		return "PREPARE"
	case COMMIT_PHASE:
		return "COMMIT"
	case DECIDE_PHASE:
		return "DECIDE"
	case TERMINATED_PHASE:
		return "TERMINATED"
	default:
		return "UNKNOWN"
	}
}

// GMessage is a message in the GossiPBFT protocol. The same message structure is
// used for all rounds and phases. Note that the message is self-attesting so no
// separate envelope or signature is needed.
//   - The signature field fixes the included sender ID via the implied public key.
//   - The signature payload includes all fields in Vote as described by Payload.MarshalForSigning.
//   - The ticket field is a signature of the same public key, so also self-attesting.
type GMessage struct {
	// Sender is the ID of the sender/signer of this message (a miner actor ID).
	Sender ActorID
	// Vote is the payload that is signed by the signature
	Vote Payload
	// Signature by the sender's public key over Instance || Round || Phase || Value.
	Signature []byte `cborgen:"maxlen=96"`
	// Ticket is the VRF ticket for CONVERGE messages (otherwise empty byte array).
	Ticket Ticket `cborgen:"maxlen=96"`
	// Justification for this message (some messages must be justified by a strong quorum of messages from some previous phase).
	Justification *Justification
}

type Justification struct {
	// Vote is the payload that is signed by the signature
	Vote Payload
	// Signers indexes in the base power table of the signers (bitset)
	Signers bitfield.BitField
	// Signature is the BLS aggregate signature of signers
	Signature []byte `cborgen:"maxlen=96"`
}

// GetSigners retrieves the total scaled power and indexes of the signers in this
// justification from the committee's power table.
//
// Returns error if the power table is nil or if any signer index is invalid. A
// nil justification returns zero power and nil signers.
func (m *Justification) GetSigners(pt *PowerTable) (int64, []int, error) {
	if m == nil {
		return 0, nil, nil
	}
	if pt == nil {
		return 0, nil, errors.New("power table cannot be nil")
	}
	var signersScaledPower int64
	var signers []int
	collect := func(index uint64) error {
		if index >= uint64(len(pt.Entries)) {
			return fmt.Errorf("invalid signer index: %d : %w", index, ErrValidationInvalid)
		}
		power := pt.ScaledPower[index]
		if power == 0 {
			return fmt.Errorf("signer with ID %d has no power: %w", pt.Entries[index].ID, ErrValidationInvalid)
		}
		signersScaledPower += power
		signers = append(signers, int(index))
		return nil
	}
	if err := m.Signers.ForEach(collect); err != nil {
		return 0, nil, fmt.Errorf("failed to iterate over signers: %w", err)
	}
	return signersScaledPower, signers, nil
}

type SupplementalData struct {
	// Commitments is the Merkle-tree of instance-specific commitments. Currently
	// empty but this will eventually include things like snark-friendly power-table
	// commitments.
	Commitments [32]byte `cborgen:"maxlen=32"`
	// PowerTable is the DagCBOR-blake2b256 CID of the power table used to validate
	// the next instance, taking lookback into account.
	PowerTable cid.Cid // []PowerEntry
}

func (d *SupplementalData) Eq(other *SupplementalData) bool {
	return d.Commitments == other.Commitments && d.PowerTable == other.PowerTable
}

func (sd SupplementalData) MarshalJSON() ([]byte, error) {
	return json.Marshal(&supplementalDataJson{
		Commitments:         sd.Commitments[:],
		supplementalDataSub: (*supplementalDataSub)(&sd),
	})
}

func (sd *SupplementalData) UnmarshalJSON(b []byte) error {
	aux := &supplementalDataJson{supplementalDataSub: (*supplementalDataSub)(sd)}
	var err error
	if err = json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if len(aux.Commitments) != 32 {
		return errors.New("commitments must be 32 bytes")
	}
	copy(sd.Commitments[:], aux.Commitments)
	return nil
}

// supplementalDataSub offers a custom JSON marshalling for SupplementalData to
// achieve a commitment field that is a base64-encoded string.
type supplementalDataSub SupplementalData
type supplementalDataJson struct {
	Commitments []byte
	*supplementalDataSub
}

// Payload specifies the fields of the message that make up the signature
// payload.
type Payload struct {
	// Instance is the GossiPBFT monotonically increasing instance number.
	Instance uint64
	// Round is the GossiPBFT round number of the instance.
	Round uint64
	// Phase is the name of the GossiPBFT phase.
	Phase Phase
	// SupplementalData is the common data used by the instance to indicate the power table and commitment.
	SupplementalData SupplementalData
	// Value is the value agreed-upon in a single instance.
	Value *ECChain
}

func (p *Payload) Eq(other *Payload) bool {
	if p == other {
		return true
	}
	if other == nil {
		return false
	}
	return p.Instance == other.Instance &&
		p.Round == other.Round &&
		p.Phase == other.Phase &&
		p.SupplementalData.Eq(&other.SupplementalData) &&
		p.Value.Eq(other.Value)
}

func (p *Payload) MarshalForSigning(nn NetworkName) []byte {
	return p.MarshalForSigningWithValueKey(nn, p.Value.Key())
}

func (p *Payload) MarshalForSigningWithValueKey(nn NetworkName, key ECChainKey) []byte {
	const separator = ":"
	var buf bytes.Buffer
	buf.Grow(len(DomainSeparationTag) +
		len(nn) + len(separator)*2 +
		1 + // Phase
		8 + // Round
		8 + // Instance
		len(p.SupplementalData.Commitments) +
		len(key) + // Key
		p.SupplementalData.PowerTable.ByteLen(),
	)
	buf.WriteString(DomainSeparationTag)
	buf.WriteString(separator)
	buf.WriteString(string(nn))
	buf.WriteString(separator)

	_ = binary.Write(&buf, binary.BigEndian, p.Phase)
	_ = binary.Write(&buf, binary.BigEndian, p.Round)
	_ = binary.Write(&buf, binary.BigEndian, p.Instance)
	_, _ = buf.Write(p.SupplementalData.Commitments[:])
	_, _ = buf.Write(key[:])
	_, _ = buf.Write(p.SupplementalData.PowerTable.Bytes())
	return buf.Bytes()
}

func (m GMessage) String() string {
	return fmt.Sprintf("%s{%d}(%d %s)", m.Vote.Phase, m.Vote.Instance, m.Vote.Round, m.Vote.Value)
}

// PartialGMessage is a GMessage with an additional VoteValueKey field specifying
// the vote value key of the inner GMessage, where the actual vote value is not
// included in the message. This is used for rebroadcasting messages without the
// value, where the value is not needed, but the key is required to verify the
// signature.
type PartialGMessage struct {
	*GMessage
	VoteValueKey ECChainKey `cborgen:"maxlen=32"`
}
