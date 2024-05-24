package gpbft

import (
	"fmt"
	"math/big"

	xerrors "golang.org/x/xerrors"
)

type MessageBuilder struct {
	powerTable powerTableAccess

	Payload Payload

	BeaconForTicket []byte

	Justification *Justification
}

type powerTableAccess interface {
	Get(ActorID) (*big.Int, PubKey)
}

// Build uses the template and a signer interface to build GMessage
// It is a shortcut for when separated flow is not required
func (mt MessageBuilder) Build(networkName NetworkName, signer Signer, id ActorID) (*GMessage, error) {
	st, err := mt.PrepareSigningInputs(networkName, id)
	if err != nil {
		return nil, xerrors.Errorf("preparing signing inputs: %w", err)
	}

	payloadSig, vrf, err := st.Sign(signer)
	if err != nil {
		return nil, xerrors.Errorf("signing template: %w", err)
	}

	return st.Complete(payloadSig, vrf), nil
}

type SignatureBuilder struct {
	ParticipantID ActorID
	NetworkName   NetworkName
	Payload       Payload
	Justification *Justification

	PubKey        PubKey
	PayloadToSign []byte
	VRFToSign     []byte
}

func (mt MessageBuilder) PrepareSigningInputs(networkName NetworkName, id ActorID) (SignatureBuilder, error) {
	fmt.Println(">>>>> PREPARING SIGNATURE ID", id)
	_, pubKey := mt.powerTable.Get(id)
	if pubKey == nil {
		return SignatureBuilder{}, xerrors.Errorf("could not find pubkey for actor %d", id)
	}
	signingTemplate := SignatureBuilder{
		ParticipantID: id,
		NetworkName:   networkName,
		Payload:       mt.Payload,
		Justification: mt.Justification,

		PubKey: pubKey,
	}
	signingTemplate.PayloadToSign = mt.Payload.MarshalForSigning(networkName)
	if mt.BeaconForTicket != nil {
		fmt.Println("verify vrf ticket")
		signingTemplate.VRFToSign = vrfSerializeSigInput(mt.BeaconForTicket, mt.Payload.Instance, mt.Payload.Round, networkName)
	}
	return signingTemplate, nil
}

// Sign creates signatures for the SigningTemplate, it could live across RPC boundry
func (st SignatureBuilder) Sign(signer Signer) ([]byte, []byte, error) {
	payloadSignature, err := signer.Sign(st.PubKey, st.PayloadToSign)
	if err != nil {
		return nil, nil, xerrors.Errorf("signing payload: %w", err)
	}
	fmt.Println(">>>>>>> Payload to sign builder", st.PubKey, "---", len(st.PayloadToSign), "----", st.PayloadToSign)
	fmt.Println(">>>>>>> Signature builder", st.PubKey, payloadSignature)
	var vrf []byte
	if st.VRFToSign != nil {
		vrf, err = signer.Sign(st.PubKey, st.VRFToSign)
		if err != nil {
			return nil, nil, xerrors.Errorf("signing vrf: %w", err)
		}
	}
	return payloadSignature, vrf, nil
}

// Complete takes the template and signatures and builds GMessage out of them
func (st SignatureBuilder) Complete(payloadSignature []byte, vrf []byte) *GMessage {
	gmsg := &GMessage{
		Sender:        st.ParticipantID,
		Vote:          st.Payload,
		Signature:     payloadSignature,
		Ticket:        vrf,
		Justification: st.Justification,
	}
	return gmsg
}
