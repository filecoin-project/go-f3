package gpbft

import xerrors "golang.org/x/xerrors"

type MessageTemplate struct {
	powerTable *PowerTable

	NetworkName NetworkName
	Payload     Payload

	BeaconForTicket []byte

	Justification *Justification
}

// Build uses the template and a signer interface to build GMessage
// It is a shortcut for when separated flow is not required
func (mt MessageTemplate) Build(signer Signer, id ActorID) (*GMessage, error) {
	st, err := mt.PrepareSigningInputs(id)
	if err != nil {
		return nil, xerrors.Errorf("preparing signing inputs: %w", err)
	}

	payloadSig, vrf, err := st.Sign(signer)
	if err != nil {
		return nil, xerrors.Errorf("signing template: %w", err)
	}

	return st.Complete(payloadSig, vrf), nil
}

type SigningTemplate struct {
	ParticipantID ActorID
	NetworkName   NetworkName
	Payload       Payload
	Justification *Justification

	PubKey        PubKey
	PayloadToSign []byte
	VRFToSign     []byte
}

func (mt MessageTemplate) PrepareSigningInputs(id ActorID) (SigningTemplate, error) {
	_, pubKey := mt.powerTable.Get(id)
	if pubKey == nil {
		return SigningTemplate{}, xerrors.Errorf("could not find pubkey for actor %d", id)
	}
	signingTemplate := SigningTemplate{
		ParticipantID: id,
		NetworkName:   mt.NetworkName,
		Payload:       mt.Payload,
		Justification: mt.Justification,

		PubKey: pubKey,
	}
	signingTemplate.PayloadToSign = mt.Payload.MarshalForSigning(mt.NetworkName)
	if mt.BeaconForTicket != nil {
		signingTemplate.VRFToSign = vrfSerializeSigInput(mt.BeaconForTicket, mt.Payload.Instance, mt.Payload.Round, mt.NetworkName)
	}
	return signingTemplate, nil
}

// Sign creates signatures for the SigningTemplate, it could live across RPC boundry
func (st SigningTemplate) Sign(signer Signer) ([]byte, []byte, error) {
	payloadSignature, err := signer.Sign(st.PubKey, st.PayloadToSign)
	if err != nil {
		return nil, nil, xerrors.Errorf("signing payload: %w", err)
	}
	var vrf []byte
	if st.VRFToSign != nil {
		vrf, err = signer.Sign(st.PubKey, st.VRFToSign)
		if err != nil {
			return nil, nil, xerrors.Errorf("signing vrf: %w", err)
		}
	}
	return payloadSignature, vrf, nil
}

// Complete takes the template and signautres and builds GMessage out of them
func (st SigningTemplate) Complete(payloadSignature []byte, vrf []byte) *GMessage {
	gmsg := &GMessage{
		Sender:        st.ParticipantID,
		Vote:          st.Payload,
		Signature:     payloadSignature,
		Ticket:        vrf,
		Justification: st.Justification,
	}
	return gmsg
}
