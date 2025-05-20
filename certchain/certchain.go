package certchain

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"slices"

	"github.com/filecoin-project/go-bitfield"
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	_ gpbft.CommitteeProvider = (*CertChain)(nil)
	_ gpbft.ProposalProvider  = (*CertChain)(nil)
)

type FinalityCertificateProvider func(context.Context, uint64) (*certs.FinalityCertificate, error)

type tipSetWithPowerTable struct {
	*gpbft.TipSet
	Beacon     []byte
	PowerTable *gpbft.PowerTable
}

type CertChain struct {
	*options

	rng              *rand.Rand
	certificates     []*certs.FinalityCertificate
	generateProposal func(context.Context, uint64) (*gpbft.ECChain, error)
}

func New(o ...Option) (*CertChain, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &CertChain{
		options: opts,
		rng:     rand.New(rand.NewSource(opts.seed)),
	}, nil
}

func (cc *CertChain) GetCommittee(ctx context.Context, instance uint64) (*gpbft.Committee, error) {
	var committeeEpoch int64
	if instance < cc.m.InitialInstance+cc.m.CommitteeLookback {
		committeeEpoch = cc.m.BootstrapEpoch - cc.m.EC.Finality
	} else {
		lookbackIndex := instance - cc.m.CommitteeLookback - cc.m.InitialInstance + 1
		if lookbackIndex >= uint64(len(cc.certificates)) {
			return nil, fmt.Errorf("no prior finality certificate to get committee at instance %d", instance)
		}
		certAtLookback := cc.certificates[lookbackIndex]
		committeeEpoch = certAtLookback.ECChain.Head().Epoch
	}
	tspt, err := cc.getTipSetWithPowerTableByEpoch(ctx, committeeEpoch)
	if err != nil {
		return nil, err
	}
	return cc.getCommittee(tspt)
}

func (cc *CertChain) GetProposal(ctx context.Context, instance uint64) (*gpbft.SupplementalData, *gpbft.ECChain, error) {
	proposal, err := cc.generateProposal(ctx, instance)
	if err != nil {
		return nil, nil, err
	}
	suppData, err := cc.getSupplementalData(ctx, instance)
	if err != nil {
		return nil, nil, err
	}
	return suppData, proposal, nil
}

func (cc *CertChain) getSupplementalData(ctx context.Context, instance uint64) (*gpbft.SupplementalData, error) {
	nextCommittee, err := cc.GetCommittee(ctx, instance+1)
	if err != nil {
		return nil, err
	}
	var data gpbft.SupplementalData
	data.PowerTable, err = certs.MakePowerTableCID(nextCommittee.PowerTable.Entries)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func (cc *CertChain) getCommittee(tspt *tipSetWithPowerTable) (*gpbft.Committee, error) {
	av, err := cc.sv.Aggregate(tspt.PowerTable.Entries.PublicKeys())
	if err != nil {
		return nil, err
	}
	return &gpbft.Committee{
		PowerTable:        tspt.PowerTable,
		Beacon:            tspt.Beacon,
		AggregateVerifier: av,
	}, nil
}

func (cc *CertChain) getTipSetWithPowerTableByEpoch(ctx context.Context, epoch int64) (*tipSetWithPowerTable, error) {
	ts, err := cc.ec.GetTipsetByEpoch(ctx, epoch)
	if err != nil {
		return nil, err
	}
	ptEntries, err := cc.ec.GetPowerTable(ctx, ts.Key())
	if err != nil {
		return nil, err
	}
	pt := gpbft.NewPowerTable()
	if err := pt.Add(ptEntries...); err != nil {
		return nil, err
	}
	ptCid, err := certs.MakePowerTableCID(ptEntries)
	if err != nil {
		return nil, err
	}
	return &tipSetWithPowerTable{
		TipSet: &gpbft.TipSet{
			Epoch:      epoch,
			Key:        ts.Key(),
			PowerTable: ptCid,
		},
		Beacon:     ts.Beacon(),
		PowerTable: pt,
	}, nil
}

func (cc *CertChain) generateRandomProposal(ctx context.Context, base *gpbft.TipSet, len int) (*gpbft.ECChain, error) {
	if len == 0 {
		return gpbft.NewChain(base)
	}

	suffix := make([]*gpbft.TipSet, len-1)
	for i := range suffix {
		epoch := base.Epoch + 1 + int64(i)
		gTS, err := cc.getTipSetWithPowerTableByEpoch(ctx, epoch)
		if err != nil {
			return nil, err
		}
		suffix[i] = gTS.TipSet
	}
	return gpbft.NewChain(base, suffix...)
}

func (cc *CertChain) signProportionally(ctx context.Context, committee *gpbft.Committee, payload *gpbft.Payload) (*bitfield.BitField, []byte, error) {
	candidateSigners := slices.Clone(committee.PowerTable.Entries)
	cc.rng.Shuffle(candidateSigners.Len(), func(this, that int) {
		candidateSigners[this], candidateSigners[that] = candidateSigners[that], candidateSigners[this]
	})

	// Pick a random proportion of signing power across committee between inclusive
	// range of 66% to 100% of total power.
	const minimumPower = 2.0 / 3.0
	targetPowerPortion := minimumPower + cc.rng.Float64()*(1.0-minimumPower)
	signingPowerThreshold := int64(float64(committee.PowerTable.ScaledTotal) * targetPowerPortion)

	var signingPowerSoFar int64
	type signatureAt struct {
		signerIndex int
		signature   []byte
	}
	var signatures []signatureAt
	marshalledPayload := payload.MarshalForSigning(cc.m.NetworkName)
	for _, p := range candidateSigners {
		scaledPower, key := committee.PowerTable.Get(p.ID)
		if scaledPower == 0 {
			continue
		}
		sig, err := cc.sv.Sign(ctx, key, marshalledPayload)
		if err != nil {
			return nil, nil, err
		}
		signingPowerSoFar += scaledPower
		signatures = append(signatures, signatureAt{
			signerIndex: committee.PowerTable.Lookup[p.ID],
			signature:   sig,
		})
		if signingPowerSoFar >= signingPowerThreshold {
			break
		}
	}

	// Now, sort the signatures in ascending order of their index in power table.
	slices.SortFunc(signatures, func(one, other signatureAt) int {
		return one.signerIndex - other.signerIndex
	})
	// Type gymnastics.
	signerIndicesMask := make([]int, len(signatures))
	signerIndices := make([]uint64, len(signatures))
	signatureBytes := make([][]byte, len(signatures))
	for i, s := range signatures {
		signerIndicesMask[i] = s.signerIndex
		signerIndices[i] = uint64(s.signerIndex)
		signatureBytes[i] = s.signature
	}
	itr, err := rlepluslazy.RunsFromSlice(signerIndices)
	if err != nil {
		return nil, nil, err
	}
	signers, err := bitfield.NewFromIter(itr)
	if err != nil {
		return nil, nil, err
	}
	signature, err := committee.AggregateVerifier.Aggregate(signerIndicesMask, signatureBytes)
	if err != nil {
		return nil, nil, err
	}
	return &signers, signature, nil
}

func (cc *CertChain) sign(ctx context.Context, committee *gpbft.Committee, payload *gpbft.Payload, signers *bitfield.BitField) ([]byte, error) {
	const minimumPower = 2.0 / 3.0
	minSigningPower := int64(float64(committee.PowerTable.ScaledTotal) * minimumPower)

	var signingPowerSoFar int64
	var signatures [][]byte
	var signersMask []int
	marshalledPayload := payload.MarshalForSigning(cc.m.NetworkName)
	if err := signers.ForEach(
		func(signerIndex uint64) error {
			p := committee.PowerTable.Entries[signerIndex]
			scaledPower, key := committee.PowerTable.Get(p.ID)
			if scaledPower == 0 {
				return fmt.Errorf("zero scaled power for actor ID: %d", p.ID)
			}
			sig, err := cc.sv.Sign(ctx, key, marshalledPayload)
			if err != nil {
				return err
			}
			signatures = append(signatures, sig)
			signersMask = append(signersMask, int(signerIndex))
			signingPowerSoFar += scaledPower
			return nil
		},
	); err != nil {
		return nil, err
	}
	if signingPowerSoFar < minSigningPower {
		signingRatio := float64(signingPowerSoFar) / float64(committee.PowerTable.ScaledTotal)
		return nil, fmt.Errorf("signing power does not meet the 2/3 of total power at instance %d: %.3f", payload.Instance, signingRatio)
	}
	return committee.AggregateVerifier.Aggregate(signersMask, signatures)
}

func (cc *CertChain) Generate(ctx context.Context, length uint64) ([]*certs.FinalityCertificate, error) {
	cc.certificates = make([]*certs.FinalityCertificate, 0, length)

	cc.generateProposal = func(ctx context.Context, instance uint64) (*gpbft.ECChain, error) {
		var baseEpoch int64
		if instance == cc.m.InitialInstance {
			baseEpoch = cc.m.BootstrapEpoch - cc.m.EC.Finality
		} else {
			latest := len(cc.certificates) - 1
			if latest < 0 {
				return nil, fmt.Errorf("no prior finality certificate to get proposal at instance %d", instance)
			}
			baseEpoch = cc.certificates[latest].ECChain.Head().Epoch
		}
		base, err := cc.getTipSetWithPowerTableByEpoch(ctx, baseEpoch)
		if err != nil {
			return nil, err
		}
		proposalLen := cc.rng.Intn(gpbft.ChainMaxLen)
		return cc.generateRandomProposal(ctx, base.TipSet, proposalLen)
	}

	instance := cc.m.InitialInstance
	committee, err := cc.GetCommittee(ctx, instance)
	if err != nil {
		return nil, err
	}
	var nextCommittee *gpbft.Committee
	for range length {
		suppData, proposal, err := cc.GetProposal(ctx, instance)
		if err != nil {
			return nil, err
		}

		bf, aggSig, err := cc.signProportionally(ctx, committee, &gpbft.Payload{
			Instance:         instance,
			Phase:            gpbft.DECIDE_PHASE,
			SupplementalData: *suppData,
			Value:            proposal,
		})
		if err != nil {
			return nil, err
		}

		nextCommittee, err = cc.GetCommittee(ctx, instance+1)
		if err != nil {
			return nil, err
		}
		certificate := &certs.FinalityCertificate{
			GPBFTInstance:    instance,
			ECChain:          proposal,
			SupplementalData: *suppData,
			Signers:          *bf,
			Signature:        aggSig,
			PowerTableDelta:  certs.MakePowerTableDiff(committee.PowerTable.Entries, nextCommittee.PowerTable.Entries),
		}
		cc.certificates = append(cc.certificates, certificate)
		committee = nextCommittee
		instance++
	}
	return cc.certificates, nil
}

func (cc *CertChain) Validate(ctx context.Context, crts []*certs.FinalityCertificate) error {
	for _, cert := range crts {
		instance := cert.GPBFTInstance
		proposal := cert.ECChain
		suppData, err := cc.getSupplementalData(ctx, instance)
		if err != nil {
			return err
		}
		if !suppData.Eq(&cert.SupplementalData) {
			return fmt.Errorf("supplemental data mismatch at instance %d", instance)
		}
		committee, err := cc.GetCommittee(ctx, instance)
		if err != nil {
			return fmt.Errorf("getting committee for instance %d: %w", instance, err)
		}
		sig, err := cc.sign(ctx, committee, &gpbft.Payload{
			Instance:         instance,
			Phase:            gpbft.DECIDE_PHASE,
			SupplementalData: *suppData,
			Value:            proposal,
		}, &cert.Signers)
		if err != nil {
			return err
		}
		if !bytes.Equal(sig, cert.Signature) {
			return fmt.Errorf("certificate signature mismatch at instance %d", instance)
		}
		gotNextCommittee, err := certs.ApplyPowerTableDiffs(committee.PowerTable.Entries, cert.PowerTableDelta)
		if err != nil {
			return err
		}
		gotNextPtCid, err := certs.MakePowerTableCID(gotNextCommittee)
		if err != nil {
			return err
		}
		if !suppData.PowerTable.Equals(gotNextPtCid) {
			return fmt.Errorf("power table diff mismatch at instance %d", instance)
		}
		cc.certificates = append(cc.certificates, cert)
	}
	return nil
}
