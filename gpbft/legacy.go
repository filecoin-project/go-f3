package gpbft

// LegacyECChain is the old representation of EC chain in earlier releases, kept for
// wire format backward compatibility with Calibration network.
type LegacyECChain []TipSet
