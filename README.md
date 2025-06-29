# Go implementation of Fast Finality in Filecoin

[![Go Test](https://github.com/filecoin-project/go-f3/actions/workflows/go-test.yml/badge.svg)](https://github.com/filecoin-project/go-f3/actions/workflows/go-test.yml) [![codecov](https://codecov.io/gh/filecoin-project/go-f3/graph/badge.svg?token=6uD131t7gs)](https://codecov.io/gh/filecoin-project/go-f3)

This repository contains the golang implementation of the Fast Finality (F3) protocol for Filecoin as specified
by [FIP-0086](https://github.com/filecoin-project/FIPs/blob/master/FIPS/fip-0086.md). This protocol uses GossipPBFT
consensus protocol to finalize tipsets when voted for by more than two-thirds of the storage power.

## Key Features

- **Core Implementation of GossipBFT Consensus Protocol**: The heart of Go-F3.
- **F3 Filecoin Integration Module**: Streamlines the integration of the F3 protocol within the broader Filecoin
  ecosystem, specifically Lotus and other Filecoin full nodes.
- **Simulation Package**: Includes a robust simulation environment with various adversary models, enabling rigorous
  testing of the protocol under different network conditions and attack scenarios.
- **Emulator Package**: Facilitates message-by-message interaction with the GossipPBFT protocol, providing a detailed
  view of protocol mechanics and performance.
- **Standalone F3 Participant Implementation**: A complete implementation of an F3 protocol participant, capable of
  operating independently within the Filecoin network.
- **Finality Certificate**: Implements the generation and management of finality certificates, which cary transportable
  proofs of finality.
- **Finality Certificate Exchange Protocol**: Features an adaptive self-configuring polling mechanism, enhancing the
  efficiency and reliability of certificate exchange among participants.

## Status

**🚀 Live on Mainnet**

Go-F3 was successfully activated on Filecoin mainnet on April 29, 2025 on epoch 4920480.

## Project Structure

- `blssig`: BLS signature schemes.
- `certexchange`: Certificate exchange mechanisms.
- `certstore`: Certificate storage.
- `cmd`: Command line to run a standalone F3 participant.
- `ec`: Expected Consensus utilities.
- `emulator`: Network emulation tools.
- `gpbft`: GossipPBFT protocol implementation.
- `merkle`: Merkle tree implementations.
- `sim`: Simulation harness.
- `test`: Test suite for various components.

## License

This project is dual-licensed under the MIT and Apache 2.0 licenses. See [LICENSE-APACHE](LICENSE-APACHE)
and [LICENSE-MIT](LICENSE-MIT) for more details.
