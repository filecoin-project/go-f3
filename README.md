# Go implementation of Fast Finality for Filecoin

This repository contains a Go implementation of the [Filecoin finality module,
F3](https://github.com/filecoin-project/FIPs/blob/f3564189d11817328168c9e75a80ff5f7292ba13/FIPS/fip-xxxx.md)
(WIP).

This executes an iterative GossiPBFT consensus protocol to declare tipsets as
final when voted for by >2/3 of the consensus power.

## Status

Work in progress.

This implementation began as a simulation during the protocol design phase. It
is currently being transformed into production code, while maintaining and
expanding the simulator.

## Usage

Run the unit tests to exercise the GossipPBFT protocol.

```
$ go test ./...
```

There is also a main entry point to run GossiPBFT instance with honest nodes in
simulation.

```
$ go run ./sim/cmd/f3sim -help
Usage of f3sim:
  -delta-back-off-exponent float
        exponential factor adjusting the delta value per round (default 1.3)
  -granite-delta float
        granite delta parameter (bound on message delay) (default 2)
  -iterations int
        number of simulation iterations (default 1)
  -latency-mean float
        mean network latency in seconds (default 0.5)
  -latency-seed int
        random seed for network latency (default <current time>)
  -max-rounds uint
        max rounds to allow before failing (default 10)
  -participants int
        number of participants (default 3)
  -trace int
        trace verbosity level
```

## Integration

The code does not yet express an API for integration into a Filecoin node.
Coming soon!

## Structure

Modules:

- `f3`: the protocol implementation
- `sim`: the simulation harness
- `adversary`: specific adversarial behaviors for use in tests
- `test`: unit tests which execute the protocol in simulation

## License

Dual-licensed under MIT + Apache 2.0