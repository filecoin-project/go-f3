# Go implementation of Fast Finality for Filecoin

This repository contains a Go implementation of the Filecoin finality module, F3.
This executes an iterative GossiPBFT consensus protocol to declare tipsets as final 
when voted for by >2/3  of the consensus power.

## Status

Work in progress.

This implementation began as a simulation during the protocol design phase.
It is currently being transformed into production code, while maintaining and expanding the simulator.

## Usage

Run the unit tests to exercise the GossipPBFT protocol.

```
$ go test ./...
```

There is also a main entry point to run GossiPBFT instance with honest nodes in simulation.

```
$ go run f3sim.go -help
Usage of /path/to/f3sim:
  -granite-delta float
    	granite delta parameter (default 6)
  -granite-delta-rate float
    	change in delta for each round (default 2)
  -iterations int
    	number of simulation iterations (default 1)
  -latency-mean float
    	mean network latency (default 0.5)
  -latency-seed int
    	random seed for network latency (default current time)
  -max-rounds int
    	max rounds to allow before failing (default 10)
  -participants int
    	number of participants (default 3)
  -trace int
    	trace verbosity level
```

## Integration

The code does not yet express an API for integration into a Filecoin node.
Coming soon.

## Structure
Modules:
- `f3`: the protocol implementation
- `net`: the simulated network
- `sim`: the simulation harness
- `adversary`: specific adversarial behaviors for use in tests
- `test`: unit tests which execute the protocol in simulation


## License