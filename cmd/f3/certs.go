package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var (
	certFrom *peer.AddrInfo

	certsCmd = cli.Command{
		Name: "certs",
		Subcommands: []*cli.Command{
			{
				Name: "list-from",
				Flags: []cli.Flag{
					limitFlag,
					fromAddrFlag,
					networkNameFlag,
					includePowerTableFlag,
				},
				Action: func(cctx *cli.Context) error {
					instanceArg := cctx.Args().First()
					if instanceArg == "" {
						return errors.New("missing instance as first argument")
					}
					instance, err := strconv.ParseUint(instanceArg, 10, 64)
					if err != nil {
						return err
					}

					if certFrom == nil {
						return errors.New("--from addrinfo is required")
					}

					host, err := libp2p.New()
					if err != nil {
						return err
					}
					defer func() { _ = host.Close() }()

					if err := host.Connect(cctx.Context, *certFrom); err != nil {
						return err
					}
					client := certexchange.Client{
						Host:           host,
						NetworkName:    gpbft.NetworkName(cctx.String(networkNameFlag.Name)),
						RequestTimeout: cctx.Duration(timeoutFlag.Name),
					}

					rh, ch, err := client.Request(cctx.Context, certFrom.ID, &certexchange.Request{
						FirstInstance:     instance,
						Limit:             cctx.Uint64(limitFlag.Name),
						IncludePowerTable: true,
					})
					if err != nil {
						return err
					}
					var result = struct {
						*certexchange.ResponseHeader
						PowerTableCID cid.Cid
						Certificates  []*certs.FinalityCertificate
					}{
						ResponseHeader: rh,
					}

					result.PowerTableCID, err = certs.MakePowerTableCID(rh.PowerTable)
					if err != nil {
						return err
					}

					for certificate := range ch {
						result.Certificates = append(result.Certificates, certificate)
					}
					output, err := json.MarshalIndent(result, "", "  ")
					if err != nil {
						return err
					}
					_, _ = fmt.Fprintln(cctx.App.Writer, string(output))
					return nil
				},
			},
		},
	}

	limitFlag = &cli.Uint64Flag{
		Name:  "limit",
		Usage: "Maximum number of certificates to list from the peer",
		Value: 100,
	}
	fromAddrFlag = &cli.StringFlag{
		Name:    "peer",
		Aliases: []string{"p"},
		Usage:   "The addrinfo of the peer to get the certificate from",
		Action: func(cctx *cli.Context, v string) (err error) {
			certFrom, err = peer.AddrInfoFromString(v)
			return
		},
		Required: true,
	}
	networkNameFlag = &cli.StringFlag{
		Name:     "networkName",
		Aliases:  []string{"nn"},
		Usage:    "The network name",
		Required: true,
	}

	includePowerTableFlag = &cli.BoolFlag{
		Name:    "includePowerTable",
		Aliases: []string{"pt"},
		Usage:   "Whether to include the power table in the results",
	}
	timeoutFlag = &cli.DurationFlag{
		Name:  "timeout",
		Usage: "Request timeout.",
		Value: 30 * time.Second,
	}
)
