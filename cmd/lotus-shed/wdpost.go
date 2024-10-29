package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/proof"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var checkCmd = &cli.Command{
	Name:  "check",
	Usage: "wdpost data check",
	Description: `if storage is qiniu:
	1. replace extern/filecoin-ffi/
	2. use export QINIU=cfg.toml`,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "miner",
			Usage:    "specify miner",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "parallel read",
			Value: 50,
		},
		&cli.StringFlag{
			Name:     "sealed",
			Usage:    "specify sealed path",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "cache",
			Usage:    "specify cache path",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "sectorsfile",
			Usage: "specify sectorslist file, one sector number per line",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeAPI, acloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer acloser()

		ctx := lcli.ReqContext(cctx)

		sealedPath := cctx.String("sealed")
		cachePath := cctx.String("cache")
		var errPathNotFound = xerrors.Errorf("fsstat: path not found")
		if sealedPath == "" || cachePath == "" {
			return errPathNotFound
		}

		throttle := make(chan struct{}, cctx.Int("parallel"))
		var wg sync.WaitGroup
		sectorsfile := cctx.String("sectorsfile")
		var fileList map[uint64]struct{}
		if cctx.IsSet("sectorsfile") {
			fileList, err = readFile(sectorsfile)
			if err != nil {
				return err
			}
		}

		minerAddr, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return err
		}
		mid, err := address.IDFromAddress(minerAddr)
		if err != nil {
			return err
		}
		minerInfo, err := nodeAPI.StateMinerInfo(ctx, minerAddr, types.EmptyTSK)
		if err != nil {
			return err
		}
		head, err := nodeAPI.ChainHead(ctx)
		if err != nil {
			return err
		}

		var toCheck []*miner.SectorOnChainInfo
		var postRand abi.PoStRandomness = make([]byte, abi.RandomnessLength)
		_, _ = rand.Read(postRand)
		postRand[31] &= 0x3f

		onChainInfo, err := nodeAPI.StateMinerSectors(ctx, minerAddr, nil, types.EmptyTSK)
		if err != nil {
			return err
		}

		liveSectors := make(map[uint64]struct{})
		for dead := 0; dead < 48; dead++ {
			partitions, err := nodeAPI.StateMinerPartitions(ctx, minerAddr, uint64(dead), types.EmptyTSK)
			if err != nil {
				return xerrors.Errorf("getting partitions for deadline %d: %w", dead, err)
			}
			for _, partition := range partitions {
				count, err := partition.LiveSectors.Count()
				if err != nil {
					return err
				}
				sectors, err := partition.LiveSectors.All(count)
				if err != nil {
					return err
				}
				for _, sector := range sectors {
					liveSectors[uint64(sector)] = struct{}{}
				}
			}
		}

		var liveOnChainInfo []*miner.SectorOnChainInfo
		for _, info := range onChainInfo {
			if _, ok := liveSectors[uint64(info.SectorNumber)]; ok {
				liveOnChainInfo = append(liveOnChainInfo, info)
			}
		}

		if len(fileList) == 0 {
			toCheck = append(toCheck, liveOnChainInfo...)
		} else {
			for _, info := range onChainInfo {
				if info.Expiration >= head.Height() {
					if _, ok := fileList[uint64(info.SectorNumber)]; ok {
						toCheck = append(toCheck, info)
					}
				}
			}
		}

		var okCount int32
		for _, info := range toCheck {
			throttle <- struct{}{}
			wg.Add(1)
			go func(info *miner.SectorOnChainInfo) {
				defer func() {
					<-throttle
					wg.Done()
				}()
				ch, err := ffi.GeneratePoStFallbackSectorChallenges(minerInfo.WindowPoStProofType, abi.ActorID(mid), postRand, []abi.SectorNumber{
					info.SectorNumber,
				})
				if err != nil {
					fmt.Printf("%v,ERROR,%v\n", info.SectorNumber, strings.ReplaceAll(fmt.Sprintf("generating fallback challenges: %s", err), "\n", ""))
				}
				psi := ffi.PrivateSectorInfo{
					SectorInfo: proof.SectorInfo{
						SealProof:    info.SealProof,
						SectorNumber: info.SectorNumber,
						SealedCID:    info.SealedCID,
					},
					CacheDirPath:     path.Join(cachePath, fmt.Sprintf("s-t0%v-%v", mid, info.SectorNumber)),
					PoStProofType:    minerInfo.WindowPoStProofType,
					SealedSectorPath: path.Join(sealedPath, fmt.Sprintf("s-t0%v-%v", mid, info.SectorNumber)),
				}
				_, err = ffi.GenerateSingleVanillaProof(psi, ch.Challenges[info.SectorNumber])
				if err != nil {
					fmt.Printf("%v,ERROR,%v\n", info.SectorNumber, strings.ReplaceAll(fmt.Sprintf("generating VanillaProof: %s", err), "\n", ""))
				} else {
					atomic.AddInt32(&okCount, 1)
					fmt.Printf("%v,SUCCESS\n", info.SectorNumber)
				}

			}(info)
		}
		wg.Wait()
		fmt.Printf("total of %v sectors, %v successes, %v errors\n", len(toCheck), atomic.LoadInt32(&okCount), int32(len(toCheck))-atomic.LoadInt32(&okCount))
		return nil
	},
}

func readFile(path string) (map[uint64]struct{}, error) {
	sectors := make(map[uint64]struct{}, 10000)
	// 打开文件
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 创建一个新的Scanner，用于读取文件内容
	scanner := bufio.NewScanner(file)

	// 逐行读取文件内容
	for scanner.Scan() {
		line := scanner.Text()
		num, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			return nil, err
		}
		sectors[num] = struct{}{}
	}

	// 检查是否发生了错误
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return sectors, nil
}
