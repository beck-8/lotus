package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"os"
	"sync"
	"time"
)

var gasCmd = &cli.Command{
	Name:  "gas",
	Usage: "gas sum",
	Subcommands: []*cli.Command{
		dcDailyGasCmd,
	},
}

var dcDailyGasCmd = &cli.Command{
	Name:  "dcgas",
	Usage: "calculate the dc gas of the whole network on the specified date",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "date",
			Value:    "2023-06-01",
			Usage:    "specify date",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "concurrency",
			Usage: "how many concurrent request calculations",
			Value: 50,
		},
	},
	Action: func(cctx *cli.Context) error {

		nodeAPI, acloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer acloser()

		ctx := lcli.ReqContext(cctx)

		startEpoch, endEpoch, err := timeToHeight(cctx.String("date"))
		if err != nil {
			return err
		}

		cache := new(Cache)
		cache.store = make(map[address.Address]*Info)

		totalGas := abi.NewTokenAmount(0)
		var gasMu sync.Mutex

		totalPower := abi.NewStoragePower(0)
		var powerMu sync.Mutex

		limit := make(chan int, cctx.Int("concurrency"))
		wg := sync.WaitGroup{}

		errChan := make(chan error, 2)
		go func() {
			for {
				select {
				case err := <-errChan:
					fmt.Println(err)
					os.Exit(1)
				}
			}
		}()

		for i := startEpoch; i <= endEpoch; i++ {

			limit <- 0
			fmt.Println("current height:", i)
			wg.Add(1)
			go func(i int64) error {
				defer func() {
					<-limit
					wg.Done()
				}()
				head, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(i), types.EmptyTSK)
				if err != nil {
					errChan <- err
					return err
				}
				msgs, err := nodeAPI.ChainGetMessagesInTipset(ctx, head.Key())
				if err != nil {
					errChan <- err
					return err
				}
				for _, msg := range msgs {
					fmt.Println("current msg:", msg.Cid.String())
					invocResult, err := nodeAPI.StateReplay(ctx, head.Key(), msg.Cid)
					if err != nil {
						errChan <- err
						return err
					}
					//跳过失败的消息，因为暂时不好处理
					if invocResult.MsgRct.ExitCode != 0 {
						switch invocResult.Msg.Method {
						case 6, 7, 25, 26:
						default:
							continue
						}

					}
					//屏蔽无关消息，否则cache会缓存全网的SP
					switch invocResult.Msg.Method {
					case 6, 7, 25, 26:
						info, err := cache.Get(invocResult.Msg.To, nodeAPI, ctx)
						if err != nil {
							errChan <- err
							return err
						}
						if info.isMiner && info.isDC {
							gas := invocResult.GasCost.TotalCost
							if len(invocResult.ExecutionTrace.Subcalls) > 0 {
								for _, call := range invocResult.ExecutionTrace.Subcalls {
									gas.Add(gas.Int, call.Msg.Value.Int)
								}

							}
							gasMu.Lock()
							totalGas.Add(totalGas.Int, gas.Int)
							gasMu.Unlock()
						}
					}

				}
				return nil
			}(i)

		}
		wg.Wait()
		for mid, _ := range cache.store {

			limit <- 0
			wg.Add(1)
			go func(mid address.Address) error {
				defer func() {
					<-limit
					wg.Done()
				}()
				info, _ := cache.Get(mid, nodeAPI, ctx)

				if info.isMiner && info.isDC {
					startHead, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(startEpoch), types.EmptyTSK)
					if err != nil {
						errChan <- err
						return err
					}
					startSectorsCount, err := nodeAPI.StateMinerSectorCount(ctx, mid, startHead.Key())
					if err != nil {
						errChan <- err
						return err
					}
					endHead, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(endEpoch), types.EmptyTSK)
					if err != nil {
						errChan <- err
						return err
					}
					endSectorsCount, err := nodeAPI.StateMinerSectorCount(ctx, mid, endHead.Key())
					if err != nil {
						errChan <- err
						return err
					}
					powerMu.Lock()
					totalPower.Add(totalPower.Int, big.NewIntUnsigned(uint64(info.sectorSize)*(endSectorsCount.Active-startSectorsCount.Active)).Int)
					powerMu.Unlock()
				}
				return nil
			}(mid)

		}
		wg.Wait()
		fmt.Println(float64(totalGas.Uint64())/1e18, float64(totalPower.Uint64())/1024/1024/1024/1024)

		return nil
	},
}

type Info struct {
	sectorSize abi.SectorSize
	isDC       bool
	isMiner    bool
}
type Cache struct {
	mu    sync.RWMutex
	store map[address.Address]*Info
}

func (c *Cache) Get(key address.Address, nodeAPI v0api.FullNode, ctx context.Context) (*Info, error) {
	c.mu.RLock()
	val, ok := c.store[key]
	c.mu.RUnlock()
	if ok {
		return val, nil
	}

	err := c.Set(key, nodeAPI, ctx)
	if err != nil {
		return c.store[key], err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store[key], nil
}

func (c *Cache) Set(key address.Address, nodeAPI v0api.FullNode, ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.store[key] = new(Info)
	actor, err := nodeAPI.StateGetActor(ctx, key, types.EmptyTSK)
	if err != nil {
		fmt.Println(err)

		return err
	}
	isMiner := builtin.IsStorageMinerActor(actor.Code)
	if isMiner {
		minerPower, err := nodeAPI.StateMinerPower(ctx, key, types.EmptyTSK)
		if err != nil {
			return err
		}
		minerInfo, err := nodeAPI.StateMinerInfo(ctx, key, types.EmptyTSK)
		if err != nil {
			return err
		}
		c.store[key].sectorSize = minerInfo.SectorSize
		c.store[key].isDC = minerPower.MinerPower.QualityAdjPower.GreaterThan(minerPower.MinerPower.RawBytePower)
	}

	c.store[key].isMiner = isMiner
	return nil

}

func timeToHeight(text string) (int64, int64, error) {
	// 主网启动时间
	bootstrapTime := int64(1598306400)
	// 中国时区
	loc, _ := time.LoadLocation("PRC")
	stamp, err := time.ParseInLocation("2006-1-2", text, loc)
	if err != nil {
		return 0, 0, err
	}
	startEpoch := (stamp.Unix() - bootstrapTime) / 30
	endEpoch := (stamp.Unix()-bootstrapTime)/30 + 2880
	return startEpoch, endEpoch, nil
}
