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
	"sync"
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
	//ArgsUsage: "[amount (FIL)]",
	//Flags: []cli.Flag{
	//	&cli.StringFlag{
	//		Name:  "actor",
	//		Usage: "specify the address of miner actor",
	//	},
	//	&cli.IntFlag{
	//		Name:  "confidence",
	//		Usage: "number of block confirmations to wait for",
	//		Value: int(build.MessageConfidence),
	//	},
	//	&cli.BoolFlag{
	//		Name:  "beneficiary",
	//		Usage: "send withdraw message from the beneficiary address",
	//	},
	//},
	Action: func(cctx *cli.Context) error {

		nodeAPI, acloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer acloser()

		ctx := lcli.ReqContext(cctx)

		startEpoch := 2908080
		endEpoch := 2910960
		cache := new(Cache)
		fmt.Println(cache)

		cache.store = make(map[address.Address]*Info)

		totalGas := abi.NewTokenAmount(0)
		var gasMu sync.Mutex

		totalPower := abi.NewStoragePower(0)
		var powerMu sync.Mutex

		limit := make(chan int, 100)
		wg := sync.WaitGroup{}

		//totalChan := make(chan abi.TokenAmount, 100)
		//go func() {
		//	for {
		//		select {
		//		case data := <-totalChan:
		//			totalGas.Add(totalGas.Int, data.Int)
		//			fmt.Println("total gas", float64(totalGas.Uint64())/1e18)
		//		}
		//	}
		//
		//}()

		for i := startEpoch; i <= endEpoch; i++ {

			limit <- 0
			fmt.Println(i)
			wg.Add(1)
			go func(i int) error {
				defer func() {
					<-limit
					wg.Done()
				}()
				head, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(i), types.EmptyTSK)
				if err != nil {
					return err
				}
				msgs, err := nodeAPI.ChainGetMessagesInTipset(ctx, head.Key())
				if err != nil {
					return err
				}
				for _, msg := range msgs {
					fmt.Println(msg, totalGas.String(), totalPower.String())
					invocResult, err := nodeAPI.StateReplay(ctx, head.Key(), msg.Cid)
					//fmt.Println(invocResult.Msg)
					if err != nil {
						fmt.Println(msg, err)
						return err
					}
					//if invocResult.MsgRct.ExitCode != 0 {
					//	continue
					//}
					info, err := cache.Get(invocResult.Msg.To, nodeAPI, ctx)
					if err != nil {
						return err
					}
					if info.isMiner && info.isDC {
						if invocResult.Msg.Method == 6 || invocResult.Msg.Method == 7 || invocResult.Msg.Method == 25 || invocResult.Msg.Method == 26 {
							gas := invocResult.GasCost.TotalCost
							fmt.Println(invocResult.GasCost.TotalCost)
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
						return err
					}
					startSectorsCount, err := nodeAPI.StateMinerSectorCount(ctx, mid, startHead.Key())
					if err != nil {
						return err
					}
					endHead, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(endEpoch), types.EmptyTSK)
					if err != nil {
						return err
					}
					endSectorsCount, err := nodeAPI.StateMinerSectorCount(ctx, mid, endHead.Key())
					if err != nil {
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
	//fmt.Println("this3")
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
	//fmt.Println("this4")
	return c.store[key], nil
}

func (c *Cache) Set(key address.Address, nodeAPI v0api.FullNode, ctx context.Context) error {
	//fmt.Println("this1")
	c.mu.Lock()
	//fmt.Println("this11")
	defer c.mu.Unlock()

	c.store[key] = new(Info)
	//fmt.Println("this111")
	actor, err := nodeAPI.StateGetActor(ctx, key, types.EmptyTSK)
	//fmt.Println("this1111")
	if err != nil {
		fmt.Println(err)
		return err
	}
	isMiner := builtin.IsStorageMinerActor(actor.Code)
	//fmt.Println(isMiner, "isMiner")
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
	//fmt.Println("this2")
	return nil

}
