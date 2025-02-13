package main

import (
	"context"
	"fmt"
	b "math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var gasCmd = &cli.Command{
	Name:  "gas",
	Usage: "gas sum",
	Subcommands: []*cli.Command{
		dcDailyGasCmd,
		spDailyGasCmd,
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
			Name:  "parallel-block",
			Usage: "parallel traversal blocks",
			Value: 50,
		},
		&cli.IntFlag{
			Name:  "parallel-msg",
			Usage: "parallel traversal msgs",
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

		f099, _ := address.NewFromString("f099")
		f05, _ := address.NewFromString("f05")

		startEpoch, endEpoch, err := timeToHeight(cctx.String("date"))
		if err != nil {
			return err
		}

		cache := new(Cache)
		cache.store = make(map[address.Address]*Info)

		totalGas := abi.NewTokenAmount(0)
		var gasMu sync.Mutex

		pubGas := abi.NewTokenAmount(0)
		var pubMu sync.Mutex

		totalPower := abi.NewStoragePower(0)
		var powerMu sync.Mutex

		limit := make(chan int, cctx.Int("parallel-block"))
		wg := sync.WaitGroup{}

		msgLimit := make(chan int, cctx.Int("parallel-msg"))
		msgWg := sync.WaitGroup{}

		errChan := make(chan error, 2)
		go func() {
			for {
				select {
				case err := <-errChan:
					fmt.Println(err)
					fmt.Println(float64(totalGas.Uint64())/1e18, float64(totalPower.Uint64())/1024/1024/1024/1024)
					os.Exit(1)
				}
			}
		}()
		// 处理中断信号
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println(float64(totalGas.Uint64())/1e18, float64(totalPower.Uint64())/1024/1024/1024/1024)
			os.Exit(0)

		}()

		for i := startEpoch; i <= endEpoch; i++ {

			limit <- 0
			fmt.Printf("current height: %v,totalGas: %s\n", i, totalGas)
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
					msgLimit <- 0
					msgWg.Add(1)
					go func(msg api.Message) error {
						defer func() {
							<-msgLimit
							msgWg.Done()
						}()

						fmt.Println("current msg:", msg.Cid.String())

						var invocResult *api.InvocResult
						replay := func(msg api.Message) {
							invocResult, err = nodeAPI.StateReplay(ctx, head.Key(), msg.Cid)
							if err != nil {
								errChan <- err
							}
						}

						// //跳过失败的消息，因为暂时不好处理
						// if invocResult.MsgRct.ExitCode != 0 {
						// 	switch invocResult.Msg.Method {
						// 	case 6, 7, 25, 26:
						// 	default:
						// 		return nil
						// 	}
						// }

						//屏蔽无关消息，否则cache会缓存全网的SP
						switch msg.Message.Method {
						// PreCommitSector,ProveCommitSector,PreCommitSectorBatch,ProveCommitAggregate,ProveCommitSectors3
						case 6, 7, 25, 26, 34:
							replay(msg)
							info, err := cache.Get(invocResult.Msg.To, nodeAPI, ctx)
							if err != nil {
								errChan <- err
								return err
							}
							if info.isMiner && info.isDC {
								gas := invocResult.GasCost.TotalCost
								if len(invocResult.ExecutionTrace.Subcalls) > 0 {
									for _, call := range invocResult.ExecutionTrace.Subcalls {
										if call.Msg.Method == 0 && call.Msg.To == f099 {
											gas.Add(gas.Int, call.Msg.Value.Int)
										}

									}

								}
								gasMu.Lock()
								totalGas.Add(totalGas.Int, gas.Int)
								gasMu.Unlock()
							}

						case 4:
							replay(msg)
							if invocResult.Msg.To == f05 {
								pubMu.Lock()
								pubGas.Add(pubGas.Int, invocResult.GasCost.TotalCost.Int)
								pubMu.Unlock()
							}
						}
						return nil
					}(msg)

				}
				msgWg.Wait()
				return nil
			}(i)

		}
		wg.Wait()
		for mid := range cache.store {

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
					totalPower.Add(totalPower.Int, big.NewIntUnsigned(uint64(info.sectorSize)*(endSectorsCount.Live-startSectorsCount.Live)).Int)
					powerMu.Unlock()
				}
				return nil
			}(mid)

		}
		wg.Wait()
		fmt.Println(types.BigDivFloat(totalGas, big.NewInt(1e18)), types.BigDivFloat(pubGas, big.NewInt(1e18)), float64(totalPower.Uint64())/1024/1024/1024/1024)

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
	_, ok := c.store[key]
	if ok {
		return nil
	}

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
	endEpoch := (stamp.Unix()-bootstrapTime)/30 + 2879
	return startEpoch, endEpoch, nil
}

var spDailyGasCmd = &cli.Command{
	Name:  "spgas",
	Usage: "calculate the dc gas of the whole network on the specified date",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "date",
			Value:    "2023-06-01",
			Usage:    "specify date",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "minerid",
			Usage:    "specify minerid",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "parallel num",
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

		parChan := make(chan struct{}, cctx.Int("parallel"))
		errChan := make(chan error, 2)
		go func() {
			for err := range errChan {
				fmt.Println(err)
				os.Exit(1)
			}
		}()

		data := &struct {
			date       string
			startEpoch int64
			endEpoch   int64
			num        int
			power      abi.SectorSize
			pledge     abi.TokenAmount
			worker     abi.TokenAmount
			publish    abi.TokenAmount
			control    abi.TokenAmount
			err        abi.TokenAmount
		}{power: abi.SectorSize(0), pledge: abi.NewTokenAmount(0), worker: abi.NewTokenAmount(0), publish: abi.NewTokenAmount(0), control: abi.NewTokenAmount(0), err: abi.NewTokenAmount(0)}
		var dataLock sync.Mutex
		var dataWg sync.WaitGroup

		minerId, err := address.NewFromString(cctx.String("minerid"))
		if err != nil {
			return err
		}
		startEpoch, endEpoch, err := timeToHeight(cctx.String("date"))
		if err != nil {
			return err
		}

		data.date = cctx.String("date")
		data.startEpoch = startEpoch
		data.endEpoch = endEpoch

		startTipSet, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(startEpoch), types.EmptyTSK)
		if err != nil {
			return err
		}
		endTipSet, err := nodeAPI.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(endEpoch), types.EmptyTSK)
		if err != nil {
			return err
		}

		minerInfo, err := nodeAPI.StateMinerInfo(ctx, minerId, types.EmptyTSK)
		if err != nil {
			return err
		}
		// 计算高度之间的：扇区数量，质押币，存力
		dataWg.Add(1)
		go func() {
			defer func() {
				dataWg.Done()
			}()
			start, err := nodeAPI.StateMinerSectors(ctx, minerId, nil, startTipSet.Key())
			if err != nil {
				errChan <- err
			}
			end, err := nodeAPI.StateMinerSectors(ctx, minerId, nil, endTipSet.Key())
			if err != nil {
				errChan <- err
			}

			newCount := 0
			set := make(map[abi.SectorNumber]bool)
			for _, startOnChain := range start {
				set[startOnChain.SectorNumber] = true
			}

			for _, endOnChain := range end {
				if !set[endOnChain.SectorNumber] {
					newCount += 1
					data.pledge = big.Sum(data.pledge, endOnChain.InitialPledge)
				}
			}

			data.num = newCount
			data.power = abi.SectorSize(uint64(minerInfo.SectorSize) * uint64(newCount))
		}()

		sumGas := func(from address.Address, to address.Address, typ string) {
			defer func() {
				dataWg.Done()
			}()
			wMsgs, err := nodeAPI.StateListMessages(ctx, &api.MessageMatch{To: to, From: from}, endTipSet.Key(), abi.ChainEpoch(startEpoch))
			if err != nil {
				errChan <- err
			}

			TotalCost := abi.NewTokenAmount(0)
			errCost := abi.NewTokenAmount(0)
			var wlock sync.Mutex
			var w sync.WaitGroup

			for _, msgCid := range wMsgs {
				w.Add(1)
				parChan <- struct{}{}
				go func(cid cid.Cid) {
					defer func() {
						w.Done()
						<-parChan
					}()
					invocResult, err := nodeAPI.StateReplay(ctx, types.EmptyTSK, cid)
					if err != nil {
						errChan <- err
					}
					wlock.Lock()
					if invocResult.MsgRct.ExitCode == 0 {
						TotalCost = big.Sum(TotalCost, invocResult.GasCost.TotalCost)
						for _, sub := range invocResult.ExecutionTrace.Subcalls {
							if sub.Msg.Method == 0 {
								TotalCost = big.Sum(TotalCost, sub.Msg.Value)
							}

						}
					} else {
						errCost = big.Sum(errCost, invocResult.GasCost.TotalCost)
						for _, sub := range invocResult.ExecutionTrace.Subcalls {
							if sub.Msg.Method == 0 {
								errCost = big.Sum(errCost, sub.Msg.Value)
							}

						}

					}
					wlock.Unlock()
				}(msgCid)

			}
			w.Wait()

			dataLock.Lock()
			if typ == "worker" {
				data.worker = big.Sum(data.worker, TotalCost)
			} else if typ == "control" {
				data.control = big.Sum(data.control, TotalCost)
			} else if typ == "publish" {
				data.publish = big.Sum(data.publish, TotalCost)
			}

			data.err = big.Sum(data.err, errCost)
			dataLock.Unlock()
		}

		f05, err := address.NewFromString("f05")
		if err != nil {
			return err
		}
		worker, err := nodeAPI.StateAccountKey(ctx, minerInfo.Worker, types.EmptyTSK)
		if err != nil {
			return err
		}
		dataWg.Add(2)
		// 计算worker gas
		go sumGas(worker, minerId, "worker")
		// 计算publish gas
		go sumGas(worker, f05, "publish")

		for _, control := range minerInfo.ControlAddresses {
			addr, err := nodeAPI.StateAccountKey(ctx, control, types.EmptyTSK)
			if err != nil {
				return err
			}
			dataWg.Add(1)
			// 计算control gas
			go sumGas(addr, minerId, "control")
		}

		dataWg.Wait()

		fmt.Println(minerId)
		fmt.Printf("Statistics time: %s 00:00:00(%v) ~ 23:59:30(%v)\n", data.date, data.startEpoch, data.endEpoch)
		fmt.Printf("24h Add power: %v\n", data.power.ShortString())
		fmt.Printf("Add sector: %v\n", data.num)
		fmt.Printf("total pledge gas: %v\n", types.FIL(data.pledge).Short())
		fmt.Printf("total worker gas: %v\n", types.FIL(data.worker).Short())
		fmt.Printf("total publish gas: %v\n", types.FIL(data.publish).Short())
		fmt.Printf("total control gas: %v\n", types.FIL(data.control).Short())
		fmt.Printf("total error gas: %v\n", types.FIL(data.err).Short())

		fil := func(fil abi.TokenAmount) string {
			return new(b.Rat).SetFrac(fil.Int, b.NewInt(1e18)).FloatString(10)
		}
		fmt.Printf("%v,%v,%v,%v,%v,%v,%v,%v,%v\n", data.date, minerId, float64(data.power)/(1<<40), data.num, fil(data.pledge), fil(data.worker), fil(data.publish), fil(data.control), fil(data.err))

		return nil
	},
}
