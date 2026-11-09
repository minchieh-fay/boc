package main

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// Application 组合配置、行情队列和 Binance 交易接口。
type Application struct {
	cfg      Config
	market   *MarketData
	exchange *Exchange
}

// newApplication 加载配置并创建行情和交易服务。
func newApplication(configPath string) (*Application, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}
	market := NewMarketData(cfg.KlineCount)
	exchange, err := NewExchange(cfg, market)
	if err != nil {
		return nil, err
	}
	return &Application{cfg: cfg, market: market, exchange: exchange}, nil
}

// run 初始化市场数据，然后并行维护五分钟区间和交易扫描。
func (a *Application) run(ctx context.Context) error {
	// step.1 加载合约、历史 K 线并启动实时行情订阅。
	symbols, err := a.exchange.Initialize(ctx)
	if err != nil {
		return err
	}
	if err := a.exchange.InitializeHistory(ctx, symbols); err != nil {
		return err
	}
	go a.runMarketSocket(ctx, symbols)
	// step.2 以独立周期轮换行情窗口并执行交易扫描。
	rotate := time.NewTicker(5 * time.Minute)
	trade := time.NewTicker(time.Duration(a.cfg.TradeIntervalSecond) * time.Second)
	defer rotate.Stop()
	defer trade.Stop()
	log.Printf("strategy started symbols=%d kline_count=%d trade_interval=%ds",
		len(symbols), a.cfg.KlineCount, a.cfg.TradeIntervalSecond)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-rotate.C:
			log.Printf("rotated kline windows symbols=%d", a.market.Rotate(now, time.Minute))
		case <-trade.C:
			go func() {
				if err := a.exchange.ManagePositions(ctx); err != nil {
					log.Printf("position management cycle failed: %v", err)
				}
			}()
			a.handleTradeCycle(ctx)
		}
	}
}

// runMarketSocket 订阅全体合约的聚合成交价，断线后持续重连。
func (a *Application) runMarketSocket(ctx context.Context, symbols []string) {
	// step.1 维持全市场行情订阅并把成交推入当前五分钟区间。
	for ctx.Err() == nil {
		// help_* 在本目录 help.go 中封装行情 WebSocket 的单次订阅。
		done, stop, err := help_serveAggTrades(symbols, func(event *futures.WsAggTradeEvent) {
			a.market.UpdatePrice(event.Symbol, event.Price)
		}, func(err error) { log.Printf("market websocket: %v", err) })
		if err != nil {
			log.Printf("market websocket connect: %v", err)
			sleepContext(ctx, 5*time.Second)
			continue
		}
		select {
		case <-ctx.Done():
			close(stop)
			return
		case <-done:
			log.Printf("market websocket closed; reconnecting")
			sleepContext(ctx, 2*time.Second)
		}
	}
}

// handleTradeCycle 按窗口振幅选币，过滤持仓并按配置方向尝试开仓。
func (a *Application) handleTradeCycle(ctx context.Context) {
	// step.1 获取账户持仓，达到最大仓位数时结束本轮。
	positions, positionCount, err := a.exchange.Positions(ctx)
	if err != nil {
		log.Printf("trade cycle skipped: %v", err)
		return
	}
	if positionCount >= a.cfg.MaxSymbols {
		log.Printf("trade cycle skipped: position limit reached (%d)", positionCount)
		return
	}
	// step.2 按窗口振幅降序选择未持仓的候选交易对。
	candidates := a.market.Candidates(a.cfg.MinMoveRate, time.Now(),
		time.Duration(a.cfg.StalePriceSeconds)*time.Second)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Rate > candidates[j].Rate })
	openedSymbols := make(map[string]bool)
	for _, candidate := range candidates {
		if positions[candidate.Symbol] || openedSymbols[candidate.Symbol] {
			continue
		}
		trendMode, ok := a.market.TrendMode(candidate.Symbol)
		if !ok {
			log.Printf("trend signal skipped symbol=%s: incomplete kline window", candidate.Symbol)
			continue
		}
		tradeMode := trendMode
		if a.cfg.TradeMode == 1 && trendMode != 0 || a.cfg.TradeMode == 2 && trendMode != 1 {
			continue
		}
		if positionCount+1 > a.cfg.MaxSymbols {
			break
		}
		// step.3 按配置方向使用 GTX 限价挂单开仓并挂保护单。
		openErr := a.exchange.Open(ctx, candidate.Symbol, candidate.Price, tradeMode)
		if openErr != nil {
			log.Printf("open skipped symbol=%s range=%.4f%%: %v",
				candidate.Symbol, candidate.Rate*100, openErr)
		} else {
			openedSymbols[candidate.Symbol] = true
			log.Printf("opened symbol=%s mode=%d range=%.4f%%",
				candidate.Symbol, tradeMode, candidate.Rate*100)
		}
		positions, positionCount, err = a.exchange.Positions(ctx)
		if err != nil {
			log.Printf("position refresh failed after entry attempt: %v", err)
			return
		}
	}
}

// sleepContext 在重连等待期间响应退出信号。
func sleepContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
