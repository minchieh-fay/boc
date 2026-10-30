package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// Application 组合配置、交易所、行情缓存和统计存储。
type Application struct {
	cfg      Config
	market   *MarketData
	exchange *Exchange
	state    *StateStore
	tracked  map[string]OpenTrade
}

// OpenTrade 保存进程内开仓参考价、方向和估算数量。
type OpenTrade struct {
	Symbol    string
	Direction string
	Long      bool
	Entry     float64
	Quantity  float64
}

// newApplication 加载配置和统计文件，并创建交易服务。
func newApplication(configPath, statePath string) (*Application, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}
	market := NewMarketData()
	exchange, err := NewExchange(cfg, market)
	if err != nil {
		return nil, err
	}
	state, err := NewStateStore(statePath)
	if err != nil {
		return nil, fmt.Errorf("加载交易统计: %w", err)
	}
	return &Application{
		cfg: cfg, market: market, exchange: exchange, state: state,
		tracked: make(map[string]OpenTrade),
	}, nil
}

// run 初始化账户和行情订阅，再按配置周期执行策略。
func (a *Application) run(ctx context.Context) error {
	// step.1 加载合约并启动全市场行情订阅。
	symbols, err := a.exchange.Initialize(ctx)
	if err != nil {
		return err
	}
	go a.runMarketSocket(ctx, symbols)
	// step.2 按用户周期执行账户检查和策略评估。
	ticker := time.NewTicker(time.Duration(a.cfg.IntervalMinutes) * time.Minute)
	defer ticker.Stop()
	log.Printf("strategy started symbols=%d interval=%dm mode=%s", len(symbols), a.cfg.IntervalMinutes, a.cfg.Strategy)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.handleCycle(ctx)
		}
	}
}

// runMarketSocket 订阅全体合约的聚合成交价，遇到断线后持续重新连接。
func (a *Application) runMarketSocket(ctx context.Context, symbols []string) {
	// step.1 订阅多合约行情，并在连接结束或失败后重新连接。
	for ctx.Err() == nil {
		// help_* 在本目录 help.go 中封装行情 WebSocket 的单次订阅。
		done, stop, err := help_serveAggTrades(symbols, func(event *futures.WsAggTradeEvent) {
			a.market.UpdatePrice(event.Symbol, event.Price)
		}, func(err error) { log.Printf("market websocket: %v", err) })
		if err != nil {
			log.Printf("market websocket connect: %v", err)
			a.market.ResetBaselines()
			sleepContext(ctx, 5*time.Second)
			continue
		}
		select {
		case <-ctx.Done():
			close(stop)
			return
		case <-done:
			log.Printf("market websocket closed; reconnecting")
			a.market.ResetBaselines()
			sleepContext(ctx, 2*time.Second)
		}
	}
}

// handleCycle 每个周期读取实时仓位，评估最大波动并执行符合条件的开仓。
func (a *Application) handleCycle(ctx context.Context) {
	maxAge := time.Duration(a.cfg.StalePriceSeconds) * time.Second
	// step.1 获取账户实际持仓，避免本地状态与交易所状态不一致。
	positions, positionCount, err := a.exchange.Positions(ctx)
	if err != nil {
		log.Printf("cycle skipped: %v", err)
		a.market.AdvanceBaseline(maxAge)
		return
	}
	// step.1.1 对照上一周期记录，估算刚刚结束的仓位盈亏。
	a.reconcileClosedTrades(positions)
	// step.2 选择幅度最大的合约，并检查幅度阈值、持仓数和重复交易对限制。
	symbol, movement, price, ok := a.market.BestMove(a.cfg.MinMoveRate, maxAge)
	if !ok {
		if symbol == "" {
			log.Printf("no fresh price data available; minimum move %.4f%%", a.cfg.MinMoveRate*100)
		} else {
			log.Printf("no signal: largest move symbol=%s move=%.4f%% minimum=%.4f%%",
				symbol, movement*100, a.cfg.MinMoveRate*100)
		}
		a.market.AdvanceBaseline(maxAge)
		return
	}
	if _, alreadyOpen := positions[symbol]; alreadyOpen {
		log.Printf("signal skipped: %s already has a position", symbol)
		a.market.AdvanceBaseline(maxAge)
		return
	}
	neededSlots := 1
	if a.cfg.Strategy == "hedge" {
		neededSlots = 2
	}
	if positionCount+neededSlots > a.cfg.MaxSymbols {
		log.Printf("signal skipped: position limit reached (%d)", a.cfg.MaxSymbols)
		a.market.AdvanceBaseline(maxAge)
		return
	}
	// step.3 执行开仓和保护单流程，随后推进本周期基准价格。
	current, previous, hasPrice := a.market.Prices(symbol)
	if hasPrice && current < previous {
		movement = -movement
	}
	directions := a.exchange.directions(movement)
	if err := a.exchange.Open(ctx, symbol, price, movement); err != nil {
		// 调用本目录辅助函数提取币安返回的原始 API 错误码。
		code, hasCode := help_apiErrorCode(err)
		if hasCode {
			log.Printf("open skipped symbol=%s move=%.4f%% binance_code=%d error=%v",
				symbol, movement*100, code, err)
		} else {
			log.Printf("open skipped symbol=%s move=%.4f%% binance_code=unavailable error=%v",
				symbol, movement*100, err)
		}
	} else {
		for _, direction := range directions {
			if err := a.state.RecordOpen(direction.name); err != nil {
				log.Printf("persist trade count symbol=%s: %v", symbol, err)
			}
			a.tracked[tradeKey(symbol, direction.positionSide == futures.PositionSideTypeLong)] = OpenTrade{
				Symbol: symbol, Direction: direction.name,
				Long:  direction.positionSide == futures.PositionSideTypeLong,
				Entry: price, Quantity: a.cfg.TradeUSDT * float64(a.cfg.Leverage) / price,
			}
		}
	}
	a.market.AdvanceBaseline(maxAge)
}

// reconcileClosedTrades 用行情缓存估算已消失仓位的盈亏并更新汇总。
func (a *Application) reconcileClosedTrades(positions map[string]bool) {
	// step.1 找出账户已不存在的方向仓位，并用最新缓存价格估算盈亏。
	for key, trade := range a.tracked {
		if positions[tradeKey(trade.Symbol, trade.Long)] {
			continue
		}
		current, _, ok := a.market.Prices(trade.Symbol)
		if !ok || current <= 0 {
			continue
		}
		pnl := (current - trade.Entry) * trade.Quantity
		if !trade.Long {
			pnl = -pnl
		}
		if err := a.state.RecordClose(trade.Direction, pnl); err != nil {
			log.Printf("persist close estimate symbol=%s: %v", trade.Symbol, err)
		}
		log.Printf("closed estimate symbol=%s direction=%s pnl=%.6f", trade.Symbol, trade.Direction, pnl)
		delete(a.tracked, key)
	}
}

// tradeKey 为同一个交易对的多空仓位生成不同的进程内索引。
func tradeKey(symbol string, long bool) string {
	if long {
		return symbol + ":LONG"
	}
	return symbol + ":SHORT"
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
