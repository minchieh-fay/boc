package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// Exchange 封装行情初始化、账户仓位读取和真实下单流程。
type Exchange struct {
	client        *futures.Client
	cfg           Config
	market        *MarketData
	stateMu       sync.Mutex
	managing      atomic.Bool
	positionState map[string]PositionState
}

// PositionState 保存策略仓位和保护单信息，供进程重启后继续管理。
type PositionState struct {
	Symbol              string                   `json:"symbol"`
	Direction           futures.PositionSideType `json:"direction"`
	Quantity            float64                  `json:"quantity"`
	EntryPrice          float64                  `json:"entry_price"`
	AnchorPrice         float64                  `json:"anchor_price"`
	StopLossPrice       float64                  `json:"stop_loss_price"`
	TakeProfitAlgoID    int64                    `json:"take_profit_algo_id"`
	StopLossAlgoID      int64                    `json:"stop_loss_algo_id"`
	ProfitTargetReached bool                     `json:"profit_target_reached"`
	ExitStarted         bool                     `json:"exit_started"`
	StopRefreshPending  bool                     `json:"stop_refresh_pending"`
}

// NewExchange 从两行密钥文件创建 Binance USDⓈ-M 合约客户端。
func NewExchange(cfg Config, market *MarketData) (*Exchange, error) {
	// step.1 读取 API 凭证并根据环境配置创建合约客户端。
	data, err := os.ReadFile(cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("读取密钥文件: %w", err)
	}
	lines := strings.Fields(string(data))
	if len(lines) != 2 {
		return nil, fmt.Errorf("密钥文件必须包含 API key 和 secret 两行")
	}
	if cfg.Testnet {
		futures.UseTestnet = true
	}
	client := futures.NewClient(lines[0], lines[1])
	exchange := &Exchange{client: client, cfg: cfg, market: market,
		positionState: make(map[string]PositionState)}
	if err := exchange.loadPositionState(); err != nil {
		return nil, err
	}
	return exchange, nil
}

// Initialize 加载可交易合约信息和本地下单规则。
func (e *Exchange) Initialize(ctx context.Context) ([]string, error) {
	// step.1 加载交易所合约信息及本地下单过滤规则。
	info, err := e.client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取合约信息: %w", err)
	}
	symbols := e.market.LoadSymbols(info.Symbols)
	if len(symbols) == 0 {
		return nil, fmt.Errorf("未找到带有有效过滤规则的 USDT 永续合约")
	}
	return symbols, nil
}

// InitializeHistory 按交易对限速下载历史五分钟 K 线并初始化策略窗口。
func (e *Exchange) InitializeHistory(ctx context.Context, symbols []string) error {
	// step.1 逐个交易对获取配置数量的已收盘 K 线并写入滚动窗口。
	for _, symbol := range symbols {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		limit := e.cfg.KlineCount + 1
		if limit > 1500 {
			limit = 1500
		}
		klines, err := e.client.NewKlinesService().Symbol(symbol).
			Interval("5m").Limit(limit).Do(ctx)
		if err != nil {
			return fmt.Errorf("读取 %s 历史 K 线: %w", symbol, err)
		}
		data := make([]MyKline, 0, len(klines))
		now := time.Now().UnixMilli()
		for _, kline := range klines {
			if kline.CloseTime >= now {
				continue
			}
			high, highErr := strconv.ParseFloat(kline.High, 64)
			low, lowErr := strconv.ParseFloat(kline.Low, 64)
			if highErr != nil || lowErr != nil || high <= 0 || low <= 0 {
				continue
			}
			data = append(data, MyKline{Timestamp: kline.OpenTime, High: high, Low: low})
		}
		e.market.SetHistory(symbol, data)
	}
	return nil
}

// Positions 查询账户实时仓位并返回有持仓的 USDT 永续交易对集合。
func (e *Exchange) Positions(ctx context.Context) (map[string]bool, int, error) {
	// step.1 读取账户实时仓位，汇总非零的 USDT 合约方向和交易对。
	positions, err := e.client.NewGetPositionRiskService().Do(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("读取账户仓位: %w", err)
	}
	opened := make(map[string]bool)
	count := 0
	for _, position := range positions {
		if !strings.HasSuffix(position.Symbol, "USDT") {
			continue
		}
		amount, parseErr := strconv.ParseFloat(position.PositionAmt, 64)
		if parseErr == nil && amount != 0 {
			opened[position.Symbol] = true
			count++
		}
	}
	return opened, count, nil
}

// Open 按方向配置以 GTX 限价单开仓，并为成交仓位创建保护单。
func (e *Exchange) Open(ctx context.Context, symbol string, price float64, tradeMode int) error {
	// step.1 校验交易规则、设置杠杆并解析需要交易的方向。
	filter, ok := e.market.Filter(symbol)
	if !ok || price <= 0 {
		return fmt.Errorf("交易对 %s 缺少有效行情或过滤规则", symbol)
	}
	if _, err := e.client.NewChangeLeverageService().Symbol(symbol).
		Leverage(e.cfg.Leverage).Do(ctx); err != nil {
		return fmt.Errorf("%s 设置杠杆失败: %w", symbol, err)
	}
	directions := tradeDirections(tradeMode)
	// step.2 逐个方向维护 maker 入场单，并为实际成交数量挂保护单。
	for _, direction := range directions {
		if err := e.openDirection(ctx, symbol, price, filter, direction); err != nil {
			return err
		}
	}
	return nil
}

// openDirection 维护单方向的 GTX 限价单，汇总部分成交后设置保护单。
func (e *Exchange) openDirection(ctx context.Context, symbol string, price float64,
	filter SymbolFilter, direction orderDirection) error {
	// step.1 按资金和精度计算目标数量，并以被动价格提交限价单。
	// 调用本目录辅助函数，按交易所步进向下取整下单数量。
	quantity := help_floorStep(e.cfg.TradeUSDT*float64(e.cfg.Leverage)/price, filter.QtyStep)
	if quantity < filter.MinQty || quantity <= 0 {
		return fmt.Errorf("%s 计算数量低于合约最小数量", symbol)
	}
	// 调用本目录辅助函数，将下单目标数量格式化为交易所接受的精度。
	qtyText := help_formatStep(quantity, filter.QtyStep)
	filled, quote, err := e.placeAndReprice(ctx, symbol, qtyText, price, filter, direction)
	if filled <= 0 {
		if err != nil {
			return err
		}
		return fmt.Errorf("%s %s GTX 挂单超时，未成交", symbol, direction.name)
	}
	entry := quote / filled
	// 调用本目录辅助函数，将实际成交量格式化为保护单失败时的平仓参数。
	filledText := help_formatStep(filled, filter.QtyStep)
	protectCtx := ctx
	var cancelProtect context.CancelFunc
	if ctx.Err() != nil {
		protectCtx, cancelProtect = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancelProtect()
	}
	// step.2 按加权成交价为真实成交仓位设置止盈和止损保护。
	state, protectErr := e.placeInitialProtection(
		protectCtx, symbol, entry, filled, filter, direction,
	)
	if protectErr != nil {
		if closeErr := e.closeDirection(protectCtx, symbol, filledText, direction); closeErr != nil {
			return fmt.Errorf("%s 保护单失败: %v；紧急平仓也失败: %w", symbol, protectErr, closeErr)
		}
		return fmt.Errorf("%s 保护单失败，已紧急平仓: %w", symbol, protectErr)
	}
	if err := e.savePositionState(state); err != nil {
		_ = e.cancelAlgoOrder(protectCtx, state.TakeProfitAlgoID)
		_ = e.cancelAlgoOrder(protectCtx, state.StopLossAlgoID)
		if closeErr := e.closeDirection(protectCtx, symbol, filledText, direction); closeErr != nil {
			return fmt.Errorf("%s 保存管理状态失败: %v；紧急平仓失败: %w", symbol, err, closeErr)
		}
		return fmt.Errorf("%s 保存管理状态失败，已紧急平仓: %w", symbol, err)
	}
	// 调用本目录辅助函数输出真实成交数量和均价。
	help_logTrade(symbol, direction.name, entry, 0, filled)
	if err != nil {
		return fmt.Errorf("%s 已保护已成交仓位，但挂单流程被中断: %w", symbol, err)
	}
	return nil
}

// placeAndReprice 按配置周期撤单重挂，并在 GTX 穿价拒绝时下移一个档位。
func (e *Exchange) placeAndReprice(ctx context.Context, symbol, quantity string, reference float64,
	filter SymbolFilter, direction orderDirection) (float64, float64, error) {
	// step.1 在总超时内按最新行情提交被动限价单。
	deadline := time.Now().Add(time.Duration(e.cfg.EntryTimeoutSeconds) * time.Second)
	filledTotal, quoteTotal := 0.0, 0.0
	targetQuantity, _ := strconv.ParseFloat(quantity, 64)
	repriceEvery := time.Duration(e.cfg.EntryRepriceSeconds * float64(time.Second))
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return filledTotal, quoteTotal, ctx.Err()
		default:
		}
		remainingQuantity := targetQuantity - filledTotal
		if remainingQuantity <= 0 {
			return filledTotal, quoteTotal, nil
		}
		// 调用本目录辅助函数，格式化撤单后重新挂单的剩余数量。
		remainingText := help_formatStep(remainingQuantity, filter.QtyStep)
		latest, ok := e.market.LastPrice(symbol)
		if !ok {
			latest = reference
		}
		price := makerEntryPrice(latest, filter.PriceTick, direction.entrySide)
		var order *futures.CreateOrderResponse
		for rejects := 0; rejects < 5; rejects++ {
			// 调用本目录辅助函数，将限价对齐到不会主动吃单的价格档位。
			priceText := help_formatStep(price, filter.PriceTick)
			created, err := e.client.NewCreateOrderService().Symbol(symbol).Side(direction.entrySide).
				PositionSide(direction.positionSide).Type(futures.OrderTypeLimit).
				TimeInForce(futures.TimeInForceTypeGTX).Quantity(remainingText).
				Price(priceText).NewOrderResponseType(futures.NewOrderRespTypeRESULT).Do(ctx)
			if err == nil {
				order = created
				break
			}
			// help_* 调用点：确认是否因 GTX 穿价被拒，避免重试其他下单错误。
			if !help_isPostOnlyCross(err) {
				return filledTotal, quoteTotal, fmt.Errorf("%s %s GTX 开仓失败: %w", symbol, direction.name, err)
			}
			price = nextMakerPrice(price, filter.PriceTick, direction.entrySide)
		}
		if order == nil {
			return filledTotal, quoteTotal, fmt.Errorf("%s GTX 连续穿价拒绝，停止重挂", symbol)
		}
		remaining, err := e.waitAndCancelEntry(ctx, symbol, order.OrderID, repriceEvery)
		if err != nil {
			return filledTotal, quoteTotal, err
		}
		filled, quote := orderFill(remaining)
		filledTotal += filled
		quoteTotal += quote
		if remaining.Status == futures.OrderStatusTypeFilled || filledTotal >= targetQuantity {
			return filledTotal, quoteTotal, nil
		}
		if time.Now().Add(repriceEvery).After(deadline) {
			break
		}
	}
	return filledTotal, quoteTotal, nil
}

// waitAndCancelEntry 等待一个报价周期，读取最终成交并取消未成交余量。
func (e *Exchange) waitAndCancelEntry(ctx context.Context, symbol string, orderID int64,
	duration time.Duration) (*futures.Order, error) {
	return e.waitAndCancelEntryPaced(ctx, symbol, orderID, duration, 0)
}

// waitAndCancelManagedEntry 为持仓退出单限速等待并确认撤单后的最终成交。
func (e *Exchange) waitAndCancelManagedEntry(
	ctx context.Context, symbol string, orderID int64, duration time.Duration,
) (*futures.Order, error) {
	return e.waitAndCancelEntryPaced(ctx, symbol, orderID, duration, 100*time.Millisecond)
}

// waitAndCancelEntryPaced 等待报价周期后撤单，并按指定间隔查询最终状态。
func (e *Exchange) waitAndCancelEntryPaced(
	ctx context.Context, symbol string, orderID int64, duration, requestDelay time.Duration,
) (*futures.Order, error) {
	// step.1 在重挂周期内轮询订单状态，到期撤销剩余数量。
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		cancelCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return e.cancelAndReadEntry(cancelCtx, symbol, orderID, requestDelay)
	case <-timer.C:
	}
	// step.2 到期或退出时撤单，并读取已成交数量供后续重挂。
	return e.cancelAndReadEntry(ctx, symbol, orderID, requestDelay)
}

// cancelAndReadEntry 确认挂单已结束后返回最终成交，避免旧报价仍留在市场。
func (e *Exchange) cancelAndReadEntry(ctx context.Context, symbol string,
	orderID int64, requestDelay time.Duration) (*futures.Order, error) {
	// step.1 将活动挂单撤销并读取最终成交信息。
	for attempt := 0; attempt < 3; attempt++ {
		cancelled, cancelErr := e.client.NewCancelOrderService().
			Symbol(symbol).OrderID(orderID).Do(ctx)
		if requestDelay > 0 {
			if err := waitRequestDelay(ctx, requestDelay); err != nil {
				return nil, err
			}
		}
		order, queryErr := e.client.NewGetOrderService().Symbol(symbol).OrderID(orderID).Do(ctx)
		if queryErr == nil && order.Status != futures.OrderStatusTypeNew &&
			order.Status != futures.OrderStatusTypePartiallyFilled {
			return order, nil
		}
		if cancelErr == nil && queryErr != nil {
			return &futures.Order{
				Status: cancelled.Status, ExecutedQuantity: cancelled.ExecutedQuantity,
				CumQuote: cancelled.CumQuote, Price: cancelled.Price,
			}, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	// step.2 多次请求仍无法确认订单终态时返回错误，避免重复挂单。
	return nil, fmt.Errorf("%s 无法确认 GTX 挂单 %d 已撤销", symbol, orderID)
}

// orderFill 读取订单累计成交数量和成交额，缺少成交额时按均价估算。
func orderFill(order *futures.Order) (float64, float64) {
	quantity, _ := strconv.ParseFloat(order.ExecutedQuantity, 64)
	quote, _ := strconv.ParseFloat(order.CumQuote, 64)
	if quote <= 0 && quantity > 0 {
		price, _ := strconv.ParseFloat(order.AvgPrice, 64)
		if price <= 0 {
			price, _ = strconv.ParseFloat(order.Price, 64)
		}
		quote = quantity * price
	}
	return quantity, quote
}

// tradeDirections 将配置值转换为多头、空头或双向持仓方向。
func tradeDirections(mode int) []orderDirection {
	long := orderDirection{name: "long", positionSide: futures.PositionSideTypeLong,
		entrySide: futures.SideTypeBuy, exitSide: futures.SideTypeSell}
	short := orderDirection{name: "short", positionSide: futures.PositionSideTypeShort,
		entrySide: futures.SideTypeSell, exitSide: futures.SideTypeBuy}
	switch mode {
	case 0:
		return []orderDirection{long}
	case 1:
		return []orderDirection{short}
	default:
		return []orderDirection{long, short}
	}
}

// makerEntryPrice 按买卖方向在最新行情外侧留出一个价格档位。
func makerEntryPrice(reference, tick float64, side futures.SideType) float64 {
	if side == futures.SideTypeBuy {
		return math.Floor((reference-tick)/tick+1e-12) * tick
	}
	return math.Ceil((reference+tick)/tick-1e-12) * tick
}

// nextMakerPrice GTX 因穿价被拒后再向被动方向移动一个价格档位。
func nextMakerPrice(price, tick float64, side futures.SideType) float64 {
	if side == futures.SideTypeBuy {
		return price - tick
	}
	return price + tick
}

// placeInitialProtection 按多空策略创建初始止盈止损保护单。
func (e *Exchange) placeInitialProtection(
	ctx context.Context, symbol string, entry, quantity float64,
	filter SymbolFilter, direction orderDirection,
) (PositionState, error) {
	state := PositionState{
		Symbol: symbol, Direction: direction.positionSide, Quantity: quantity,
		EntryPrice: entry, AnchorPrice: entry,
	}
	stopRate := 1 + e.cfg.StopLossRate
	if direction.positionSide == futures.PositionSideTypeLong {
		stopRate = 1 - e.cfg.StopLossRate
	}
	// 调用本目录辅助函数，将初始止损价对齐到交易所价格步进。
	stopPrice := help_nearestStep(entry*stopRate, filter.PriceTick)
	state.StopLossPrice = stopPrice
	stopOrder, err := e.createStopLoss(ctx, symbol, stopPrice, filter, direction)
	if err != nil {
		return PositionState{}, err
	}
	state.StopLossAlgoID = stopOrder.AlgoId
	if direction.positionSide == futures.PositionSideTypeShort {
		return state, nil
	}
	// step.1 多头开仓时同时设置全量止损和首笔部分止盈。
	stopAlgoID := state.StopLossAlgoID
	state, err = e.replaceLongTakeProfit(ctx, state, entry, quantity, filter, direction)
	if err != nil {
		_ = e.cancelAlgoOrder(ctx, stopAlgoID)
		return PositionState{}, err
	}
	// step.2 返回止盈、止损条件单编号供持仓状态恢复使用。
	return state, nil
}

// createStopLoss 创建指定仓位方向的全量止损条件单。
func (e *Exchange) createStopLoss(
	ctx context.Context, symbol string, price float64, filter SymbolFilter,
	direction orderDirection,
) (*futures.CreateAlgoOrderResp, error) {
	// 调用本目录辅助函数，按交易所价格精度格式化止损触发价。
	priceText := help_formatStep(price, filter.PriceTick)
	return e.client.NewCreateAlgoOrderService().Symbol(symbol).Side(direction.exitSide).
		PositionSide(direction.positionSide).Type(futures.AlgoOrderTypeStopMarket).
		ClosePosition(true).TriggerPrice(priceText).
		WorkingType(futures.WorkingTypeMarkPrice).Do(ctx)
}

// replaceLongTakeProfit 按锚点和仓位余量创建多头部分止盈条件单。
func (e *Exchange) replaceLongTakeProfit(
	ctx context.Context, state PositionState, anchor, quantity float64,
	filter SymbolFilter, direction orderDirection,
) (PositionState, error) {
	// step.1 根据余仓 10% 和最小名义金额计算止盈数量。
	if quantity <= 0 {
		return PositionState{}, fmt.Errorf("%s 多头止盈数量无效", state.Symbol)
	}
	// 调用本目录辅助函数，将新止盈价格对齐到交易所价格步进。
	profitPrice := help_nearestStep(anchor*(1+e.cfg.TakeProfitRate), filter.PriceTick)
	profitQty := quantity * 0.1
	minimumValue := e.cfg.TradeUSDT * e.cfg.TakeProfitRate
	if quantity*anchor <= minimumValue {
		profitQty = quantity
	} else if profitQty*anchor < minimumValue {
		profitQty = minimumValue / anchor
	}
	profitQty = math.Round(profitQty/filter.QtyStep) * filter.QtyStep
	if profitQty < filter.MinQty {
		profitQty = math.Ceil(filter.MinQty/filter.QtyStep-1e-12) * filter.QtyStep
	}
	if profitQty > quantity {
		profitQty = quantity
	}
	if profitQty < filter.MinQty {
		return PositionState{}, fmt.Errorf("%s 多头止盈数量低于交易所最小数量", state.Symbol)
	}
	// step.2 按锚点止盈价提交部分止盈条件单并返回关联 ID。
	// 调用本目录辅助函数，按交易所步进格式化止盈数量和触发价。
	quantityText := help_formatStep(profitQty, filter.QtyStep)
	priceText := help_formatStep(profitPrice, filter.PriceTick)
	created, err := e.client.NewCreateAlgoOrderService().Symbol(state.Symbol).
		Side(direction.exitSide).PositionSide(direction.positionSide).
		Type(futures.AlgoOrderTypeTakeProfitMarket).
		Quantity(quantityText).TriggerPrice(priceText).
		WorkingType(futures.WorkingTypeMarkPrice).Do(ctx)
	if err != nil {
		return PositionState{}, err
	}
	log.Printf("partial take-profit armed symbol=%s quantity=%s trigger=%s algo_id=%d",
		state.Symbol, quantityText, priceText, created.AlgoId)
	state.TakeProfitAlgoID = created.AlgoId
	state.AnchorPrice = anchor
	state.Quantity = quantity
	return state, nil
}

// cancelAlgoOrder 撤销指定的活动条件单。
func (e *Exchange) cancelAlgoOrder(ctx context.Context, algoID int64) error {
	if algoID == 0 {
		return nil
	}
	_, err := e.client.NewCancelAlgoOrderService().AlgoID(algoID).Do(ctx)
	return err
}

// loadPositionState 从本地状态文件恢复策略持仓和条件单关联信息。
func (e *Exchange) loadPositionState() error {
	data, err := os.ReadFile("data/position_state.json")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("读取持仓管理状态: %w", err)
	}
	var states []PositionState
	if err := json.Unmarshal(data, &states); err != nil {
		return fmt.Errorf("解析持仓管理状态: %w", err)
	}
	for _, state := range states {
		e.positionState[positionStateKey(state.Symbol, state.Direction)] = state
	}
	return nil
}

// savePositionState 更新一个方向的持仓状态并原子写入状态文件。
func (e *Exchange) savePositionState(state PositionState) error {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	// step.1 更新方向仓位状态并生成完整 JSON 快照。
	key := positionStateKey(state.Symbol, state.Direction)
	if state.Quantity <= 0 {
		delete(e.positionState, key)
	} else {
		e.positionState[key] = state
	}
	states := make([]PositionState, 0, len(e.positionState))
	for _, current := range e.positionState {
		states = append(states, current)
	}
	data, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		return err
	}
	// step.2 创建状态目录并通过临时文件原子替换持久化快照。
	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}
	temporary := filepath.Join("data", "position_state.json.tmp")
	if err := os.WriteFile(temporary, data, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join("data", "position_state.json"))
}

// positionStateKey 生成交易对与持仓方向组合的状态索引。
func positionStateKey(symbol string, direction futures.PositionSideType) string {
	return symbol + ":" + string(direction)
}

// ManagePositions 按持仓方向处理止盈成交、趋势退出和保护单更新。
func (e *Exchange) ManagePositions(ctx context.Context) error {
	if !e.managing.CompareAndSwap(false, true) {
		return nil
	}
	defer e.managing.Store(false)
	states := e.positionStatesSnapshot()
	if len(states) == 0 {
		return nil
	}
	managedStates := make(map[string]PositionState, len(states))
	needAlgoOrders := false
	for _, state := range states {
		managedStates[positionStateKey(state.Symbol, state.Direction)] = state
		if state.Direction == futures.PositionSideTypeLong && state.TakeProfitAlgoID != 0 {
			needAlgoOrders = true
		}
	}
	openAlgoOrders := make(map[int64]futures.GetAlgoOrderResp)
	if needAlgoOrders {
		// step.1 批量读取全部活动条件单，避免逐个仓位查询止盈单。
		orders, err := e.client.NewListOpenAlgoOrdersService().Do(ctx)
		if err != nil {
			return fmt.Errorf("批量读取活动条件单: %w", err)
		}
		for _, order := range orders {
			openAlgoOrders[order.AlgoId] = order
		}
	}
	// step.2 批量读取当前账户仓位，确认策略管理状态仍对应真实仓位。
	positions, err := e.client.NewGetPositionRiskService().Do(ctx)
	if err != nil {
		return fmt.Errorf("读取持仓管理仓位: %w", err)
	}
	for _, position := range positions {
		amount, parseErr := strconv.ParseFloat(position.PositionAmt, 64)
		if parseErr != nil || amount == 0 {
			continue
		}
		direction := futures.PositionSideTypeLong
		if strings.EqualFold(position.PositionSide, "SHORT") {
			direction = futures.PositionSideTypeShort
		}
		key := positionStateKey(position.Symbol, direction)
		state, managed := managedStates[key]
		if !managed {
			continue
		}
		if err := e.managePosition(ctx, state, math.Abs(amount), openAlgoOrders); err != nil {
			return fmt.Errorf("管理 %s %s 仓位: %w", position.Symbol, direction, err)
		}
	}
	// step.3 清除已经完全平仓的策略状态及遗留保护单。
	for _, state := range states {
		found := false
		for _, position := range positions {
			if position.Symbol == state.Symbol &&
				strings.EqualFold(position.PositionSide, string(state.Direction)) {
				amount, _ := strconv.ParseFloat(position.PositionAmt, 64)
				found = amount != 0
				break
			}
		}
		if !found {
			if err := waitPositionRequest(ctx); err != nil {
				return err
			}
			_ = e.cancelAlgoOrder(ctx, state.TakeProfitAlgoID)
			if err := waitPositionRequest(ctx); err != nil {
				return err
			}
			_ = e.cancelAlgoOrder(ctx, state.StopLossAlgoID)
			if err := e.savePositionState(PositionState{Symbol: state.Symbol, Direction: state.Direction}); err != nil {
				return err
			}
		}
	}
	return nil
}

// positionStatesSnapshot 返回持仓状态快照，避免持锁期间调用交易所接口。
func (e *Exchange) positionStatesSnapshot() []PositionState {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	states := make([]PositionState, 0, len(e.positionState))
	for _, state := range e.positionState {
		states = append(states, state)
	}
	return states
}

// managePosition 根据多头止盈成交或空头盈利趋势更新订单状态。
func (e *Exchange) managePosition(
	ctx context.Context, state PositionState, quantity float64,
	openAlgoOrders map[int64]futures.GetAlgoOrderResp,
) error {
	filter, ok := e.market.Filter(state.Symbol)
	if !ok {
		return fmt.Errorf("缺少交易规则")
	}
	price, ok := e.market.LastPrice(state.Symbol)
	if !ok || price <= 0 {
		return fmt.Errorf("缺少最新报价")
	}
	direction := orderDirectionFor(state.Direction)
	if state.Direction == futures.PositionSideTypeLong {
		return e.manageLongPosition(ctx, state, quantity, price, filter,
			direction, openAlgoOrders)
	}
	state.Quantity = quantity
	return e.manageShortPosition(ctx, state, price, filter, direction)
}

// manageLongPosition 在部分止盈成交后按最新报价重新设置止盈和止损。
func (e *Exchange) manageLongPosition(
	ctx context.Context, state PositionState, quantity, price float64,
	filter SymbolFilter, direction orderDirection,
	openAlgoOrders map[int64]futures.GetAlgoOrderResp,
) error {
	quantityReduced := quantity < state.Quantity-1e-12 || state.StopRefreshPending
	if !quantityReduced && state.TakeProfitAlgoID != 0 {
		if _, active := openAlgoOrders[state.TakeProfitAlgoID]; active {
			return nil
		}
		if err := waitPositionRequest(ctx); err != nil {
			return err
		}
		order, err := e.client.NewGetAlgoOrderService().AlgoID(state.TakeProfitAlgoID).Do(ctx)
		if err != nil {
			return err
		}
		if order.AlgoStatus == futures.AlgoOrderStatusTypeNew {
			return nil
		}
		if order.ActualOrderId != "" {
			actualID, parseErr := strconv.ParseInt(order.ActualOrderId, 10, 64)
			if parseErr != nil {
				return fmt.Errorf("解析止盈触发订单编号: %w", parseErr)
			}
			if err := waitPositionRequest(ctx); err != nil {
				return err
			}
			actual, queryErr := e.client.NewGetOrderService().Symbol(state.Symbol).
				OrderID(actualID).Do(ctx)
			if queryErr != nil {
				return queryErr
			}
			if actual.Status == futures.OrderStatusTypeNew ||
				actual.Status == futures.OrderStatusTypePartiallyFilled ||
				actual.Status == futures.OrderStatusTypeFilled {
				return nil
			}
		}
		state.TakeProfitAlgoID = 0
		state.AnchorPrice = price
	}
	if quantityReduced {
		// step.1 止盈成交后先撤销旧止盈和旧止损，避免重复 closePosition 条件单。
		if err := e.cancelManagedAlgoOrder(ctx, state.TakeProfitAlgoID, openAlgoOrders); err != nil {
			return fmt.Errorf("撤销旧止盈单失败: %w", err)
		}
		if err := e.cancelManagedAlgoOrder(ctx, state.StopLossAlgoID, openAlgoOrders); err != nil {
			return fmt.Errorf("撤销旧止损单失败: %w", err)
		}
		// step.2 创建新止损并先保存余仓状态，保证后续失败时可以单独补挂止盈。
		stopPrice := help_nearestStep(price*(1-e.cfg.StopLossRate), filter.PriceTick)
		if err := waitPositionRequest(ctx); err != nil {
			return err
		}
		stop, err := e.createStopLoss(ctx, state.Symbol, stopPrice, filter, direction)
		if err != nil {
			state.Quantity = quantity
			state.AnchorPrice = price
			state.TakeProfitAlgoID = 0
			state.StopRefreshPending = true
			return e.restoreLongStop(ctx, state, filter, direction, err)
		}
		state.Quantity = quantity
		state.AnchorPrice = price
		state.StopLossPrice = stopPrice
		state.StopLossAlgoID = stop.AlgoId
		state.TakeProfitAlgoID = 0
		state.StopRefreshPending = false
		if err := e.savePositionState(state); err != nil {
			return fmt.Errorf("新止损已创建但保存持仓状态失败: %w", err)
		}
	}
	if state.TakeProfitAlgoID == 0 {
		// step.3 缺少活动止盈单时仅补挂止盈，不重复创建全量止损。
		anchor := state.AnchorPrice
		if anchor <= 0 {
			anchor = price
		}
		state.Quantity = quantity
		if err := waitPositionRequest(ctx); err != nil {
			return err
		}
		updated, err := e.replaceLongTakeProfit(ctx, state, anchor, quantity, filter, direction)
		if err != nil {
			return err
		}
		return e.savePositionState(updated)
	}
	return nil
}

// cancelManagedAlgoOrder 仅撤销批量快照中仍活动的条件单。
func (e *Exchange) cancelManagedAlgoOrder(
	ctx context.Context, algoID int64, openAlgoOrders map[int64]futures.GetAlgoOrderResp,
) error {
	if algoID == 0 {
		return nil
	}
	if _, active := openAlgoOrders[algoID]; !active {
		return nil
	}
	if err := waitPositionRequest(ctx); err != nil {
		return err
	}
	return e.cancelAlgoOrder(ctx, algoID)
}

// restoreLongStop 在新止损创建失败时恢复旧触发价，避免余仓失去止损保护。
func (e *Exchange) restoreLongStop(
	ctx context.Context, state PositionState, filter SymbolFilter,
	direction orderDirection, cause error,
) error {
	// step.1 新止损创建失败时按旧触发价恢复全量保护。
	if err := waitPositionRequest(ctx); err != nil {
		return fmt.Errorf("新止损失败: %v；恢复旧止损等待失败: %w", cause, err)
	}
	stop, err := e.createStopLoss(ctx, state.Symbol, state.StopLossPrice, filter, direction)
	if err != nil {
		return fmt.Errorf("新止损失败: %v；恢复旧止损也失败: %w", cause, err)
	}
	// step.2 保存恢复后的条件单编号，并报告新止损失败供下一轮重试。
	state.StopLossAlgoID = stop.AlgoId
	if saveErr := e.savePositionState(state); saveErr != nil {
		return fmt.Errorf("新止损失败: %v；恢复止损已创建但保存状态失败: %w", cause, saveErr)
	}
	return fmt.Errorf("新止损创建失败，已恢复原止损: %w", cause)
}

// manageShortPosition 按空头盈利阈值判断继续持有或启动限价平仓。
func (e *Exchange) manageShortPosition(
	ctx context.Context, state PositionState, price float64,
	filter SymbolFilter, direction orderDirection,
) error {
	if state.ExitStarted {
		return e.closeMaker(ctx, state, filter, direction)
	}
	// step.1 达到初始盈利阈值后，检查趋势并决定持有或开始退出。
	if !state.ProfitTargetReached && price > state.EntryPrice*(1-e.cfg.TakeProfitRate) {
		return nil
	}
	trend, ok := e.market.TrendMode(state.Symbol)
	if !ok {
		return nil
	}
	if trend != 1 {
		state.ProfitTargetReached = true
		state.ExitStarted = true
		if err := e.savePositionState(state); err != nil {
			return err
		}
		return e.closeMaker(ctx, state, filter, direction)
	}
	// step.2 趋势延续时，以最新报价重新挂止损并保留旧保护直到新单就绪。
	stopPrice := help_nearestStep(price*(1+e.cfg.StopLossRate), filter.PriceTick)
	if math.Abs(stopPrice-state.StopLossPrice) < filter.PriceTick*0.5 {
		return nil
	}
	if err := waitPositionRequest(ctx); err != nil {
		return err
	}
	newStop, err := e.createStopLoss(ctx, state.Symbol, stopPrice, filter, direction)
	if err != nil {
		return err
	}
	oldStopID := state.StopLossAlgoID
	state.StopLossAlgoID = newStop.AlgoId
	state.StopLossPrice = stopPrice
	state.ProfitTargetReached = true
	state.AnchorPrice = price
	if err := e.savePositionState(state); err != nil {
		_ = e.cancelAlgoOrder(ctx, newStop.AlgoId)
		return err
	}
	if err := waitPositionRequest(ctx); err != nil {
		return err
	}
	return e.cancelAlgoOrder(ctx, oldStopID)
}

// waitPositionRequest 在不持有状态锁时限制逐笔持仓管理请求的发送速度。
func waitPositionRequest(ctx context.Context) error {
	return waitRequestDelay(ctx, 100*time.Millisecond)
}

// waitRequestDelay 在上下文可取消的前提下等待逐个 API 请求之间的间隔。
func waitRequestDelay(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// closeMaker 按被动限价持续重挂空头退出单，并保留交易所止损保护。
func (e *Exchange) closeMaker(
	ctx context.Context, state PositionState, filter SymbolFilter,
	direction orderDirection,
) error {
	// step.1 在配置的超时时间内持续按最新行情提交 GTX 平仓单。
	deadline := time.Now().Add(time.Duration(e.cfg.EntryTimeoutSeconds) * time.Second)
	remaining := state.Quantity
	for time.Now().Before(deadline) {
		price, ok := e.market.LastPrice(state.Symbol)
		if !ok || price <= 0 {
			return fmt.Errorf("缺少退出报价")
		}
		limitPrice := makerEntryPrice(price, filter.PriceTick, direction.exitSide)
		var created *futures.CreateOrderResponse
		var err error
		for rejects := 0; rejects < 5; rejects++ {
			if err := waitPositionRequest(ctx); err != nil {
				return err
			}
			// 调用本目录辅助函数，按交易所价格步进格式化被动退出价和数量。
			created, err = e.client.NewCreateOrderService().Symbol(state.Symbol).
				Side(direction.exitSide).PositionSide(direction.positionSide).
				Type(futures.OrderTypeLimit).TimeInForce(futures.TimeInForceTypeGTX).
				Quantity(help_formatStep(remaining, filter.QtyStep)).
				Price(help_formatStep(limitPrice, filter.PriceTick)).
				NewOrderResponseType(futures.NewOrderRespTypeRESULT).Do(ctx)
			if err == nil {
				break
			}
			// 调用本目录辅助函数，仅对 GTX 穿价错误向被动方向重试。
			if !help_isPostOnlyCross(err) {
				return err
			}
			limitPrice = nextMakerPrice(limitPrice, filter.PriceTick, direction.exitSide)
		}
		if created == nil {
			return fmt.Errorf("%s GTX 退出订单连续穿价拒绝", state.Symbol)
		}
		order, err := e.waitAndCancelManagedEntry(ctx, state.Symbol, created.OrderID,
			time.Duration(e.cfg.EntryRepriceSeconds*float64(time.Second)))
		if err != nil {
			return err
		}
		// 调用本目录辅助函数，读取本次退出订单的最终成交数量。
		filled, _ := orderFill(order)
		remaining -= filled
		if order.Status == futures.OrderStatusTypeFilled || remaining <= 0 {
			return nil
		}
	}
	// step.2 超时后保留交易所止损和持仓状态，后续周期继续尝试退出。
	return nil
}

// orderDirectionFor 将交易所持仓方向转换成开平仓下单方向。
func orderDirectionFor(side futures.PositionSideType) orderDirection {
	if side == futures.PositionSideTypeLong {
		return orderDirection{name: "long", positionSide: side,
			entrySide: futures.SideTypeBuy, exitSide: futures.SideTypeSell}
	}
	return orderDirection{name: "short", positionSide: side,
		entrySide: futures.SideTypeSell, exitSide: futures.SideTypeBuy}
}

// closeDirection 在保护单创建失败后用市价单关闭对应方向，限制保护缺失风险。
func (e *Exchange) closeDirection(ctx context.Context, symbol, quantity string, direction orderDirection) error {
	_, err := e.client.NewCreateOrderService().Symbol(symbol).Side(direction.exitSide).
		PositionSide(direction.positionSide).Type(futures.OrderTypeMarket).Quantity(quantity).
		NewOrderResponseType(futures.NewOrderRespTypeRESULT).Do(ctx)
	return err
}

// orderDirection 描述双向持仓中的开仓和平仓订单方向。
type orderDirection struct {
	name         string
	positionSide futures.PositionSideType
	entrySide    futures.SideType
	exitSide     futures.SideType
}
