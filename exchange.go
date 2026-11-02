package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// Exchange 封装行情初始化、账户仓位读取和真实下单流程。
type Exchange struct {
	client *futures.Client
	cfg    Config
	market *MarketData
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
	return &Exchange{client: client, cfg: cfg, market: market}, nil
}

// Initialize 加载可交易合约，并确保账户处于双向持仓模式。
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
	// step.2 开启双向持仓，确保 LONG 和 SHORT 订单参数可用。
	if err := e.client.NewChangePositionModeService().DualSide(true).Do(ctx); err != nil && !strings.Contains(err.Error(), "No need to change position side") {
		return nil, fmt.Errorf("启用双向持仓失败: %w", err)
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
func (e *Exchange) Open(ctx context.Context, symbol string, price float64) error {
	// step.1 校验交易规则、设置杠杆并解析需要交易的方向。
	filter, ok := e.market.Filter(symbol)
	if !ok || price <= 0 {
		return fmt.Errorf("交易对 %s 缺少有效行情或过滤规则", symbol)
	}
	if _, err := e.client.NewChangeLeverageService().Symbol(symbol).
		Leverage(e.cfg.Leverage).Do(ctx); err != nil {
		return fmt.Errorf("%s 设置杠杆失败: %w", symbol, err)
	}
	directions := tradeDirections(e.cfg.TradeMode)
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
	if protectErr := e.placeProtection(protectCtx, symbol, entry, filter, direction); protectErr != nil {
		if closeErr := e.closeDirection(protectCtx, symbol, filledText, direction); closeErr != nil {
			return fmt.Errorf("%s 保护单失败: %v；紧急平仓也失败: %w", symbol, protectErr, closeErr)
		}
		return fmt.Errorf("%s 保护单失败，已紧急平仓: %w", symbol, protectErr)
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
	// step.1 在重挂周期内轮询订单状态，到期撤销剩余数量。
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		cancelCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return e.cancelAndReadEntry(cancelCtx, symbol, orderID)
	case <-timer.C:
	}
	return e.cancelAndReadEntry(ctx, symbol, orderID)
}

// cancelAndReadEntry 确认挂单已结束后返回最终成交，避免旧报价仍留在市场。
func (e *Exchange) cancelAndReadEntry(ctx context.Context, symbol string,
	orderID int64) (*futures.Order, error) {
	// step.1 撤销活动订单并查询最终状态，遇到网络竞态时有限重试。
	for attempt := 0; attempt < 3; attempt++ {
		cancelled, cancelErr := e.client.NewCancelOrderService().
			Symbol(symbol).OrderID(orderID).Do(ctx)
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

// placeProtection 为指定持仓方向挂止盈和止损市价条件单。
func (e *Exchange) placeProtection(ctx context.Context, symbol string, entry float64, filter SymbolFilter, direction orderDirection) error {
	// step.1 按持仓方向计算并对齐止盈、止损触发价。
	takePrice, stopPrice := entry, entry
	if direction.positionSide == futures.PositionSideTypeLong {
		takePrice *= 1 + e.cfg.TakeProfitRate
		stopPrice *= 1 - e.cfg.StopLossRate
	} else {
		takePrice *= 1 - e.cfg.TakeProfitRate
		stopPrice *= 1 + e.cfg.StopLossRate
	}
	// 调用本目录辅助函数，将两个条件价对齐交易所的价格步进。
	takePrice = help_nearestStep(takePrice, filter.PriceTick)
	stopPrice = help_nearestStep(stopPrice, filter.PriceTick)
	// step.2 提交平仓方向一致、使用标记价格触发的两张条件单。
	var placedIDs []int64
	for _, protection := range []struct {
		typ   futures.AlgoOrderType
		price float64
	}{{futures.AlgoOrderTypeTakeProfitMarket, takePrice}, {futures.AlgoOrderTypeStopMarket, stopPrice}} {
		// 调用本目录辅助函数，按交易所价格精度格式化保护单触发价。
		created, err := e.client.NewCreateAlgoOrderService().Symbol(symbol).Side(direction.exitSide).
			PositionSide(direction.positionSide).Type(protection.typ).ClosePosition(true).
			TriggerPrice(help_formatStep(protection.price, filter.PriceTick)).WorkingType(futures.WorkingTypeMarkPrice).
			Do(ctx)
		if err != nil {
			for _, algoID := range placedIDs {
				if _, cancelErr := e.client.NewCancelAlgoOrderService().AlgoID(algoID).Do(ctx); cancelErr != nil {
					return fmt.Errorf("创建保护单失败: %v；撤销已创建条件单 %d 失败: %w", err, algoID, cancelErr)
				}
			}
			return err
		}
		placedIDs = append(placedIDs, created.AlgoId)
	}
	return nil
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
