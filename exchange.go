package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

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
			opened[position.Symbol+":"+string(position.PositionSide)] = true
			count++
		}
	}
	return opened, count, nil
}

// Open 创建市价入场单，并为每个持仓方向立即挂独立止盈和止损条件单。
func (e *Exchange) Open(ctx context.Context, symbol string, price, movement float64) error {
	// step.1 根据涨跌方向和策略配置确定需要创建的持仓方向。
	filter, ok := e.market.Filter(symbol)
	if !ok || price <= 0 {
		return fmt.Errorf("交易对 %s 缺少有效行情或过滤规则", symbol)
	}
	directions := e.directions(movement)
	// step.2 为每个方向执行市价入场，并要求保护单成功挂出。
	for _, direction := range directions {
		if err := e.openDirection(ctx, symbol, price, movement, filter, direction); err != nil {
			return err
		}
	}
	return nil
}

// openDirection 按合约精度执行单方向入场，并在止盈止损未就绪时立即平仓。
func (e *Exchange) openDirection(ctx context.Context, symbol string, price, movement float64, filter SymbolFilter, direction orderDirection) error {
	// step.1 按资金和合约步进计算数量，提交市价入场单。
	// 调用本目录辅助函数，按交易所步进向下取整下单数量。
	quantity := help_floorStep(e.cfg.TradeUSDT*float64(e.cfg.Leverage)/price, filter.QtyStep)
	if quantity < filter.MinQty || quantity <= 0 {
		return fmt.Errorf("%s 计算数量低于合约最小数量", symbol)
	}
	// 调用本目录辅助函数，将下单数量格式化为交易所接受的精度。
	qtyText := help_formatStep(quantity, filter.QtyStep)
	order, err := e.client.NewCreateOrderService().Symbol(symbol).Side(direction.entrySide).
		PositionSide(direction.positionSide).Type(futures.OrderTypeMarket).Quantity(qtyText).
		NewOrderResponseType(futures.NewOrderRespTypeRESULT).Do(ctx)
	if err != nil {
		return fmt.Errorf("%s %s 市价开仓失败: %w", symbol, direction.name, err)
	}
	if order.ExecutedQuantity != "" {
		if executed, parseErr := strconv.ParseFloat(order.ExecutedQuantity, 64); parseErr == nil && executed > 0 {
			quantity = executed
			// 调用本目录辅助函数，按实际成交数量更新紧急平仓参数。
			qtyText = help_formatStep(quantity, filter.QtyStep)
		}
	}
	entry := price
	if order.AvgPrice != "" && order.AvgPrice != "0" {
		if parsed, parseErr := strconv.ParseFloat(order.AvgPrice, 64); parseErr == nil && parsed > 0 {
			entry = parsed
		}
	}
	// step.2 按实际成交价挂止盈止损，失败时尝试立即平仓。
	if err := e.placeProtection(ctx, symbol, entry, filter, direction); err != nil {
		if closeErr := e.closeDirection(ctx, symbol, qtyText, direction); closeErr != nil {
			return fmt.Errorf("%s 保护单失败: %v；紧急平仓也失败: %w", symbol, err, closeErr)
		}
		return fmt.Errorf("%s 保护单失败，已紧急平仓: %w", symbol, err)
	}
	// 调用本目录辅助函数输出已挂保护单的开仓摘要。
	help_logTrade(symbol, direction.name, entry, movement, quantity)
	return nil
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

// directions 将策略信号转换成顺向、逆向或两侧的独立持仓方向。
func (e *Exchange) directions(movement float64) []orderDirection {
	// step.1 将涨跌方向转换为顺势或反向的持仓侧。
	longOnRise := movement >= 0
	if e.cfg.Strategy == "reverse" {
		longOnRise = !longOnRise
	}
	trend := orderDirection{name: "trend", positionSide: futures.PositionSideTypeLong,
		entrySide: futures.SideTypeBuy, exitSide: futures.SideTypeSell}
	reverse := orderDirection{name: "reverse", positionSide: futures.PositionSideTypeShort,
		entrySide: futures.SideTypeSell, exitSide: futures.SideTypeBuy}
	if !longOnRise {
		trend, reverse = reverse, trend
	}
	// step.2 对冲模式同时返回多空两侧，其余模式返回单侧。
	if e.cfg.Strategy == "hedge" {
		return []orderDirection{trend, reverse}
	}
	return []orderDirection{trend}
}

// orderDirection 描述双向持仓中的开仓和平仓订单方向。
type orderDirection struct {
	name         string
	positionSide futures.PositionSideType
	entrySide    futures.SideType
	exitSide     futures.SideType
}
