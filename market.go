package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// SymbolInfo 保存交易对最新价格与上个策略周期的基准价格。
type SymbolInfo struct {
	Price       float64
	OldPrice    float64
	LastUpdated time.Time
}

// MarketData 管理 WebSocket 行情和交易所合约过滤规则。
type MarketData struct {
	mu      sync.RWMutex
	prices  map[string]SymbolInfo
	filters map[string]SymbolFilter
}

// SymbolFilter 保存下单所需数量步进与价格步进。
type SymbolFilter struct {
	QtyStep   float64
	MinQty    float64
	PriceTick float64
}

// NewMarketData 初始化行情缓存和合约规则表。
func NewMarketData() *MarketData {
	return &MarketData{prices: make(map[string]SymbolInfo), filters: make(map[string]SymbolFilter)}
}

// LoadSymbols 从交易所元数据选择可交易的 USDT 永续合约并记录下单步进。
func (m *MarketData) LoadSymbols(symbols []futures.Symbol) []string {
	// step.1 筛选可交易的 USDT 永续合约，并提取交易所下单规则。
	var names []string
	for _, symbol := range symbols {
		if symbol.QuoteAsset != "USDT" || symbol.ContractType != futures.ContractTypePerpetual ||
			symbol.Status != string(futures.SymbolStatusTypeTrading) {
			continue
		}
		filter := SymbolFilter{}
		for _, raw := range symbol.Filters {
			typ, _ := raw["filterType"].(string)
			read := func(key string) float64 {
				value, _ := strconv.ParseFloat(fmt.Sprint(raw[key]), 64)
				return value
			}
			switch typ {
			case string(futures.SymbolFilterTypeMarketLotSize):
				filter.QtyStep, filter.MinQty = read("stepSize"), read("minQty")
			case string(futures.SymbolFilterTypeLotSize):
				if filter.QtyStep == 0 {
					filter.QtyStep, filter.MinQty = read("stepSize"), read("minQty")
				}
			case string(futures.SymbolFilterTypePrice):
				filter.PriceTick = read("tickSize")
			}
		}
		if filter.QtyStep <= 0 || filter.PriceTick <= 0 {
			continue
		}
		m.filters[symbol.Symbol] = filter
		m.prices[symbol.Symbol] = SymbolInfo{}
		names = append(names, symbol.Symbol)
	}
	return names
}

// UpdatePrice 更新一个交易对的最新聚合成交价格。
func (m *MarketData) UpdatePrice(symbol, rawPrice string) {
	price, err := strconv.ParseFloat(rawPrice, 64)
	if err != nil || price <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	info := m.prices[strings.ToUpper(symbol)]
	info.Price = price
	info.LastUpdated = time.Now()
	m.prices[strings.ToUpper(symbol)] = info
}

// BestMove 选择基准价格和最新价格都有效且幅度最大的交易对。
func (m *MarketData) BestMove(minimum float64, maxAge time.Duration) (string, float64, float64, bool) {
	// step.1 在读锁保护下比较每个合约的周期涨跌幅。
	m.mu.RLock()
	defer m.mu.RUnlock()
	bestSymbol, bestMove, bestPrice := "", 0.0, 0.0
	for symbol, info := range m.prices {
		if info.Price <= 0 || info.OldPrice <= 0 || time.Since(info.LastUpdated) > maxAge {
			continue
		}
		low := info.Price
		if info.OldPrice < low {
			low = info.OldPrice
		}
		move := abs(info.OldPrice-info.Price) / low
		if move > bestMove {
			bestSymbol, bestMove, bestPrice = symbol, move, info.Price
		}
	}
	return bestSymbol, bestMove, bestPrice, bestSymbol != "" && bestMove >= minimum
}

// AdvanceBaseline 在每个策略周期结束后推进所有交易对的比较基准。
func (m *MarketData) AdvanceBaseline(maxAge time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for symbol, info := range m.prices {
		if info.Price > 0 && time.Since(info.LastUpdated) <= maxAge {
			info.OldPrice = info.Price
		} else {
			info.OldPrice = 0
		}
		m.prices[symbol] = info
	}
}

// ResetBaselines 在行情连接中断后清空所有周期基准，避免跨断线比较价格。
func (m *MarketData) ResetBaselines() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for symbol, info := range m.prices {
		info.OldPrice = 0
		m.prices[symbol] = info
	}
}

// Filter 返回交易对的下单精度配置。
func (m *MarketData) Filter(symbol string) (SymbolFilter, bool) {
	filter, ok := m.filters[symbol]
	return filter, ok
}

// Prices 返回指定交易对最新价和周期基准价的快照。
func (m *MarketData) Prices(symbol string) (float64, float64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, ok := m.prices[symbol]
	return info.Price, info.OldPrice, ok
}

// abs 返回浮点数绝对值。
func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
