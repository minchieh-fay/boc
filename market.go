package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// MyKline 保存一个五分钟区间内的最高价和最低价。
type MyKline struct {
	Timestamp int64
	Low       float64
	High      float64
}

// SymbolQueue 管理单个交易对的历史区间和当前未完成区间。
type SymbolQueue struct {
	Data      []MyKline
	Highest   float64
	Lowest    float64
	NewData   MyKline
	LastTick  time.Time
	LastPrice float64
}

// MarketData 管理所有交易对的区间队列和下单精度规则。
type MarketData struct {
	mu      sync.RWMutex
	queues  map[string]*SymbolQueue
	filters map[string]SymbolFilter
	count   int
}

// SymbolFilter 保存下单所需数量步进与价格步进。
type SymbolFilter struct {
	QtyStep   float64
	MinQty    float64
	PriceTick float64
}

// NewMarketData 初始化各交易对的滚动队列。
func NewMarketData(count int) *MarketData {
	return &MarketData{
		queues: make(map[string]*SymbolQueue), filters: make(map[string]SymbolFilter), count: count,
	}
}

// LoadSymbols 筛选可交易的 USDT 永续合约并记录下单规则。
func (m *MarketData) LoadSymbols(symbols []futures.Symbol) []string {
	// step.1 筛选有效合约并读取数量、价格精度。
	var names []string
	m.mu.Lock()
	defer m.mu.Unlock()
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
		m.queues[symbol.Symbol] = &SymbolQueue{NewData: emptyKline()}
		names = append(names, symbol.Symbol)
	}
	return names
}

// SetHistory 设置交易对已经收盘的历史 K 线并计算窗口极值。
func (m *MarketData) SetHistory(symbol string, data []MyKline) {
	m.mu.Lock()
	defer m.mu.Unlock()
	queue, ok := m.queues[symbol]
	if !ok {
		return
	}
	if len(data) > m.count {
		data = data[len(data)-m.count:]
	}
	queue.Data = append([]MyKline(nil), data...)
	refreshExtremes(queue)
}

// UpdatePrice 将聚合成交价累积到当前五分钟区间。
func (m *MarketData) UpdatePrice(symbol, rawPrice string) {
	// step.1 校验成交价并找到对应交易对的实时区间。
	price, err := strconv.ParseFloat(rawPrice, 64)
	if err != nil || price <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	queue, ok := m.queues[strings.ToUpper(symbol)]
	if !ok {
		return
	}
	// step.2 累积当前区间高低价，并刷新行情时间和最新价。
	now := time.Now()
	if queue.NewData.Timestamp == 0 {
		queue.NewData = MyKline{Timestamp: now.Unix(), Low: price, High: price}
	} else {
		if price < queue.NewData.Low {
			queue.NewData.Low = price
		}
		if price > queue.NewData.High {
			queue.NewData.High = price
		}
	}
	queue.LastTick = now
	queue.LastPrice = price
}

// Rotate 收入新完成的实时区间，并为下一段区间重置临时数据。
func (m *MarketData) Rotate(now time.Time, maxAge time.Duration) int {
	// step.1 将近期有成交的当前区间移入固定长度历史窗口。
	m.mu.Lock()
	defer m.mu.Unlock()
	rotated := 0
	for _, queue := range m.queues {
		if queue.NewData.High > 0 && now.Sub(queue.LastTick) <= maxAge {
			queue.Data = append(queue.Data, queue.NewData)
			if len(queue.Data) > m.count {
				queue.Data = queue.Data[len(queue.Data)-m.count:]
			}
			refreshExtremes(queue)
			rotated++
		}
		queue.NewData = emptyKline()
	}
	return rotated
}

// Candidates 返回窗口振幅达到阈值且实时数据仍然新鲜的交易对。
func (m *MarketData) Candidates(minimum float64, now time.Time, maxAge time.Duration) []MoveCandidate {
	// step.1 合并历史区间与当前实时区间的极值并计算振幅。
	m.mu.RLock()
	defer m.mu.RUnlock()
	var candidates []MoveCandidate
	for symbol, queue := range m.queues {
		if len(queue.Data) == 0 || queue.LastTick.IsZero() || now.Sub(queue.LastTick) > maxAge {
			continue
		}
		high, low := queue.Highest, queue.Lowest
		if queue.NewData.High > high {
			high = queue.NewData.High
		}
		if queue.NewData.Low > 0 && (low <= 0 || queue.NewData.Low < low) {
			low = queue.NewData.Low
		}
		if low <= 0 || high <= low {
			continue
		}
		rate := (high - low) / low
		if rate > minimum {
			candidates = append(candidates, MoveCandidate{
				Symbol: symbol, Rate: rate, Price: queue.LastPrice,
			})
		}
	}
	return candidates
}

// Filter 返回交易对的下单精度配置。
func (m *MarketData) Filter(symbol string) (SymbolFilter, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	filter, ok := m.filters[symbol]
	return filter, ok
}

// LastPrice 返回交易对当前价及其是否已收到实时行情。
func (m *MarketData) LastPrice(symbol string) (float64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	queue, ok := m.queues[symbol]
	if !ok || queue.LastPrice <= 0 {
		return 0, false
	}
	return queue.LastPrice, true
}

// MoveCandidate 表示满足振幅门槛的交易对及其振幅。
type MoveCandidate struct {
	Symbol string
	Rate   float64
	Price  float64
}

// emptyKline 创建用于标记当前区间尚无成交的初始值。
func emptyKline() MyKline {
	return MyKline{Low: math.MaxFloat64, High: -math.MaxFloat64}
}

// refreshExtremes 根据历史队列重算全窗口最高价和最低价。
func refreshExtremes(queue *SymbolQueue) {
	queue.Highest, queue.Lowest = 0, math.MaxFloat64
	for _, kline := range queue.Data {
		if kline.High > queue.Highest {
			queue.Highest = kline.High
		}
		if kline.Low > 0 && kline.Low < queue.Lowest {
			queue.Lowest = kline.Low
		}
	}
	if len(queue.Data) == 0 {
		queue.Lowest = 0
	}
}
