package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
)

// help_serveAggTrades 封装 SDK 的多交易对聚合成交 WebSocket 订阅。
func help_serveAggTrades(
	symbols []string,
	handler futures.WsAggTradeHandler,
	errHandler futures.ErrHandler,
) (chan struct{}, chan struct{}, error) {
	return futures.WsCombinedAggTradeServe(symbols, handler, errHandler)
}

// help_floorStep 将数量向下取整到交易所要求的步进。
func help_floorStep(value, step float64) float64 {
	return math.Floor(value/step+1e-12) * step
}

// help_nearestStep 将触发价格取整到最近的交易所价格步进。
func help_nearestStep(value, step float64) float64 {
	return math.Round(value/step) * step
}

// help_formatStep 按步进精度格式化交易参数，避免生成科学计数法。
func help_formatStep(value, step float64) string {
	precision := 0
	for step < 1 && precision < 12 {
		step *= 10
		precision++
	}
	return strconv.FormatFloat(value, 'f', precision, 64)
}

// help_logTrade 输出可用于生产日志审计的开仓摘要。
func help_logTrade(symbol, direction string, entry, movement, quantity float64) {
	fmt.Printf("opened symbol=%s direction=%s entry=%.10f move=%.4f%% qty=%.10f time=%s\n",
		symbol, direction, entry, movement*100, quantity, time.Now().Format(time.RFC3339))
}

// help_isPostOnlyCross 识别 Binance 因 GTX 订单会立即成交而返回的拒单错误。
func help_isPostOnlyCross(err error) bool {
	var apiErr *common.APIError
	return errors.As(err, &apiErr) && apiErr.IsValid() && apiErr.Code == -5022
}
