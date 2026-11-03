package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config 保存策略参数和交易所密钥文件路径。
type Config struct {
	KeyFile             string  `json:"keyfile"`
	KlineCount          int     `json:"kline_count"`
	TradeIntervalSecond int     `json:"trade_interval_seconds"`
	TradeMode           int     `json:"trade_mode"`
	EntryRepriceSeconds float64 `json:"entry_reprice_seconds"`
	EntryTimeoutSeconds int     `json:"entry_timeout_seconds"`
	TradeUSDT           float64 `json:"trade_usdt"`
	MaxSymbols          int     `json:"max_symbols"`
	TakeProfitRate      float64 `json:"take_profit_rate"`
	StopLossRate        float64 `json:"stop_loss_rate"`
	MinMoveRate         float64 `json:"min_move_rate"`
	Leverage            int     `json:"leverage"`
	Testnet             bool    `json:"testnet"`
	ReceiveWindowMillis int64   `json:"receive_window_millis"`
	StalePriceSeconds   int     `json:"stale_price_seconds"`
}

// loadConfig 读取部署方准备的配置文件，并用默认值填充未配置字段。
func loadConfig(path string) (Config, error) {
	// step.1 准备策略默认值，并要求部署方预先提供配置文件。
	cfg := Config{
		KeyFile: "/root/key", KlineCount: 12, TradeIntervalSecond: 20,
		TradeMode: 0, EntryRepriceSeconds: 1.5, EntryTimeoutSeconds: 300,
		TradeUSDT: 100, MaxSymbols: 2, TakeProfitRate: 0.03, StopLossRate: 0.1,
		MinMoveRate: 0.2, Leverage: 1,
		ReceiveWindowMillis: 5000, StalePriceSeconds: 30,
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Config{}, fmt.Errorf("配置文件 %s 不存在，请从 config.sample.json 复制后修改", path)
	}
	if err != nil {
		return Config{}, err
	}
	// step.2 读取用户配置并校验必需参数。
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("解析配置: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate 校验策略参数，避免错误配置直接进入实盘下单流程。
func (c Config) validate() error {
	// step.1 校验行情窗口、交易周期、资金和止盈止损参数。
	if c.KeyFile == "" || c.KlineCount <= 0 || c.TradeIntervalSecond <= 0 ||
		c.TradeUSDT <= 0 || c.MaxSymbols <= 0 {
		return fmt.Errorf("keyfile、kline_count、trade_interval_seconds、trade_usdt 和 max_symbols 必须为正数或非空")
	}
	if c.KlineCount > 1499 {
		return fmt.Errorf("kline_count 不能超过 1499，以便额外请求当前未收盘 K 线")
	}
	if c.TradeMode < 0 || c.TradeMode > 3 || c.EntryRepriceSeconds <= 0 ||
		c.EntryRepriceSeconds < 0.1 || float64(c.EntryTimeoutSeconds) < c.EntryRepriceSeconds {
		return fmt.Errorf("trade_mode 必须为 0、1、2 或 3，重挂间隔至少 0.1 秒且不能超过挂单超时")
	}
	if c.TradeMode == 2 && c.MaxSymbols < 2 {
		return fmt.Errorf("双向模式下 max_symbols 至少为 2")
	}
	if c.TakeProfitRate <= 0 || c.TakeProfitRate >= 1 || c.StopLossRate <= 0 ||
		c.StopLossRate >= 1 || c.MinMoveRate <= 0 {
		return fmt.Errorf("止盈和止损必须在 0 和 1 之间，最小波动必须大于 0")
	}
	if c.Leverage <= 0 || c.ReceiveWindowMillis <= 0 || c.StalePriceSeconds <= 0 {
		return fmt.Errorf("leverage、receive_window_millis 和 stale_price_seconds 必须大于0")
	}
	return nil
}
