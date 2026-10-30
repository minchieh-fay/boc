package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config 保存策略参数和交易所密钥文件路径。
type Config struct {
	KeyFile             string  `json:"keyfile"`
	IntervalMinutes     int     `json:"interval_minutes"`
	TradeUSDT           float64 `json:"trade_usdt"`
	MaxSymbols          int     `json:"max_symbols"`
	TakeProfitRate      float64 `json:"take_profit_rate"`
	StopLossRate        float64 `json:"stop_loss_rate"`
	MinMoveRate         float64 `json:"min_move_rate"`
	Strategy            string  `json:"strategy"`
	Leverage            int     `json:"leverage"`
	Testnet             bool    `json:"testnet"`
	ReceiveWindowMillis int64   `json:"receive_window_millis"`
	StalePriceSeconds   int     `json:"stale_price_seconds"`
}

// loadConfig 读取部署方准备的配置文件，并用默认值填充未配置字段。
func loadConfig(path string) (Config, error) {
	// step.1 准备策略默认值，并要求部署方预先提供配置文件。
	cfg := Config{
		KeyFile: "/root/key", IntervalMinutes: 5, TradeUSDT: 100,
		MaxSymbols: 2, TakeProfitRate: 0.04, StopLossRate: 0.02,
		MinMoveRate: 0.05, Strategy: "trend", Leverage: 1,
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
	// step.1 校验资金、周期、止盈止损和策略类型。
	if c.KeyFile == "" || c.IntervalMinutes <= 0 || c.TradeUSDT <= 0 || c.MaxSymbols <= 0 {
		return fmt.Errorf("keyfile、interval_minutes、trade_usdt 和 max_symbols 必须为正数或非空")
	}
	if c.TakeProfitRate <= 0 || c.StopLossRate <= 0 || c.MinMoveRate < 0 {
		return fmt.Errorf("止盈、止损必须大于0，最小幅度不能小于0")
	}
	if c.Strategy != "trend" && c.Strategy != "reverse" && c.Strategy != "hedge" {
		return fmt.Errorf("strategy 仅支持 trend、reverse、hedge")
	}
	if c.Leverage <= 0 || c.ReceiveWindowMillis <= 0 || c.StalePriceSeconds <= 0 {
		return fmt.Errorf("leverage、receive_window_millis 和 stale_price_seconds 必须大于0")
	}
	return nil
}
