package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// TradeStats 记录指定方向的成交、止盈和止损汇总。
type TradeStats struct {
	TotalTrades   int     `json:"total_trades"`
	TakeProfits   int     `json:"take_profit_count"`
	StopLosses    int     `json:"stop_loss_count"`
	TakeProfitPnL float64 `json:"take_profit_total_profit"`
	StopLossPnL   float64 `json:"stop_loss_total_loss"`
}

// TradeState 保存总计和对冲策略的顺向、逆向统计。
type TradeState struct {
	Overall TradeStats `json:"overall"`
	Trend   TradeStats `json:"trend"`
	Reverse TradeStats `json:"reverse"`
}

// StateStore 负责线程安全地持久化交易统计。
type StateStore struct {
	mu   sync.Mutex
	path string
	data TradeState
}

// NewStateStore 读取交易统计文件，不存在时初始化为空统计。
func NewStateStore(path string) (*StateStore, error) {
	// step.1 加载已经持久化的汇总数据，或创建空统计文件。
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	s := &StateStore{path: path}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, s.saveLocked()
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &s.data); err != nil {
		return nil, err
	}
	return s, nil
}

// RecordOpen 在开仓成交确认后增加交易次数。
func (s *StateStore) RecordOpen(direction string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Overall.TotalTrades++
	if direction == "trend" {
		s.data.Trend.TotalTrades++
	} else {
		s.data.Reverse.TotalTrades++
	}
	return s.saveLocked()
}

// RecordClose 根据估算盈亏更新止盈止损次数和金额汇总。
func (s *StateStore) RecordClose(direction string, pnl float64) error {
	// step.1 更新总计和对应方向的止盈止损次数与金额。
	s.mu.Lock()
	defer s.mu.Unlock()
	update := func(stats *TradeStats) {
		if pnl >= 0 {
			stats.TakeProfits++
			stats.TakeProfitPnL += pnl
		} else {
			stats.StopLosses++
			stats.StopLossPnL += -pnl
		}
	}
	update(&s.data.Overall)
	if direction == "trend" {
		update(&s.data.Trend)
	} else {
		update(&s.data.Reverse)
	}
	return s.saveLocked()
}

// Snapshot 返回当前统计的副本，供后续对账或监控使用。
func (s *StateStore) Snapshot() TradeState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

// saveLocked 以临时文件替换方式写入，降低进程中断造成的文件损坏风险。
func (s *StateStore) saveLocked() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
