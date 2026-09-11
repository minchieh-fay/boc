package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

const (
	credentialsFile = "/root/key"
	orderPrefix     = "BOC_"
	refreshInterval = 3 * time.Hour
	avoidTakerGap   = 10.0
)

type Config struct {
	Symbol     string   `json:"symbol"`
	Quantity   string   `json:"quantity"`
	Intervals  []string `json:"intervals"`
	BollPeriod int      `json:"boll_period"`
	BollStdDev float64  `json:"boll_stddev"`
	StateFile  string   `json:"state_file"`
	ErrorRetry int      `json:"error_retry_seconds"`
}

type Rules struct {
	Tick, Step, MinQty *big.Float
	TickDecimals       int
}

type Bot struct {
	cfg    Config
	client *futures.Client
	rules  Rules
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	configPath := "config.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}
	key, secret, err := loadCredentials()
	if err != nil {
		log.Fatal(err)
	}
	bot := &Bot{cfg: cfg, client: futures.NewClient(key, secret)}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = bot.loadRules(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("BOC started symbol=%s quantity=%s intervals=%s boll_period=%d boll_stddev=%g refresh=3h REAL", cfg.Symbol, cfg.Quantity, strings.Join(cfg.Intervals, ","), cfg.BollPeriod, cfg.BollStdDev)
	bot.run(ctx)
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err = json.Unmarshal(data, &c); err != nil {
		return Config{}, err
	}
	c.Symbol = strings.ToUpper(strings.TrimSpace(c.Symbol))
	if c.Symbol == "" {
		c.Symbol = "ETHUSDC"
	}
	if c.Quantity == "" {
		c.Quantity = "0.1"
	}
	if len(c.Intervals) == 0 {
		c.Intervals = []string{"1h", "2h"}
	}
	for i := range c.Intervals {
		c.Intervals[i] = strings.TrimSpace(c.Intervals[i])
	}
	if c.BollPeriod <= 0 {
		c.BollPeriod = 20
	}
	if c.BollStdDev <= 0 {
		c.BollStdDev = 2
	}
	if c.StateFile == "" {
		c.StateFile = "state.json"
	}
	if c.ErrorRetry <= 0 {
		c.ErrorRetry = 60
	}
	return c, nil
}

func loadCredentials() (string, string, error) {
	data, err := os.ReadFile(credentialsFile)
	if err != nil {
		return "", "", fmt.Errorf("read %s: %w", credentialsFile, err)
	}
	parts := strings.Fields(string(data))
	if len(parts) < 2 {
		return "", "", errors.New("/root/key must contain API key and secret")
	}
	return parts[0], parts[1], nil
}

func (b *Bot) loadRules(ctx context.Context) error {
	info, err := b.client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return err
	}
	for _, symbol := range info.Symbols {
		if symbol.Symbol != b.cfg.Symbol {
			continue
		}
		var tick, step, min string
		for _, filter := range symbol.Filters {
			t, _ := filter["filterType"].(string)
			if t == "PRICE_FILTER" {
				tick, _ = filter["tickSize"].(string)
			}
			if t == "LOT_SIZE" {
				step, _ = filter["stepSize"].(string)
				min, _ = filter["minQty"].(string)
			}
		}
		b.rules.Tick, err = decimal(tick)
		if err != nil {
			return err
		}
		b.rules.TickDecimals = decimalPlaces(tick)
		b.rules.Step, err = decimal(step)
		if err != nil {
			return err
		}
		b.rules.MinQty, err = decimal(min)
		return err
	}
	return fmt.Errorf("symbol not found: %s", b.cfg.Symbol)
}

func (b *Bot) run(ctx context.Context) {
	for {
		if err := b.refresh(ctx); err != nil {
			log.Printf("refresh failed: %v; retry in %ds", err, b.cfg.ErrorRetry)
			if !sleep(ctx, time.Duration(b.cfg.ErrorRetry)*time.Second) {
				return
			}
			continue
		}
		if !sleep(ctx, refreshInterval) {
			return
		}
	}
}

func (b *Bot) refresh(ctx context.Context) error {
	upper, lower, err := b.bollingerTargets(ctx)
	if err != nil {
		return err
	}
	current, err := b.currentPrice(ctx)
	if err != nil {
		return err
	}
	upper, lower, err = safePrices(upper, lower, current)
	if err != nil {
		return err
	}
	orders, err := b.client.NewListOpenOrdersService().Symbol(b.cfg.Symbol).Do(ctx)
	if err != nil {
		return err
	}
	for _, order := range orders {
		if strings.HasPrefix(order.ClientOrderID, orderPrefix) {
			if _, err = b.client.NewCancelOrderService().Symbol(b.cfg.Symbol).OrderID(order.OrderID).Do(ctx); err != nil {
				return err
			}
		}
	}
	if err = b.place(ctx, "SELL", upper); err != nil {
		return err
	}
	if err = b.place(ctx, "BUY", lower); err != nil {
		return err
	}
	log.Printf("orders refreshed current=%.8f sell=%.8f buy=%.8f", current, upper, lower)
	return nil
}

func (b *Bot) bollingerTargets(ctx context.Context) (float64, float64, error) {
	var uppers, lowers []float64
	for _, interval := range b.cfg.Intervals {
		klines, err := b.client.NewKlinesService().Symbol(b.cfg.Symbol).Interval(interval).Limit(b.cfg.BollPeriod + 1).Do(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("get %s klines: %w", interval, err)
		}
		if len(klines) < b.cfg.BollPeriod+1 {
			return 0, 0, fmt.Errorf("not enough %s klines: got %d", interval, len(klines))
		}
		closes := make([]float64, 0, b.cfg.BollPeriod)
		for _, k := range klines[len(klines)-b.cfg.BollPeriod-1 : len(klines)-1] {
			closePrice, err := parseFloat(k.Close)
			if err != nil {
				return 0, 0, err
			}
			closes = append(closes, closePrice)
		}
		up, down := bollinger(closes, b.cfg.BollStdDev)
		log.Printf("boll interval=%s middle=%.8f upper=%.8f lower=%.8f", interval, mean(closes), up, down)
		uppers = append(uppers, up)
		lowers = append(lowers, down)
	}
	return max(uppers), min(lowers), nil
}

func bollinger(values []float64, multiplier float64) (float64, float64) {
	m := mean(values)
	variance := 0.0
	for _, value := range values {
		variance += (value - m) * (value - m)
	}
	variance /= float64(len(values))
	deviation := math.Sqrt(variance) * multiplier
	return m + deviation, m - deviation
}

func safePrices(upper, lower, current float64) (float64, float64, error) {
	if upper <= current+avoidTakerGap {
		upper = current + avoidTakerGap
	}
	if lower >= current-avoidTakerGap {
		lower = current - avoidTakerGap
	}
	if lower <= 0 {
		return 0, 0, fmt.Errorf("buy price would be non-positive: %.8f", lower)
	}
	return upper, lower, nil
}

func (b *Bot) place(ctx context.Context, side string, rawPrice float64) error {
	price, err := decimal(fmt.Sprintf("%.12f", rawPrice))
	if err != nil {
		return err
	}
	if side == "SELL" {
		price = ceil(price, b.rules.Tick)
	} else {
		price = floor(price, b.rules.Tick)
	}
	if price.Sign() <= 0 {
		return errors.New("calculated price is not positive")
	}
	priceText := price.Text('f', b.rules.TickDecimals)
	quantityText := strings.TrimSpace(b.cfg.Quantity)
	if quantityText == "" {
		return errors.New("quantity must not be empty")
	}
	id := fmt.Sprintf("%s%d_%s", orderPrefix, time.Now().UnixNano(), strings.ToLower(side))
	_, err = b.client.NewCreateOrderService().Symbol(b.cfg.Symbol).Side(futures.SideType(side)).Type(futures.OrderTypeLimit).TimeInForce(futures.TimeInForceTypeGTC).Quantity(quantityText).Price(priceText).NewClientOrderID(id).Do(ctx)
	return err
}

func (b *Bot) currentPrice(ctx context.Context) (float64, error) {
	prices, err := b.client.NewListPricesService().Symbol(b.cfg.Symbol).Do(ctx)
	if err != nil {
		return 0, err
	}
	if len(prices) != 1 {
		return 0, errors.New("unexpected price response")
	}
	return parseFloat(prices[0].Price)
}

func mean(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}
func max(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}
func min(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}
func parseFloat(value string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(value), 64)
}
func decimal(value string) (*big.Float, error) {
	f, _, err := big.ParseFloat(strings.TrimSpace(value), 10, 256, big.ToNearestEven)
	return f, err
}

func decimalPlaces(value string) int {
	value = strings.TrimSpace(value)
	if dot := strings.IndexByte(value, '.'); dot >= 0 {
		return len(strings.TrimRight(value[dot+1:], "0"))
	}
	return 0
}
func floor(value, step *big.Float) *big.Float {
	q := new(big.Float).Quo(value, step)
	integer, _ := q.Int(nil)
	return new(big.Float).Mul(new(big.Float).SetInt(integer), step)
}
func ceil(value, step *big.Float) *big.Float {
	q := new(big.Float).Quo(value, step)
	integer, _ := q.Int(nil)
	if new(big.Float).SetInt(integer).Cmp(q) < 0 {
		integer.Add(integer, big.NewInt(1))
	}
	return new(big.Float).Mul(new(big.Float).SetInt(integer), step)
}
func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
