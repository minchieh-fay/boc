# BOC Bollinger 挂单程序

程序通过 Binance USDⓈ-M Futures 实盘 API 运行。每次刷新会：

1. 读取配置中的多个 K 线周期，使用最近 `boll_period` 根已收盘 K 线计算 Bollinger Bands。
2. 取所有周期中最大的上轨作为 SELL 限价单价格，最小的下轨作为 BUY 限价单价格。
3. 撤销本程序创建的订单（`clientOrderId` 以 `BOC_` 开头），重新挂出一张多单和一张空单。

程序启动时刷新一次，之后每 3 小时刷新。若计算出的 SELL 不高于当前价，则上移 10 USDT；若 BUY 不低于当前价，则下移 10 USDT，避免限价单直接成为吃单。平仓由人工处理，程序不会管理仓位。

## 配置和运行

```bash
cp config.example.json config.json
go mod download
go test ./...
go build -o boc .
```

生成服务器使用的 Linux amd64 程序：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o boc-linux-amd64 .
```

服务器上的 `/root/key` 必须包含两项内容：第一行 API Key，第二行 Secret。API Key 只应开启合约交易权限，不要开启提现权限。

获取方式：登录 Binance 网页端，进入头像菜单的 `API Management`，创建 API Key，并在权限中开启 USDⓈ-M Futures 交易；将生成的 Key 和 Secret 分别写入 `/root/key` 的前两项。建议同时绑定服务器 IP 白名单。

```text
your-api-key
your-api-secret
```

运行：

```bash
./boc config.json
```

`symbol`、`quantity`、`intervals`、`boll_period` 和 `boll_stddev` 可在配置中修改；未填写时默认 `BTCUSDT`、`0.1`、`["1h", "2h"]`、`20` 和 `2`。Binance Futures API 地址由依赖库固定为实盘地址 `https://fapi.binance.com`，本项目不提供 testnet 配置。

## Compose 部署

将 `boc-linux-amd64`、`config.json` 和 `/root/key` 准备好后，在服务器执行：

```bash
mkdir -p data
docker compose up -d
docker compose logs -f boc
```

Compose 使用 `alpine:3.22` 运行本地编译的静态 Linux amd64 程序；订单状态目录 `./data` 会持久化。
