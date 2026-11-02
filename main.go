package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 主函数负责初始化实盘服务并响应系统退出信号。
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := newApplication("config.json")
	if err != nil {
		log.Fatal(err)
	}
	if err := app.run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
