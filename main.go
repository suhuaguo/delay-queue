package main

import "github.com/ouqiang/delay-queue/cmd"

// 所有的入口。golang 规定
func main() {
	// Redis 的客户端不支持 集群模式，建议更改掉。
	cmd := new(cmd.Cmd)
	// 执行方法
	cmd.Run()
}
