package main

import "github.com/ouqiang/delay-queue/cmd"

// 所有的入口。golang 规定
func main() {
	//
	cmd := new(cmd.Cmd)
	// 执行方法
	cmd.Run()
}
