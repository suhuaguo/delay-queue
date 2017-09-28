package cmd

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/ouqiang/delay-queue/config"
	"github.com/ouqiang/delay-queue/delayqueue"
	"github.com/ouqiang/delay-queue/routers"
)

// 直接使用struct{} 来初始化strut时，返回的是一个struct类型的值，而不是指针两者是不一样的
type Cmd struct{}

var (
	version    bool
	configFile string
)

const (
	AppVersion = "0.3"
)

// 是在 main.go 中运行的。直接调用 run 方法
func (cmd *Cmd) Run() {

	// 解析命令行参数
	cmd.parseCommandArgs()
	if version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}
	// 初始化配置，参数配置，redis 设置
	config.Init(configFile)
	// 初始化队列
	delayqueue.Init()

	// 运行web server
	cmd.runWeb()
}

func (cmd *Cmd) parseCommandArgs() {
	// 配置文件
	flag.StringVar(&configFile, "c", "", "./delay-queue -c /path/to/delay-queue.conf")
	// 版本
	flag.BoolVar(&version, "v", false, "./delay-queue -v")
	// 生成命令
	flag.Parse()
}

func (cmd *Cmd) runWeb() {

	// 有详细的说明：https://studygolang.com/resources/4657 讲的非常详细
	http.HandleFunc("/push", routers.Push)
	http.HandleFunc("/pop", routers.Pop)
	http.HandleFunc("/finish", routers.Delete)
	http.HandleFunc("/delete", routers.Delete)
	http.HandleFunc("/get", routers.Get) // 查询 job

	log.Printf("listen %s\n", config.Setting.BindAddress)


	err := http.ListenAndServe(config.Setting.BindAddress, nil)
	if err != nil {
		log.Fatalln(err)
	}
}
