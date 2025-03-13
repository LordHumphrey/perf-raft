package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	httpd "github.com/hashicorp/etcdraft/http"
	"github.com/hashicorp/etcdraft/store"
)

// 命令行默认值
const (
	DefaultHTTPAddr = "localhost:12379"
	DefaultRaftAddr = "localhost:12000"
)

// 命令行参数
var inmem bool
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string
var dataDir string

func init() {
	flag.BoolVar(&inmem, "inmem", false, "使用内存存储Raft状态")
	flag.StringVar(&httpAddr, "http-addr", DefaultHTTPAddr, "设置HTTP绑定地址")
	flag.StringVar(&raftAddr, "raft-addr", DefaultRaftAddr, "设置Raft绑定地址")
	flag.StringVar(&joinAddr, "join", "", "设置加入地址，如果有的话")
	flag.StringVar(&nodeID, "id", "", "节点ID。如果未设置，则与Raft绑定地址相同")
	flag.StringVar(&dataDir, "data-dir", "", "数据目录路径")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "用法: %s [选项]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// 确保数据目录存在
	if dataDir == "" {
		fmt.Fprintf(os.Stderr, "未指定数据目录\n")
		os.Exit(1)
	}

	// 如果未设置节点ID，则使用Raft地址
	if nodeID == "" {
		nodeID = raftAddr
	}

	// 确保Raft存储目录存在
	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		log.Fatalf("创建Raft存储目录失败: %s", err.Error())
	}

	// 创建并打开存储
	s := store.New(inmem)
	s.RaftDir = raftDir
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("打开存储失败: %s", err.Error())
	}
	defer s.Close()

	// 创建并启动HTTP服务
	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("启动HTTP服务失败: %s", err.Error())
	}
	defer h.Close()

	// 如果指定了加入地址，则发送加入请求
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("加入节点 %s 失败: %s", joinAddr, err.Error())
		}
	}

	// 启动成功！
	log.Printf("etcdraft 启动成功，监听 http://%s", httpAddr)

	// 等待中断信号
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("etcdraft 退出")
}

// join发送加入请求到指定的地址
func join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
