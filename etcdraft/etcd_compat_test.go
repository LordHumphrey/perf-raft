package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 测试配置
const (
	testHTTPAddr = "localhost:12379"
	testRaftAddr = "localhost:13000"
	testNodeID   = "test-node-1"
)

// TestEtcdClientCompat测试etcd v3客户端的兼容性
func TestEtcdClientCompat(t *testing.T) {
	// 创建临时数据目录
	dataDir, err := os.MkdirTemp("", "etcdraft-test")
	if err != nil {
		t.Fatalf("创建临时数据目录失败: %s", err)
	}
	defer os.RemoveAll(dataDir)

	// 启动服务器
	cmd := exec.Command(
		os.Args[0],
		"-http-addr", testHTTPAddr,
		"-raft-addr", testRaftAddr,
		"-id", testNodeID,
		"-data-dir", dataDir,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动服务器失败: %s", err)
	}
	defer cmd.Process.Kill()

	// 等待服务器启动
	time.Sleep(3 * time.Second)

	// 创建etcd客户端
	// 注意：gRPC端口是HTTP端口+1
	grpcPort := testHTTPAddr[0:len(testHTTPAddr)-len("79")] + "80"
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{grpcPort},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("创建etcd客户端失败: %s", err)
	}
	defer cli.Close()

	// 测试Put API
	t.Run("TestPut", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := "test-key"
		value := "test-value"

		// 测试Put
		_, err := cli.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("Put失败: %s", err)
		}

		// 验证Put结果
		resp, err := cli.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get失败: %s", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("预期1个键值对，实际得到%d个", len(resp.Kvs))
		}
		if string(resp.Kvs[0].Key) != key {
			t.Fatalf("预期键为%s，实际为%s", key, resp.Kvs[0].Key)
		}
		if string(resp.Kvs[0].Value) != value {
			t.Fatalf("预期值为%s，实际为%s", value, resp.Kvs[0].Value)
		}
	})

	// 测试Range API
	t.Run("TestRange", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 准备测试数据
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			_, err := cli.Put(ctx, key, value)
			if err != nil {
				t.Fatalf("准备测试数据失败: %s", err)
			}
		}

		// 测试单键查询
		resp, err := cli.Get(ctx, "key-5")
		if err != nil {
			t.Fatalf("Get单键失败: %s", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("预期1个键值对，实际得到%d个", len(resp.Kvs))
		}
		if string(resp.Kvs[0].Key) != "key-5" {
			t.Fatalf("预期键为key-5，实际为%s", resp.Kvs[0].Key)
		}
		if string(resp.Kvs[0].Value) != "value-5" {
			t.Fatalf("预期值为value-5，实际为%s", resp.Kvs[0].Value)
		}

		// 测试前缀查询
		resp, err = cli.Get(ctx, "key", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("Get前缀失败: %s", err)
		}
		if len(resp.Kvs) != 10 {
			t.Fatalf("预期10个键值对，实际得到%d个", len(resp.Kvs))
		}

		// 测试范围查询
		resp, err = cli.Get(ctx, "key-3", clientv3.WithRange("key-7"))
		if err != nil {
			t.Fatalf("Get范围失败: %s", err)
		}
		if len(resp.Kvs) != 4 {
			t.Fatalf("预期4个键值对，实际得到%d个", len(resp.Kvs))
		}

		// 测试限制查询
		resp, err = cli.Get(ctx, "key", clientv3.WithPrefix(), clientv3.WithLimit(5))
		if err != nil {
			t.Fatalf("Get限制失败: %s", err)
		}
		if len(resp.Kvs) != 5 {
			t.Fatalf("预期5个键值对，实际得到%d个", len(resp.Kvs))
		}
	})

	// 测试Delete API
	t.Run("TestDelete", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 准备测试数据
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("delete-key-%d", i)
			value := fmt.Sprintf("delete-value-%d", i)
			_, err := cli.Put(ctx, key, value)
			if err != nil {
				t.Fatalf("准备测试数据失败: %s", err)
			}
		}

		// 测试单键删除
		_, err := cli.Delete(ctx, "delete-key-2")
		if err != nil {
			t.Fatalf("Delete单键失败: %s", err)
		}

		// 验证单键删除结果
		resp, err := cli.Get(ctx, "delete-key-2")
		if err != nil {
			t.Fatalf("Get失败: %s", err)
		}
		if len(resp.Kvs) != 0 {
			t.Fatalf("预期0个键值对，实际得到%d个", len(resp.Kvs))
		}

		// 测试前缀删除
		_, err = cli.Delete(ctx, "delete-key", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("Delete前缀失败: %s", err)
		}

		// 验证前缀删除结果
		resp, err = cli.Get(ctx, "delete-key", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("Get失败: %s", err)
		}
		if len(resp.Kvs) != 0 {
			t.Fatalf("预期0个键值对，实际得到%d个", len(resp.Kvs))
		}
	})
}

// TestMain用于在测试前编译程序
func TestMain(m *testing.M) {
	// 如果是被测试程序启动的，则运行服务器
	if os.Getenv("TEST_ETCDRAFT_SERVER") == "1" {
		main()
		return
	}

	// 编译程序
	tempDir, err := os.MkdirTemp("", "etcdraft-build")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建临时目录失败: %s\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	binary := filepath.Join(tempDir, "etcdraft")
	cmd := exec.Command("go", "build", "-o", binary, ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "编译程序失败: %s\n", err)
		os.Exit(1)
	}

	// 设置环境变量，使测试程序可以找到编译好的程序
	os.Args[0] = binary
	os.Setenv("TEST_ETCDRAFT_SERVER", "1")

	// 运行测试
	os.Exit(m.Run())
}
