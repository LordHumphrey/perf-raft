# etcdraft

etcdraft 是一个基于 Hashicorp Raft 算法实现的分布式键值存储系统，API 与 etcd v3 兼容。

## 功能特性

- 基于 Hashicorp Raft 实现的强一致性分布式键值存储
- 支持 etcd v3 API 兼容（Range、Put、Delete）
- 使用 bbolt 作为持久化存储引擎
- 支持多节点集群

## 使用方法

### 启动服务器

```bash
# 启动单节点
./Perf-Raft-KV -id node1 -raft-addr localhost:12000 -http-addr localhost:2379 -data-dir ./data/node1

# 启动集群中的其他节点
./Perf-Raft-KV -id node2 -raft-addr localhost:12001 -http-addr localhost:2380 -data-dir ./data/node2 -join localhost:2379
./Perf-Raft-KV -id node3 -raft-addr localhost:12002 -http-addr localhost:2381 -data-dir ./data/node3 -join localhost:2379
```

### 使用 etcd 客户端连接

```go
import (
    "context"
    "log"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"localhost:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    // 使用Put API
    _, err = cli.Put(context.Background(), "foo", "bar")
    if err != nil {
        log.Fatal(err)
    }

    // 使用Range API
    resp, err := cli.Get(context.Background(), "foo")
    if err != nil {
        log.Fatal(err)
    }
    for _, ev := range resp.Kvs {
        log.Printf("%s : %s\n", ev.Key, ev.Value)
    }

    // 使用Delete API
    _, err = cli.Delete(context.Background(), "foo")
    if err != nil {
        log.Fatal(err)
    }
}
```

## 架构设计

etcdraft 基于 Hashicorp Raft 实现分布式一致性，使用 bbolt 作为持久化存储引擎，并提供与 etcd v3 兼容的 API 接口。

### 组件说明

- `store`: 实现键值存储和 Raft 状态机
- `http`: 提供 HTTP 和 gRPC 服务接口
- `server`: 服务器主程序
- `api`: API 定义和工具函数

## 许可证

Apache License 2.0
