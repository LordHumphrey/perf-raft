#!/bin/bash

# 设置调试模式
set -x

# 设置默认节点数量
DEFAULT_NODES=5

# 检查参数，如果没有提供则使用默认值
NODES=${1:-$DEFAULT_NODES}

# 集群数据目录
CLUSTER_DIR="/tmp/kvstore"

# 关闭所有已有的etcdraft进程
echo "关闭所有已有的etcdraft进程..."
pkill -f etcdraft || true
sleep 1

# 清理旧的数据目录
rm -rf "$CLUSTER_DIR"
mkdir -p "$CLUSTER_DIR"

# 获取当前脚本所在目录的绝对路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 可执行文件路径（使用绝对路径）
EXECUTABLE="$SCRIPT_DIR/etcdraft"

# 检查可执行文件是否存在
if [ ! -x "$EXECUTABLE" ]; then
    echo "错误：可执行文件 $EXECUTABLE 不存在或没有执行权限"
    echo "当前路径: $(pwd)"
    echo "脚本目录: $SCRIPT_DIR"
    echo "文件列表："
    ls -l "$SCRIPT_DIR"
    exit 1
fi

# 启动第一个节点（初始节点）
echo "启动第一个节点（初始节点）"
$EXECUTABLE \
    -id node1 \
    -raft-addr localhost:12000 \
    -http-addr localhost:12379 \
    -data-dir "$CLUSTER_DIR/node1" &

# 等待第一个节点启动
sleep 2

# 启动其他节点
for ((i=2; i<=NODES; i++)); do
    echo "启动节点 node$i"
    $EXECUTABLE \
        -id "node$i" \
        -raft-addr "localhost:$((12000 + i - 1))" \
        -http-addr "localhost:$((12379 + i))" \
        -data-dir "$CLUSTER_DIR/node$i" \
        -join localhost:12379 &

    # 每个节点启动间隔
    sleep 1
done

# 打印集群信息
echo "已启动 $NODES 节点集群"
echo "数据目录: $CLUSTER_DIR"
echo "第一个节点HTTP地址: localhost:12379"
echo "运行的节点："
ps aux | grep etcdraft
echo "使用 'ps aux | grep etcdraft' 查看运行的节点"
echo "使用 'pkill -f etcdraft' 停止所有节点"

# 等待所有后台进程
wait

# 如果没有节点启动成功，显示详细信息
if ! pgrep -f etcdraft > /dev/null; then
    echo "错误：没有节点成功启动"
    echo "当前目录: $(pwd)"
    echo "脚本目录: $SCRIPT_DIR"
    echo "文件列表："
    ls -l "$SCRIPT_DIR"
    exit 1
fi
