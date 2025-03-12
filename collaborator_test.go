// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"
)

// TestRaft_CollaboratorBasic 测试协作者基本功能
func TestRaft_CollaboratorBasic(t *testing.T) {
	// 创建一个小型集群
	cluster := MakeCluster(3, t, nil)
	defer cluster.Close()

	// 等待领导者选举完成
	leader := cluster.Leader()
	if leader == nil {
		t.Fatalf("应该有一个领导者")
	}

	// 初始化核心节点状态
	for _, r := range cluster.rafts {
		r.coreNodesState = newCoreNodesState()
		r.coreNodesState.updateCoreNodes(r.configurations.latest)
	}

	// 应用一些日志条目
	for i := 0; i < 5; i++ {
		future := leader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(200 * time.Millisecond)

	// 检查所有节点的日志是否一致
	lastIndex := leader.getLastIndex()
	for _, r := range cluster.rafts {
		if r.getLastIndex() != lastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), lastIndex)
		}
	}
}

// TestRaft_CollaboratorReplicate 测试协作者复制函数
func TestRaft_CollaboratorReplicate(t *testing.T) {
	// 创建一个小型集群
	cluster := MakeCluster(3, t, nil)
	defer cluster.Close()

	// 等待领导者选举完成
	leader := cluster.Leader()
	if leader == nil {
		t.Fatalf("应该有一个领导者")
	}

	// 初始化核心节点状态
	for _, r := range cluster.rafts {
		r.coreNodesState = newCoreNodesState()
		r.coreNodesState.updateCoreNodes(r.configurations.latest)
	}

	// 获取一个跟随者节点作为协作者
	var follower *Raft
	for _, r := range cluster.rafts {
		if r.getState() == Follower {
			follower = r
			break
		}
	}

	if follower == nil {
		t.Fatalf("应该有一个跟随者节点")
	}

	// 应用一些日志条目
	for i := 0; i < 5; i++ {
		future := leader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(200 * time.Millisecond)

	// 手动调用replicateToNonCoreNodes
	leader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(200 * time.Millisecond)

	// 检查所有节点的日志是否一致
	lastIndex := leader.getLastIndex()
	for _, r := range cluster.rafts {
		if r.getLastIndex() != lastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), lastIndex)
		}
	}
}
