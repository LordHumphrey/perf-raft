// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sync/atomic"
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

// TestRaft_CollaboratorWithNonCoreNodes 测试协作者向非核心节点分发日志的功能
func TestRaft_CollaboratorWithNonCoreNodes(t *testing.T) {
	// 创建一个5节点集群
	cluster := MakeCluster(5, t, nil)
	defer cluster.Close()

	// 等待领导者选举完成
	leader := cluster.Leader()
	if leader == nil {
		t.Fatalf("应该有一个领导者")
	}

	// 初始化核心节点状态
	// 在5个节点中，选择前3个作为核心节点（(5/2)+1=3）
	for _, r := range cluster.rafts {
		r.coreNodesState = newCoreNodesState()
		r.coreNodesState.updateCoreNodes(r.configurations.latest)
	}

	// 确认核心节点数量
	coreNodes := leader.coreNodesState.getCoreNodes()
	if len(coreNodes) != 3 {
		t.Fatalf("核心节点数量应该是3，实际是%d", len(coreNodes))
	}

	// 找出协作者节点
	var collaborator *Raft
	collaboratorID := ""
	for _, nodeID := range coreNodes {
		if nodeID != leader.localID {
			collaboratorID = string(nodeID)
			break
		}
	}

	if collaboratorID == "" {
		t.Fatalf("找不到协作者节点")
	}

	// 获取协作者节点实例
	for _, r := range cluster.rafts {
		if string(r.localID) == collaboratorID {
			collaborator = r
			break
		}
	}

	if collaborator == nil {
		t.Fatalf("找不到协作者节点实例")
	}

	// 验证协作者节点身份
	if !collaborator.isCollaborator() {
		t.Fatalf("协作者节点应该返回isCollaborator()=true")
	}

	// 应用一些日志条目
	for i := 0; i < 10; i++ {
		future := leader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(300 * time.Millisecond)

	// 手动调用replicateToNonCoreNodes
	leader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(300 * time.Millisecond)

	// 检查所有节点的日志是否一致
	lastIndex := leader.getLastIndex()
	for _, r := range cluster.rafts {
		if r.getLastIndex() != lastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), lastIndex)
		}
	}

	// 检查非核心节点的matchIndex是否正确更新
	for _, r := range cluster.rafts {
		// 跳过核心节点
		if leader.coreNodesState.isCoreNode(r.localID) {
			continue
		}

		// 获取节点的复制状态
		if s, ok := leader.leaderState.replState[r.localID]; ok {
			matchIndex := atomic.LoadUint64(&s.matchIndex)
			if matchIndex != lastIndex {
				t.Fatalf("非核心节点的matchIndex未正确更新: %d vs %d", matchIndex, lastIndex)
			}
		}
	}
}

// TestRaft_CollaboratorFailover 测试当协作者节点故障时，领导者能够正确处理
func TestRaft_CollaboratorFailover(t *testing.T) {
	// 创建一个5节点集群
	cluster := MakeCluster(5, t, nil)
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

	// 找出协作者节点
	var collaborator *Raft
	collaboratorID := ""
	coreNodes := leader.coreNodesState.getCoreNodes()
	for _, nodeID := range coreNodes {
		if nodeID != leader.localID {
			collaboratorID = string(nodeID)
			break
		}
	}

	if collaboratorID == "" {
		t.Fatalf("找不到协作者节点")
	}

	// 获取协作者节点实例
	for _, r := range cluster.rafts {
		if string(r.localID) == collaboratorID {
			collaborator = r
			break
		}
	}

	if collaborator == nil {
		t.Fatalf("找不到协作者节点实例")
	}

	// 应用一些日志条目
	for i := 0; i < 5; i++ {
		future := leader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(300 * time.Millisecond)

	// 模拟协作者节点故障（断开连接）
	cluster.Disconnect(collaborator.localAddr)

	// 等待一段时间，让系统检测到协作者节点故障
	time.Sleep(300 * time.Millisecond)

	// 应用更多日志条目
	for i := 5; i < 10; i++ {
		future := leader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 手动调用replicateToNonCoreNodes
	leader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(300 * time.Millisecond)

	// 检查非协作者节点的日志是否一致
	lastIndex := leader.getLastIndex()
	for _, r := range cluster.rafts {
		// 跳过协作者节点
		if string(r.localID) == collaboratorID {
			continue
		}

		if r.getLastIndex() != lastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), lastIndex)
		}
	}

	// 重新连接协作者节点
	cluster.FullyConnect()

	// 等待一段时间，让协作者节点赶上
	time.Sleep(500 * time.Millisecond)

	// 检查协作者节点是否赶上
	if collaborator.getLastIndex() != lastIndex {
		t.Fatalf("协作者节点日志不一致: %d vs %d", collaborator.getLastIndex(), lastIndex)
	}

	// 应用更多日志条目
	for i := 10; i < 15; i++ {
		future := leader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 手动调用replicateToNonCoreNodes
	leader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(300 * time.Millisecond)

	// 检查所有节点的日志是否一致
	finalLastIndex := leader.getLastIndex()
	for _, r := range cluster.rafts {
		if r.getLastIndex() != finalLastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), finalLastIndex)
		}
	}
}

// TestRaft_CollaboratorLargeLogBatch 测试协作者处理大量日志的能力
func TestRaft_CollaboratorLargeLogBatch(t *testing.T) {
	// 创建一个5节点集群
	cluster := MakeCluster(5, t, nil)
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

	// 找出协作者节点
	var collaborator *Raft
	collaboratorID := ""
	coreNodes := leader.coreNodesState.getCoreNodes()
	for _, nodeID := range coreNodes {
		if nodeID != leader.localID {
			collaboratorID = string(nodeID)
			break
		}
	}

	if collaboratorID == "" {
		t.Fatalf("找不到协作者节点")
	}

	// 获取协作者节点实例
	for _, r := range cluster.rafts {
		if string(r.localID) == collaboratorID {
			collaborator = r
			break
		}
	}

	if collaborator == nil {
		t.Fatalf("找不到协作者节点实例")
	}

	// 应用大量日志条目
	logCount := 100
	for i := 0; i < logCount; i++ {
		// 创建一个较大的日志条目
		data := make([]byte, 1024) // 1KB
		for j := 0; j < len(data); j++ {
			data[j] = byte(i % 256)
		}
		future := leader.Apply(data, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(1 * time.Second)

	// 手动调用replicateToNonCoreNodes
	leader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(1 * time.Second)

	// 检查所有节点的日志是否一致
	lastIndex := leader.getLastIndex()
	for _, r := range cluster.rafts {
		if r.getLastIndex() != lastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), lastIndex)
		}
	}

	// 检查非核心节点的matchIndex是否正确更新
	for _, r := range cluster.rafts {
		// 跳过核心节点
		if leader.coreNodesState.isCoreNode(r.localID) {
			continue
		}

		// 获取节点的复制状态
		if s, ok := leader.leaderState.replState[r.localID]; ok {
			matchIndex := atomic.LoadUint64(&s.matchIndex)
			if matchIndex != lastIndex {
				t.Fatalf("非核心节点的matchIndex未正确更新: %d vs %d", matchIndex, lastIndex)
			}
		}
	}

	// 验证日志内容
	for _, r := range cluster.rafts {
		for i := uint64(1); i <= lastIndex; i++ {
			var log Log
			if err := r.logs.GetLog(i, &log); err != nil {
				t.Fatalf("获取日志失败: %v", err)
			}

			// 验证日志内容
			if i > uint64(logCount) {
				continue
			}

			expectedByte := byte((i - 1) % 256)
			if len(log.Data) != 1024 || log.Data[0] != expectedByte {
				t.Fatalf("日志内容不正确: index=%d, len=%d, first_byte=%d, expected_byte=%d",
					i, len(log.Data), log.Data[0], expectedByte)
			}
		}
	}
}

// TestRaft_CollaboratorLeaderChange 测试领导者变更后协作者的行为
func TestRaft_CollaboratorLeaderChange(t *testing.T) {
	// 创建一个5节点集群
	cluster := MakeCluster(5, t, nil)
	defer cluster.Close()

	// 等待领导者选举完成
	oldLeader := cluster.Leader()
	if oldLeader == nil {
		t.Fatalf("应该有一个领导者")
	}

	// 初始化核心节点状态
	for _, r := range cluster.rafts {
		r.coreNodesState = newCoreNodesState()
		r.coreNodesState.updateCoreNodes(r.configurations.latest)
	}

	// 找出旧的协作者节点
	var oldCollaborator *Raft
	oldCollaboratorID := ""
	coreNodes := oldLeader.coreNodesState.getCoreNodes()
	for _, nodeID := range coreNodes {
		if nodeID != oldLeader.localID {
			oldCollaboratorID = string(nodeID)
			break
		}
	}

	if oldCollaboratorID == "" {
		t.Fatalf("找不到旧的协作者节点")
	}

	// 获取旧的协作者节点实例
	for _, r := range cluster.rafts {
		if string(r.localID) == oldCollaboratorID {
			oldCollaborator = r
			break
		}
	}

	if oldCollaborator == nil {
		t.Fatalf("找不到旧的协作者节点实例")
	}

	// 应用一些日志条目
	for i := 0; i < 5; i++ {
		future := oldLeader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(300 * time.Millisecond)

	// 断开旧领导者的连接，触发新的领导者选举
	cluster.Disconnect(oldLeader.localAddr)

	// 等待新的领导者选举完成
	time.Sleep(500 * time.Millisecond)

	// 获取新的领导者
	newLeader := cluster.Leader()
	if newLeader == nil {
		t.Fatalf("应该有一个新的领导者")
	}

	if newLeader.localID == oldLeader.localID {
		t.Fatalf("新的领导者不应该是旧的领导者")
	}

	// 找出新的协作者节点
	var newCollaborator *Raft
	newCollaboratorID := ""
	coreNodes = newLeader.coreNodesState.getCoreNodes()
	for _, nodeID := range coreNodes {
		if nodeID != newLeader.localID {
			newCollaboratorID = string(nodeID)
			break
		}
	}

	if newCollaboratorID == "" {
		t.Fatalf("找不到新的协作者节点")
	}

	// 获取新的协作者节点实例
	for _, r := range cluster.rafts {
		if string(r.localID) == newCollaboratorID {
			newCollaborator = r
			break
		}
	}

	if newCollaborator == nil {
		t.Fatalf("找不到新的协作者节点实例")
	}

	// 验证新的协作者节点身份
	if !newCollaborator.isCollaborator() {
		t.Fatalf("新的协作者节点应该返回isCollaborator()=true")
	}

	// 应用更多日志条目
	for i := 5; i < 10; i++ {
		future := newLeader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 等待日志复制完成
	time.Sleep(300 * time.Millisecond)

	// 手动调用replicateToNonCoreNodes
	newLeader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(300 * time.Millisecond)

	// 检查非旧领导者节点的日志是否一致
	newLastIndex := newLeader.getLastIndex()
	for _, r := range cluster.rafts {
		// 跳过旧领导者节点
		if r.localID == oldLeader.localID {
			continue
		}

		if r.getLastIndex() != newLastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), newLastIndex)
		}
	}

	// 重新连接旧领导者
	cluster.FullyConnect()

	// 等待一段时间，让旧领导者赶上
	time.Sleep(500 * time.Millisecond)

	// 检查旧领导者是否赶上
	if oldLeader.getLastIndex() != newLastIndex {
		t.Fatalf("旧领导者日志不一致: %d vs %d", oldLeader.getLastIndex(), newLastIndex)
	}

	// 应用更多日志条目
	for i := 10; i < 15; i++ {
		future := newLeader.Apply([]byte{byte(i)}, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// 手动调用replicateToNonCoreNodes
	newLeader.replicateToNonCoreNodes()

	// 等待一段时间
	time.Sleep(300 * time.Millisecond)

	// 检查所有节点的日志是否一致
	finalLastIndex := newLeader.getLastIndex()
	for _, r := range cluster.rafts {
		if r.getLastIndex() != finalLastIndex {
			t.Fatalf("日志不一致: %d vs %d", r.getLastIndex(), finalLastIndex)
		}
	}
}
