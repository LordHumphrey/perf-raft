// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sort"
	"sync"
)

// coreNodesState 用于管理核心节点的状态
type coreNodesState struct {
	// 保护以下字段的互斥锁
	sync.RWMutex

	// 核心节点列表，按照节点ID排序
	coreNodes []ServerID

	// 记录每个核心节点的响应状态
	// 当所有核心节点都响应后，可以提交日志
	coreNodeResponses map[ServerID]bool

	// 当前是否正在等待核心节点响应
	waitingForCoreNodes bool

	// 最后一个等待核心节点响应的日志索引
	lastWaitingIndex uint64
}

// newCoreNodesState 创建一个新的核心节点状态管理器
func newCoreNodesState() *coreNodesState {
	return &coreNodesState{
		coreNodeResponses: make(map[ServerID]bool),
		waitingForCoreNodes: false,
	}
}

// updateCoreNodes 根据当前配置更新核心节点列表
// 核心节点是按照节点ID排序后选择前(N/2)+1个节点
func (c *coreNodesState) updateCoreNodes(configuration Configuration) {
	c.Lock()
	defer c.Unlock()

	// 获取所有投票节点
	var voters []ServerID
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			voters = append(voters, server.ID)
		}
	}

	// 按照节点ID排序
	sort.Slice(voters, func(i, j int) bool {
		return string(voters[i]) < string(voters[j])
	})

	// 选择前(N/2)+1个节点作为核心节点
	coreNodeCount := (len(voters) / 2) + 1
	if coreNodeCount > len(voters) {
		coreNodeCount = len(voters)
	}

	c.coreNodes = voters[:coreNodeCount]

	// 重置响应状态
	c.resetCoreNodeResponses()
}

// resetCoreNodeResponses 重置所有核心节点的响应状态
func (c *coreNodesState) resetCoreNodeResponses() {
	c.coreNodeResponses = make(map[ServerID]bool)
	for _, nodeID := range c.coreNodes {
		c.coreNodeResponses[nodeID] = false
	}
}

// isCoreNode 判断给定的节点ID是否是核心节点
func (c *coreNodesState) isCoreNode(id ServerID) bool {
	c.RLock()
	defer c.RUnlock()

	for _, nodeID := range c.coreNodes {
		if nodeID == id {
			return true
		}
	}
	return false
}

// updateCoreNodeResponse 更新核心节点的响应状态
func (c *coreNodesState) updateCoreNodeResponse(id ServerID, success bool) bool {
	c.Lock()
	defer c.Unlock()

	// 如果不是核心节点或者不在等待核心节点响应，直接返回
	if !c.waitingForCoreNodes {
		return false
	}

	// 检查是否是核心节点
	isCoreNode := false
	for _, nodeID := range c.coreNodes {
		if nodeID == id {
			isCoreNode = true
			break
		}
	}

	if !isCoreNode {
		return false
	}

	// 更新响应状态
	c.coreNodeResponses[id] = success

	// 检查是否所有核心节点都已响应
	allResponded := true
	for _, responded := range c.coreNodeResponses {
		if !responded {
			allResponded = false
			break
		}
	}

	// 如果所有核心节点都已响应，重置等待状态
	if allResponded {
		c.waitingForCoreNodes = false
		return true
	}

	return false
}

// startWaitingForCoreNodes 开始等待核心节点响应
func (c *coreNodesState) startWaitingForCoreNodes(index uint64) {
	c.Lock()
	defer c.Unlock()

	c.waitingForCoreNodes = true
	c.lastWaitingIndex = index
	c.resetCoreNodeResponses()
}

// isWaitingForCoreNodes 判断是否正在等待核心节点响应
func (c *coreNodesState) isWaitingForCoreNodes() bool {
	c.RLock()
	defer c.RUnlock()

	return c.waitingForCoreNodes
}

// getLastWaitingIndex 获取最后一个等待核心节点响应的日志索引
func (c *coreNodesState) getLastWaitingIndex() uint64 {
	c.RLock()
	defer c.RUnlock()

	return c.lastWaitingIndex
}

// getCoreNodes 获取核心节点列表
func (c *coreNodesState) getCoreNodes() []ServerID {
	c.RLock()
	defer c.RUnlock()

	return append([]ServerID{}, c.coreNodes...)
}

// resetWaitingState 重置等待核心节点响应的状态
func (c *coreNodesState) resetWaitingState() {
	c.Lock()
	defer c.Unlock()

	c.waitingForCoreNodes = false
	c.resetCoreNodeResponses()
}
