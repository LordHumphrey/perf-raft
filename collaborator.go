// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-metrics/compat"
)

// CollaboratorReplicateRequest 是领导者发送给协作者的请求，
// 包含需要复制到非核心节点的日志信息
type CollaboratorReplicateRequest struct {
	RPCHeader

	// 当前任期
	Term uint64

	// 非核心节点信息列表
	NonCoreNodes []NonCoreNodeInfo

	// 需要复制的日志条目
	Entries []*Log

	// 领导者的提交索引
	LeaderCommitIndex uint64
}

// GetRPCHeader 实现WithRPCHeader接口
func (r *CollaboratorReplicateRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// NonCoreNodeInfo 包含非核心节点的信息
type NonCoreNodeInfo struct {
	// 节点ID
	ID ServerID

	// 节点地址
	Addr ServerAddress

	// 下一个要发送的日志索引
	NextIndex uint64

	// 已经复制到该节点的最高日志索引
	MatchIndex uint64
}

// CollaboratorReplicateResponse 是协作者返回给领导者的响应
type CollaboratorReplicateResponse struct {
	RPCHeader

	// 当前任期，如果协作者发现自己的任期更高，领导者需要更新自己的任期
	Term uint64

	// 非核心节点的响应结果
	Results []NonCoreNodeResult

	// 操作是否成功
	Success bool
}

// GetRPCHeader 实现WithRPCHeader接口
func (r *CollaboratorReplicateResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// NonCoreNodeResult 包含非核心节点的响应结果
type NonCoreNodeResult struct {
	// 节点ID
	ID ServerID

	// 节点地址
	Addr ServerAddress

	// 操作是否成功
	Success bool

	// 节点的最新日志索引
	LastLog uint64
}

// collaboratorReplicate 处理来自领导者的CollaboratorReplicate RPC请求
// 协作者接收到请求后，向非核心节点发送AppendEntries请求
func (r *Raft) collaboratorReplicate(rpc RPC, a *CollaboratorReplicateRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "collaboratorReplicate"}, time.Now())

	// 设置响应
	resp := &CollaboratorReplicateResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Success:   false,
		Results:   make([]NonCoreNodeResult, 0, len(a.NonCoreNodes)),
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// 忽略旧的任期
	if a.Term < r.getCurrentTerm() {
		return
	}

	// 如果看到更新的任期，增加任期并转换为跟随者
	if a.Term > r.getCurrentTerm() {
		// 确保转换为跟随者
		r.setState(Follower)
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
		return
	}

	// 检查自己是否是协作者（核心节点组中的第一个非领导者节点）
	if !r.isCollaborator() {
		r.logger.Warn("收到CollaboratorReplicate请求，但不是协作者节点")
		return
	}

	// 如果没有非核心节点或日志条目，直接返回成功
	if len(a.NonCoreNodes) == 0 || len(a.Entries) == 0 {
		resp.Success = true
		return
	}

	// 向每个非核心节点发送AppendEntries请求
	for _, node := range a.NonCoreNodes {
		result := NonCoreNodeResult{
			ID:      node.ID,
			Addr:    node.Addr,
			Success: false,
			LastLog: node.MatchIndex,
		}

		// 构造AppendEntries请求
		req := &AppendEntriesRequest{
			RPCHeader:         r.getRPCHeader(),
			Term:              a.Term,
			Leader:            r.trans.EncodePeer(r.localID, r.localAddr),
			LeaderCommitIndex: a.LeaderCommitIndex,
		}

		// 设置前一个日志条目的索引和任期
		if node.NextIndex > 1 {
			var prevLog Log
			if err := r.logs.GetLog(node.NextIndex-1, &prevLog); err != nil {
				r.logger.Error("无法获取前一个日志", "error", err, "index", node.NextIndex-1)
				resp.Results = append(resp.Results, result)
				continue
			}
			req.PrevLogEntry = node.NextIndex - 1
			req.PrevLogTerm = prevLog.Term
		}

		// 添加需要发送的日志条目
		var newEntries []*Log
		for _, entry := range a.Entries {
			if entry.Index >= node.NextIndex {
				// 创建日志条目的深拷贝，避免引用原始日志对象
				entryCopy := &Log{
					Index:      entry.Index,
					Term:       entry.Term,
					Type:       entry.Type,
					Data:       append([]byte(nil), entry.Data...),
					Extensions: entry.Extensions,
				}
				newEntries = append(newEntries, entryCopy)
			}
		}

		// 如果没有新的日志条目需要发送，跳过
		if len(newEntries) == 0 {
			result.Success = true
			resp.Results = append(resp.Results, result)
			continue
		}

		req.Entries = newEntries

		// 发送AppendEntries请求
		var appendResp AppendEntriesResponse
		if err := r.trans.AppendEntries(node.ID, node.Addr, req, &appendResp); err != nil {
			r.logger.Error("向非核心节点发送AppendEntries失败", "node-id", node.ID, "error", err)
			resp.Results = append(resp.Results, result)
			continue
		}

		// 处理响应
		result.Success = appendResp.Success
		result.LastLog = appendResp.LastLog
		resp.Results = append(resp.Results, result)
	}

	// 如果至少有一个非核心节点成功，则设置成功标志
	for _, result := range resp.Results {
		if result.Success {
			resp.Success = true
			break
		}
	}
}

// isCollaborator 判断当前节点是否是协作者（核心节点组中的第一个非领导者节点）
func (r *Raft) isCollaborator() bool {
	// 如果核心节点状态未初始化，返回false
	if r.coreNodesState == nil {
		return false
	}

	// 获取核心节点列表
	coreNodes := r.coreNodesState.getCoreNodes()
	if len(coreNodes) <= 1 {
		return false
	}

	// 获取当前领导者ID
	leaderID := r.leaderID

	// 如果没有领导者，返回false
	if leaderID == "" {
		return false
	}

	// 如果当前节点是领导者，它不能是协作者
	if r.localID == leaderID {
		return false
	}

	// 遍历核心节点，找到第一个非领导者节点
	for _, nodeID := range coreNodes {
		// 跳过领导者节点
		if nodeID == leaderID {
			continue
		}

		// 如果当前节点是第一个非领导者核心节点，则它是协作者
		return nodeID == r.localID
	}

	return false
}

// replicateToNonCoreNodes 由领导者调用，将日志复制到非核心节点
// 如果有协作者可用，则通过协作者进行复制
func (r *Raft) replicateToNonCoreNodes() {
	// 如果不是领导者，直接返回
	if r.getState() != Leader {
		return
	}

	// 如果leaderState还没有初始化，直接返回
	if r.leaderState.replState == nil {
		return
	}

	// 获取协作者节点
	collaborator := r.getCollaborator()
	if collaborator == nil {
		r.logger.Debug("没有可用的协作者节点")
		return
	}

	// 获取非核心节点列表
	nonCoreNodes := r.getNonCoreNodes()
	if len(nonCoreNodes) == 0 {
		return
	}

	// 获取需要复制的日志条目
	lastLogIdx, _ := r.getLastLog()
	commitIdx := r.getCommitIndex()

	// 如果没有新的日志需要提交，直接返回
	if commitIdx >= lastLogIdx {
		return
	}

	var entries []*Log
	for idx := commitIdx + 1; idx <= lastLogIdx; idx++ {
		var log Log
		if err := r.logs.GetLog(idx, &log); err != nil {
			r.logger.Error("无法获取日志条目", "index", idx, "error", err)
			return
		}
		entries = append(entries, &log)
	}

	if len(entries) == 0 {
		return
	}

	// 构造CollaboratorReplicate请求
	req := &CollaboratorReplicateRequest{
		RPCHeader:         r.getRPCHeader(),
		Term:              r.getCurrentTerm(),
		NonCoreNodes:      nonCoreNodes,
		Entries:           entries,
		LeaderCommitIndex: commitIdx,
	}

	// 发送请求给协作者
	var resp CollaboratorReplicateResponse
	if err := r.trans.CollaboratorReplicate(collaborator.ID, collaborator.Address, req, &resp); err != nil {
		r.logger.Error("向协作者发送CollaboratorReplicate请求失败", "error", err)
		return
	}

	// 处理协作者的响应
	if resp.Term > r.getCurrentTerm() {
		r.setState(Follower)
		r.setCurrentTerm(resp.Term)
		return
	}

	// 更新非核心节点的复制状态
	for _, result := range resp.Results {
		if s, ok := r.leaderState.replState[result.ID]; ok {
			if result.Success {
				// 更新matchIndex和nextIndex
				atomic.StoreUint64(&s.matchIndex, result.LastLog)
				atomic.StoreUint64(&s.nextIndex, result.LastLog+1)
			} else if result.LastLog > 0 {
				// 如果失败但返回了LastLog，可以尝试回退nextIndex
				nextIdx := min(atomic.LoadUint64(&s.nextIndex), result.LastLog+1)
				atomic.StoreUint64(&s.nextIndex, nextIdx)
			}
		}
	}
}

// getCollaborator 获取协作者节点
func (r *Raft) getCollaborator() *Server {
	// 如果自己不是领导者，不需要协作者
	if r.getState() != Leader {
		return nil
	}

	if r.coreNodesState == nil {
		return nil
	}

	coreNodes := r.coreNodesState.getCoreNodes()
	if len(coreNodes) <= 1 {
		return nil
	}

	// 协作者是核心节点组中的第一个非领导者节点
	for _, nodeID := range coreNodes {
		// 跳过自己（领导者）
		if nodeID == r.localID {
			continue
		}

		// 从配置中查找协作者的地址
		configuration := r.getLatestConfiguration()
		for _, server := range configuration.Servers {
			if server.ID == nodeID {
				// 检查节点是否在线（通过检查replState）
				if _, ok := r.leaderState.replState[server.ID]; ok {
					return &server
				}
			}
		}
	}

	return nil
}

// getNonCoreNodes 获取需要更新的非核心节点列表
func (r *Raft) getNonCoreNodes() []NonCoreNodeInfo {
	var nonCoreNodes []NonCoreNodeInfo

	// 如果核心节点状态未初始化，返回空列表
	if r.coreNodesState == nil {
		return nonCoreNodes
	}

	// 获取所有节点
	configuration := r.getLatestConfiguration()

	// 遍历所有节点，找出非核心节点
	for _, server := range configuration.Servers {
		// 跳过自己
		if server.ID == r.localID {
			continue
		}

		// 跳过非投票节点
		if server.Suffrage != Voter {
			continue
		}

		// 检查是否是核心节点
		if r.coreNodesState.isCoreNode(server.ID) {
			continue
		}

		// 获取节点的复制状态
		if s, ok := r.leaderState.replState[server.ID]; ok {
			// 检查是否有需要复制的日志
			nextIdx := atomic.LoadUint64(&s.nextIndex)
			lastLogIdx, _ := r.getLastLog()

			// 如果节点已经是最新的，跳过
			if nextIdx > lastLogIdx {
				continue
			}

			nonCoreNode := NonCoreNodeInfo{
				ID:         server.ID,
				Addr:       server.Address,
				NextIndex:  nextIdx,
				MatchIndex: atomic.LoadUint64(&s.matchIndex),
			}
			nonCoreNodes = append(nonCoreNodes, nonCoreNode)
		}
	}

	return nonCoreNodes
}

// leaderReplicate 是领导者用来将日志复制到协作者节点的方法
// 领导者将需要更新的非核心节点信息和日志条目发送给协作者节点
func (r *Raft) leaderReplicate() {
	// 如果不是领导者，直接返回
	if r.getState() != Leader {
		return
	}

	// 获取协作者节点
	collaborator := r.getCollaborator()
	if collaborator == nil {
		return
	}

	// 获取需要更新的非核心节点
	nonCoreNodes := r.getNonCoreNodes()
	if len(nonCoreNodes) == 0 {
		return
	}

	// 获取需要复制的日志条目
	var entries []*Log
	for _, node := range nonCoreNodes {
		// 找出所有需要发送的日志条目
		nextIndex := node.NextIndex
		lastLogIdx, _ := r.getLastLog()
		for i := nextIndex; i <= lastLogIdx; i++ {
			var log Log
			if err := r.logs.GetLog(i, &log); err != nil {
				r.logger.Error("无法获取日志", "error", err, "index", i)
				continue
			}

			// 检查是否已经添加过这个日志条目
			found := false
			for _, entry := range entries {
				if entry.Index == log.Index {
					found = true
					break
				}
			}

			if !found {
				// 创建日志条目的深拷贝
				entryCopy := &Log{
					Index:      log.Index,
					Term:       log.Term,
					Type:       log.Type,
					Data:       append([]byte(nil), log.Data...),
					Extensions: log.Extensions,
				}
				entries = append(entries, entryCopy)
			}
		}
	}

	// 如果没有需要复制的日志条目，直接返回
	if len(entries) == 0 {
		return
	}

	// 构造CollaboratorReplicate请求
	req := &CollaboratorReplicateRequest{
		RPCHeader:         r.getRPCHeader(),
		Term:              r.getCurrentTerm(),
		LeaderCommitIndex: r.getCommitIndex(),
		NonCoreNodes:      nonCoreNodes,
		Entries:           entries,
	}

	// 发送CollaboratorReplicate请求
	var resp CollaboratorReplicateResponse
	if err := r.trans.CollaboratorReplicate(collaborator.ID, collaborator.Address, req, &resp); err != nil {
		r.logger.Error("向协作者节点发送CollaboratorReplicate失败", "collaborator-id", collaborator.ID, "error", err)
		return
	}

	// 处理响应
	if resp.Term > r.getCurrentTerm() {
		r.setState(Follower)
		r.setCurrentTerm(resp.Term)
		return
	}

	if resp.Success {
		// 更新非核心节点的复制状态
		for _, result := range resp.Results {
			if result.Success {
				s := r.leaderState.replState[result.ID]
				if s != nil && s.nextIndex <= result.LastLog {
					s.matchIndex = result.LastLog
					s.nextIndex = result.LastLog + 1
				}
			}
		}

		// 尝试提交日志
		r.updateCommitIndex()
	}
}

// updateCommitIndex 尝试更新领导者的提交索引
func (r *Raft) updateCommitIndex() {
	// 如果不是领导者，直接返回
	if r.getState() != Leader {
		return
	}

	// 获取最后一个日志索引
	lastLogIdx, _ := r.getLastLog()

	// 从最后一个日志索引开始，向前查找可以提交的日志
	for idx := lastLogIdx; idx > r.getCommitIndex(); idx-- {
		// 获取该索引处的日志条目
		var log Log
		if err := r.logs.GetLog(idx, &log); err != nil {
			r.logger.Error("无法获取日志", "error", err, "index", idx)
			continue
		}

		// 如果日志条目的任期不是当前任期，跳过
		if log.Term != r.getCurrentTerm() {
			continue
		}

		// 计算已经复制到多少节点
		count := 1 // 包括自己
		for _, server := range r.getLatestConfiguration().Servers {
			if server.ID == r.localID {
				continue
			}

			if server.Suffrage != Voter {
				continue
			}

			if repl, ok := r.leaderState.replState[server.ID]; ok {
				if repl.matchIndex >= idx {
					count++
				}
			}
		}

		// 如果已经复制到多数节点，更新提交索引
		if count > len(r.getLatestConfiguration().Servers)/2 {
			r.setCommitIndex(idx)
			r.logger.Debug("更新提交索引", "index", idx)
			break
		}
	}
}
