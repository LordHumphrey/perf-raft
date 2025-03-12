// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"time"

	"github.com/hashicorp/go-metrics/compat"
)

// appendEntries 处理来自领导者的AppendEntries RPC请求
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, time.Now())
	// 设置响应
	resp := &AppendEntriesResponse{
		RPCHeader:      r.getRPCHeader(),
		Term:           r.getCurrentTerm(),
		LastLog:        r.getLastIndex(),
		Success:        false,
		NoRetryBackoff: false,
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
	if a.Term > r.getCurrentTerm() || (r.getState() != Follower && !r.candidateFromLeadershipTransfer.Load()) {
		// 确保转换为跟随者
		r.setState(Follower)
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
	}

	// 保存当前领导者
	if len(a.Addr) > 0 {
		r.setLeader(r.trans.DecodePeer(a.Addr), ServerID(a.ID))
	} else {
		r.setLeader(r.trans.DecodePeer(a.Leader), ServerID(a.ID))
	}

	// 验证前一个日志条目
	if a.PrevLogEntry > 0 {
		lastIdx, lastTerm := r.getLastEntry()

		var prevLogTerm uint64
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm
		} else {
			var prevLog Log
			if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
				r.logger.Warn("无法获取前一个日志",
					"previous-index", a.PrevLogEntry,
					"last-index", lastIdx,
					"error", err)
				resp.NoRetryBackoff = true
				return
			}
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			r.logger.Warn("前一个日志term不匹配",
				"ours", prevLogTerm,
				"remote", a.PrevLogTerm)
			resp.NoRetryBackoff = true
			return
		}
	}

	// 应用日志条目
	if len(a.Entries) > 0 {
		// 获取当前最后的日志索引和任期
		lastLogIdx, lastLogTerm := r.getLastLog()

		// 删除冲突的日志条目，然后添加新的日志条目
		if err := r.logs.DeleteRange(a.PrevLogEntry+1, lastLogIdx); err != nil {
			r.logger.Error("failed to delete conflict logs", "error", err)
			return
		}

		// 追加日志条目
		if err := r.logs.StoreLogs(a.Entries); err != nil {
			r.logger.Error("failed to append logs", "error", err)
			return
		}

		// 更新最后一条日志的索引和任期
		lastLogIdx = a.Entries[len(a.Entries)-1].Index
		lastLogTerm = a.Entries[len(a.Entries)-1].Term
		r.setLastLog(lastLogIdx, lastLogTerm)
	}

	// 更新提交索引
	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		idx := min(a.LeaderCommitIndex, r.getLastIndex())
		r.setCommitIndex(idx)
		r.processCommittedLogs(r.getCommitIndex())
	}

	// 如果是核心节点且有新的日志条目，更新核心节点响应状态
	if len(a.Entries) > 0 && r.coreNodesState != nil && r.coreNodesState.isCoreNode(r.localID) {
		r.coreNodesState.updateCoreNodeResponse(r.localID, true)
	}

	// 一切正常，返回成功
	resp.Success = true
	r.setLastContact()
}

// processCommittedLogs 处理已提交但尚未应用的日志条目
func (r *Raft) processCommittedLogs(commitIndex uint64) {
	// 获取最后应用的索引
	lastApplied := r.getLastApplied()
	if commitIndex <= lastApplied {
		return
	}

	// 应用所有未应用的日志条目
	for idx := lastApplied + 1; idx <= commitIndex; idx++ {
		// 获取日志条目
		var log Log
		if err := r.logs.GetLog(idx, &log); err != nil {
			r.logger.Error("failed to get log", "index", idx, "error", err)
			panic(err)
		}

		// 应用日志条目
		switch log.Type {
		case LogCommand:
			// 将命令应用到状态机
			r.fsm.Apply(&log)
		case LogConfiguration:
			// 处理配置变更
			var configuration Configuration
			configuration = DecodeConfiguration(log.Data)
			r.setCommittedConfiguration(configuration, log.Index)
		case LogBarrier:
			// 屏障日志也需要应用到状态机
			r.fsm.Apply(&log)
		case LogNoop:
			// 忽略空操作日志
		default:
			r.logger.Warn("unrecognized log type", "type", log.Type)
		}
	}

	// 更新最后应用的索引
	r.setLastApplied(commitIndex)
}
