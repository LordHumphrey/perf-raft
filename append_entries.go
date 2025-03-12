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
		Term:          r.getCurrentTerm(),
		LastLog:       r.getLastIndex(),
		Success:       false,
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

	// 处理新的日志条目
	if len(a.Entries) > 0 {
		start := time.Now()

		// 删除冲突的日志条目，跳过重复的条目
		lastLogIdx, _ := r.getLastLog()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {
				newEntries = a.Entries[i:]
				break
			}
			var storeEntry Log
			if err := r.logs.GetLog(entry.Index, &storeEntry); err != nil {
				r.logger.Warn("无法获取日志条目",
					"index", entry.Index,
					"error", err)
				return
			}
			if entry.Term != storeEntry.Term {
				r.logger.Warn("清除日志后缀", "from", entry.Index, "to", lastLogIdx)
				if err := r.logs.DeleteRange(entry.Index, lastLogIdx); err != nil {
					r.logger.Error("无法清除日志后缀", "error", err)
					return
				}
				if entry.Index <= r.configurations.latestIndex {
					r.setLatestConfiguration(r.configurations.committed, r.configurations.committedIndex)
				}
				newEntries = a.Entries[i:]
				break
			}
		}

		if n := len(newEntries); n > 0 {
			// 追加新的日志条目
			if err := r.logs.StoreLogs(newEntries); err != nil {
				r.logger.Error("无法追加到日志", "error", err)
				return
			}

			// 处理新的配置变更
			for _, newEntry := range newEntries {
				if err := r.processConfigurationLogEntry(newEntry); err != nil {
					r.logger.Warn("无法追加条目",
						"index", newEntry.Index,
						"error", err)
					rpcErr = err
					return
				}
			}

			// 更新最后的日志
			last := newEntries[n-1]
			r.setLastLog(last.Index, last.Term)
		}

		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "storeLogs"}, start)
	}

	// 更新提交索引
	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		start := time.Now()
		idx := min(a.LeaderCommitIndex, r.getLastIndex())
		r.setCommitIndex(idx)
		if r.configurations.latestIndex <= idx {
			r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		}
		r.processLogs(idx, nil)
		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "processLogs"}, start)
	}

	// 如果是核心节点且有新的日志条目，更新核心节点响应状态
	if len(a.Entries) > 0 && r.coreNodesState != nil && r.coreNodesState.isCoreNode(r.localID) {
		r.coreNodesState.updateCoreNodeResponse(r.localID, true)
	}

	// 一切顺利，设置成功
	resp.Success = true
	r.setLastContact()
}
