// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hashicorp/go-metrics/compat"
)

const (
	minCheckInterval          = 10 * time.Millisecond
	oldestLogGaugeInterval    = 10 * time.Second
	rpcUnexpectedCommandError = "unexpected command"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

// getRPCHeader returns an initialized RPCHeader struct for the given
// Raft instance. This structure is sent along with RPC requests and
// responses.
func (r *Raft) getRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: r.config().ProtocolVersion,
		ID:              []byte(r.config().LocalID),
		Addr:            r.trans.EncodePeer(r.config().LocalID, r.localAddr),
	}
}

// checkRPCHeader houses logic about whether this instance of Raft can process
// the given RPC message.
func (r *Raft) checkRPCHeader(rpc RPC) error {
	// Get the header off the RPC message.
	wh, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC does not have a header")
	}
	header := wh.GetRPCHeader()

	// First check is to just make sure the code can understand the
	// protocol at all.
	if header.ProtocolVersion < ProtocolVersionMin ||
		header.ProtocolVersion > ProtocolVersionMax {
		return ErrUnsupportedProtocol
	}

	// Second check is whether we should support this message, given the
	// current protocol we are configured to run. This will drop support
	// for protocol version 0 starting at protocol version 2, which is
	// currently what we want, and in general support one version back. We
	// may need to revisit this policy depending on how future protocol
	// changes evolve.
	if header.ProtocolVersion < r.config().ProtocolVersion-1 {
		return ErrUnsupportedProtocol
	}

	return nil
}

// getSnapshotVersion returns the snapshot version that should be used when
// creating snapshots, given the protocol version in use.
func getSnapshotVersion(protocolVersion ProtocolVersion) SnapshotVersion {
	// Right now we only have two versions and they are backwards compatible
	// so we don't need to look at the protocol version.
	return 1
}

// commitTuple is used to send an index that was committed,
// with an optional associated future that should be invoked.
type commitTuple struct {
	log    *Log
	future *logFuture
}

// leaderState is state that is used while we are a leader.
type leaderState struct {
	leadershipTransferInProgress int32 // indicates that a leadership transfer is in progress.
	commitCh                     chan struct{}
	commitment                   *commitment
	inflight                     *list.List // list of logFuture in log index order
	replState                    map[ServerID]*followerReplication
	notify                       map[*verifyFuture]struct{}
	stepDown                     chan struct{}
}

// setLeader is used to modify the current leader Address and ID of the cluster
func (r *Raft) setLeader(leaderAddr ServerAddress, leaderID ServerID) {
	r.leaderLock.Lock()
	oldLeaderAddr := r.leaderAddr
	r.leaderAddr = leaderAddr
	oldLeaderID := r.leaderID
	r.leaderID = leaderID
	r.leaderLock.Unlock()
	if oldLeaderAddr != leaderAddr || oldLeaderID != leaderID {
		r.observe(LeaderObservation{Leader: leaderAddr, LeaderAddr: leaderAddr, LeaderID: leaderID})
	}
}

// requestConfigChange is a helper for the above functions that make
// configuration change requests. 'req' describes the change. For timeout,
// see AddVoter.
func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &configurationChangeFuture{
		req: req,
	}
	future.init()

	// 如果当前正在等待核心节点响应，重置等待状态
	// 这是为了避免在配置变更时阻塞
	if r.getState() == Leader && r.coreNodesState != nil && r.coreNodesState.isWaitingForCoreNodes() {
		r.coreNodesState.resetWaitingState()
	}

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

// run the main thread that handles leadership and RPC requests.
func (r *Raft) run() {
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			// Clear the leader to prevent forwarding
			r.setLeader("", "")
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the main loop while in the follower state.
func (r *Raft) runFollower() {
	didWarn := false
	leaderAddr, leaderID := r.LeaderWithID()
	r.logger.Info("entering follower state", "follower", r, "leader-address", leaderAddr, "leader-id", leaderID)
	metrics.IncrCounter([]string{"raft", "state", "follower"}, 1)
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		r.mainThreadSaturation.sleeping()

		select {
		case rpc := <-r.rpcCh:
			r.mainThreadSaturation.working()
			r.processRPC(rpc)

		case c := <-r.configurationChangeCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			v.respond(ErrNotLeader)

		case ur := <-r.userRestoreCh:
			r.mainThreadSaturation.working()
			// Reject any restores since we are not the leader
			ur.respond(ErrNotLeader)

		case l := <-r.leadershipTransferCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			l.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			r.mainThreadSaturation.working()
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			r.mainThreadSaturation.working()
			b.respond(r.liveBootstrap(b.configuration))

		case <-r.leaderNotifyCh:
			//  Ignore since we are not the leader

		case <-r.followerNotifyCh:
			heartbeatTimer = time.After(0)

		case <-heartbeatTimer:
			r.mainThreadSaturation.working()
			// Restart the heartbeat timer
			hbTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(hbTimeout)

			// Check if we have had a successful contact
			lastContact := r.LastContact()
			if time.Since(lastContact) < hbTimeout {
				continue
			}

			// Heartbeat failed! Transition to the candidate state
			lastLeaderAddr, lastLeaderID := r.LeaderWithID()
			r.setLeader("", "")

			if r.configurations.latestIndex == 0 {
				if !didWarn {
					r.logger.Warn("no known peers, aborting election")
					didWarn = true
				}
			} else if r.configurations.latestIndex == r.configurations.committedIndex &&
				!hasVote(r.configurations.latest, r.localID) {
				if !didWarn {
					r.logger.Warn("not part of stable configuration, aborting election")
					didWarn = true
				}
			} else {
				metrics.IncrCounter([]string{"raft", "transition", "heartbeat_timeout"}, 1)
				if hasVote(r.configurations.latest, r.localID) {
					r.logger.Warn("heartbeat timeout reached, starting election", "last-leader-addr", lastLeaderAddr, "last-leader-id", lastLeaderID)
					r.setState(Candidate)
					return
				} else if !didWarn {
					r.logger.Warn("heartbeat timeout reached, not part of a stable configuration or a non-voter, not triggering a leader election")
					didWarn = true
				}
			}

		case <-r.shutdownCh:
			return
		}
	}
}

// liveBootstrap attempts to seed an initial configuration for the cluster. See
// the Raft object's member BootstrapCluster for more details. This must only be
// called on the main thread, and only makes sense in the follower state.
func (r *Raft) liveBootstrap(configuration Configuration) error {
	if !hasVote(configuration, r.localID) {
		// Reject this operation since we are not a voter
		return ErrNotVoter
	}

	// Use the pre-init API to make the static updates.
	cfg := r.config()
	err := BootstrapCluster(&cfg, r.logs, r.stable, r.snapshots, r.trans, configuration)
	if err != nil {
		return err
	}

	// Make the configuration live.
	var entry Log
	if err := r.logs.GetLog(1, &entry); err != nil {
		panic(err)
	}
	r.setCurrentTerm(1)
	r.setLastLog(entry.Index, entry.Term)
	return r.processConfigurationLogEntry(&entry)
}

// runCandidate runs the main loop while in the candidate state.
func (r *Raft) runCandidate() {
	term := r.getCurrentTerm() + 1
	r.logger.Info("entering candidate state", "node", r, "term", term)
	metrics.IncrCounter([]string{"raft", "state", "candidate"}, 1)

	// Start vote for us, and set a timeout
	var voteCh <-chan *voteResult
	var prevoteCh <-chan *preVoteResult

	// check if pre-vote is active and that this is not a leader transfer.
	// Leader transfer do not perform prevote by design
	if !r.preVoteDisabled && !r.candidateFromLeadershipTransfer.Load() {
		prevoteCh = r.preElectSelf()
	} else {
		voteCh = r.electSelf()
	}

	// Make sure the leadership transfer flag is reset after each run. Having this
	// flag will set the field LeadershipTransfer in a RequestVoteRequest to true,
	// which will make other servers vote even though they have a leader already.
	// It is important to reset that flag, because this privilege could be abused
	// otherwise.
	defer func() { r.candidateFromLeadershipTransfer.Store(false) }()

	electionTimeout := r.config().ElectionTimeout
	electionTimer := randomTimeout(electionTimeout)

	// Tally the votes, need a simple majority
	preVoteGrantedVotes := 0
	preVoteRefusedVotes := 0
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	r.logger.Debug("calculated votes needed", "needed", votesNeeded, "term", term)

	for r.getState() == Candidate {
		r.mainThreadSaturation.sleeping()

		select {
		case rpc := <-r.rpcCh:
			r.mainThreadSaturation.working()
			r.processRPC(rpc)
		case preVote := <-prevoteCh:
			// This a pre-vote case it should trigger a "real" election if the pre-vote is won.
			r.mainThreadSaturation.working()
			r.logger.Debug("pre-vote received", "from", preVote.voterID, "term", preVote.Term, "tally", preVoteGrantedVotes)
			// Check if the term is greater than ours, bail
			if preVote.Term > term {
				r.logger.Debug("pre-vote denied: found newer term, falling back to follower", "term", preVote.Term)
				r.setState(Follower)
				r.setCurrentTerm(preVote.Term)
				return
			}

			// Check if the preVote is granted
			if preVote.Granted {
				preVoteGrantedVotes++
				r.logger.Debug("pre-vote granted", "from", preVote.voterID, "term", preVote.Term, "tally", preVoteGrantedVotes)
			} else {
				preVoteRefusedVotes++
				r.logger.Debug("pre-vote denied", "from", preVote.voterID, "term", preVote.Term, "tally", preVoteGrantedVotes)
			}

			// Check if we've won the pre-vote and proceed to election if so
			if preVoteGrantedVotes >= votesNeeded {
				r.logger.Info("pre-vote successful, starting election", "term", preVote.Term,
					"tally", preVoteGrantedVotes, "refused", preVoteRefusedVotes, "votesNeeded", votesNeeded)
				preVoteGrantedVotes = 0
				preVoteRefusedVotes = 0
				electionTimer = randomTimeout(electionTimeout)
				prevoteCh = nil
				voteCh = r.electSelf()
			}
			// Check if we've lost the pre-vote and wait for the election to timeout so we can do another time of
			// prevote.
			if preVoteRefusedVotes >= votesNeeded {
				r.logger.Info("pre-vote campaign failed, waiting for election timeout", "term", preVote.Term,
					"tally", preVoteGrantedVotes, "refused", preVoteRefusedVotes, "votesNeeded", votesNeeded)
			}
		case vote := <-voteCh:
			r.mainThreadSaturation.working()
			// Check if the term is greater than ours, bail
			if vote.Term > r.getCurrentTerm() {
				r.logger.Debug("newer term discovered, fallback to follower", "term", vote.Term)
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			// Check if the vote is granted
			if vote.Granted {
				grantedVotes++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.Term, "tally", grantedVotes)
			}

			// Check if we've become the leader
			if grantedVotes >= votesNeeded {
				r.logger.Info("election won", "term", vote.Term, "tally", grantedVotes)
				r.setState(Leader)
				r.setLeader(r.localAddr, r.localID)
				return
			}
		case c := <-r.configurationChangeCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			v.respond(ErrNotLeader)

		case ur := <-r.userRestoreCh:
			r.mainThreadSaturation.working()
			// Reject any restores since we are not the leader
			ur.respond(ErrNotLeader)

		case l := <-r.leadershipTransferCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			l.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			r.mainThreadSaturation.working()
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			r.mainThreadSaturation.working()
			b.respond(ErrCantBootstrap)

		case <-r.leaderNotifyCh:
			//  Ignore since we are not the leader

		case <-r.followerNotifyCh:
			if electionTimeout != r.config().ElectionTimeout {
				electionTimeout = r.config().ElectionTimeout
				electionTimer = randomTimeout(electionTimeout)
			}

		case <-electionTimer:
			r.mainThreadSaturation.working()
			// Election failed! Restart the election. We simply return,
			// which will kick us back into runCandidate
			r.logger.Warn("Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) setLeadershipTransferInProgress(v bool) {
	if v {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 1)
	} else {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 0)
	}
}

func (r *Raft) getLeadershipTransferInProgress() bool {
	v := atomic.LoadInt32(&r.leaderState.leadershipTransferInProgress)
	return v == 1
}

func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh,
		r.configurations.latest,
		r.getLastIndex()+1 /* first index that may be committed in this term */)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.notify = make(map[*verifyFuture]struct{})
	r.leaderState.stepDown = make(chan struct{}, 1)

	// 更新核心节点列表（如果coreNodesState不为nil）
	if r.coreNodesState != nil {
		r.coreNodesState.updateCoreNodes(r.configurations.latest)
	}
}

// runLeader runs the FSM for a leader. Do the setup here and drop into
// the leaderLoop for the hot loop.
func (r *Raft) runLeader() {
	r.logger.Info("entering leader state", "leader", r)

	// Notify that we are the leader
	overrideNotifyBool(r.leaderCh, true)

	// Push to the notify channel if given
	if notify := r.config().NotifyCh; notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
		}
	}

	// setup leader state
	r.setupLeaderState()

	// Cleanup state on step down
	defer func() {
		r.leaderLock.Lock()
		if r.leaderState.commitCh != nil {
			close(r.leaderState.commitCh)
			r.leaderState.commitCh = nil
		}
		r.leaderState.inflight = nil
		r.leaderState.replState = nil
		r.leaderState.notify = nil
		r.leaderState.stepDown = nil
		r.leaderLock.Unlock()
	}()

	// Start a replication routine for each peer
	r.startStopReplication()

	// Dispatch a no-op log entry first. This gets this leader up to the latest
	// possible commit index, even in the absence of client commands.
	noop := &logFuture{log: Log{Type: LogNoop}}
	r.dispatchLogs([]*logFuture{noop})

	// Sit in the leader loop until we step down
	r.leaderLoop()
}

// startStopReplication will set up state and start asynchronous replication to
// new peers, and stop replication to removed peers. Before removing a peer,
// it'll instruct the replication routines to try to replicate to the current
// index. This must only be called from the main thread.
func (r *Raft) startStopReplication() {
	inConfig := make(map[ServerID]bool, len(r.configurations.latest.Servers))
	lastIdx := r.getLastIndex()

	// Start replication goroutines that need starting
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}

		inConfig[server.ID] = true

		s, ok := r.leaderState.replState[server.ID]
		if !ok {
			r.logger.Info("added peer, starting replication", "peer", server.ID)
			s = &followerReplication{
				peer:                server,
				commitment:          r.leaderState.commitment,
				stopCh:              make(chan uint64, 1),
				triggerCh:           make(chan struct{}, 1),
				triggerDeferErrorCh: make(chan *deferError, 1),
				currentTerm:         r.getCurrentTerm(),
				nextIndex:           lastIdx + 1,
				matchIndex:          0, // 初始化为0，表示还没有复制任何日志
				lastContact:         time.Now(),
				notify:              make(map[*verifyFuture]struct{}),
				notifyCh:            make(chan struct{}, 1),
				stepDown:            r.leaderState.stepDown,
			}

			r.leaderState.replState[server.ID] = s
			r.goFunc(func() { r.replicate(s) })
			asyncNotifyCh(s.triggerCh)
			r.observe(PeerObservation{Peer: server, Removed: false})
		} else if ok {

			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()

			if peer.Address != server.Address {
				r.logger.Info("updating peer", "peer", server.ID)
				s.peerLock.Lock()
				s.peer = server
				s.peerLock.Unlock()
			}
		}
	}

	// Stop replication goroutines that need stopping
	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}
		// Replicate up to lastIdx and stop
		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIdx)
		repl.stopCh <- lastIdx
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
		r.observe(PeerObservation{Peer: repl.peer, Removed: true})
	}

	// Update peers metric
	metrics.SetGauge([]string{"raft", "peers"}, float32(len(r.configurations.latest.Servers)))
}

// configurationChangeChIfStable returns r.configurationChangeCh if it's safe
// to process requests from it, or nil otherwise. This must only be called
// from the main thread.
//
// Note that if the conditions here were to change outside of leaderLoop to take
// this from nil to non-nil, we would need leaderLoop to be kicked.
func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {
	// Have to wait until:
	// 1. The latest configuration is committed, and
	// 2. This leader has committed some entry (the noop) in this term
	//    https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

	// 如果当前正在等待核心节点响应，重置等待状态
	// 这是为了避免在配置变更时阻塞
	if r.coreNodesState != nil && r.coreNodesState.isWaitingForCoreNodes() {
		r.coreNodesState.resetWaitingState()
	}

	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
}

// leaderLoop is the hot loop for a leader. It is invoked
// after all the various leader setup is done.
func (r *Raft) leaderLoop() {
	// 用于跟踪是否需要下台
	lease := time.After(r.config().LeaderLeaseTimeout)

	for r.getState() == Leader {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case <-r.leaderState.stepDown:
			r.setState(Follower)

		case future := <-r.leadershipTransferCh:
			if r.getState() != Leader {
				future.respond(ErrNotLeader)
				continue
			}
			// 处理领导权转移请求
			r.handleLeadershipTransfer(future)

		case c := <-r.configurationChangeCh:
			// Exit the leader loop if we've been shutdown
			select {
			case <-r.shutdownCh:
				return
			default:
			}

			// Process the configuration change
			if err := r.processConfigurationChange(c); err != nil {
				if err == ErrEnqueueTimeout {
					c.respond(err)
				} else {
					r.logger.Error("failed to process configuration change", "error", err)
				}
			}

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case <-r.leaderState.commitCh:
			// Process the newly committed entries
			commitIndex := r.getCommitIndex()

			// Pull all inflight logs that are committed off the queue.
			r.leaderLock.Lock()
			for e := r.leaderState.inflight.Front(); e != nil; e = r.leaderState.inflight.Front() {
				commitLog := e.Value.(*logFuture)
				idx := commitLog.log.Index
				if idx > commitIndex {
					// Don't go past the committed index
					break
				}

				// Measure the commit time
				metrics.MeasureSince([]string{"raft", "commitTime"}, commitLog.dispatch)
				r.processLog(idx, commitLog)
				r.leaderState.inflight.Remove(e)
			}

			// 更新最后应用的索引
			r.updateLastApplied()
			r.leaderLock.Unlock()

		case v := <-r.verifyCh:
			if v.quorumSize == 0 {
				// Just dispatched, start the verification
				r.verifyLeader(v)
			} else if v.votes < v.quorumSize {
				// Early return
				v.respond(ErrNotLeader)
				r.logger.Warn("stepping down as leader since verification failed")
				r.setState(Follower)
				return
			} else {
				// Quorum of members agree, we are still leader
				v.respond(nil)
			}

		case p := <-r.userRestoreCh:
			p.respond(ErrLeadershipLost)

		case <-lease:
			// Check if we've exceeded the lease, potentially stepping down
			maxDiff := r.checkLeaderLease()

			// Next check interval should adjust for the last node we've
			// contacted, without going negative
			checkInterval := r.config().LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}

			// Renew the lease timer
			lease = time.After(checkInterval)

			// 调用leaderReplicate方法，通过协作者节点复制日志到非核心节点
			r.leaderReplicate()

		case <-r.shutdownCh:
			return
		}
	}
}

// handleLeadershipTransfer 处理领导权转移请求
func (r *Raft) handleLeadershipTransfer(future *leadershipTransferFuture) {
	// 如果不是领导者，直接返回错误
	if r.getState() != Leader {
		future.respond(ErrNotLeader)
		return
	}

	// 获取目标节点
	targetID := ServerID("")
	targetAddr := ServerAddress("")

	if future.ID != nil {
		targetID = *future.ID
	}

	if future.Address != nil {
		targetAddr = *future.Address
	}

	// 如果没有指定目标节点，选择一个合适的节点
	if targetID == "" {
		server := r.pickServer()
		if server != nil {
			targetID = server.ID
			targetAddr = server.Address
		} else {
			future.respond(fmt.Errorf("cannot find valid server to transfer leadership to"))
			return
		}
	}

	// 发送TimeoutNow请求给目标节点
	req := &TimeoutNowRequest{
		RPCHeader: r.getRPCHeader(),
	}
	var resp TimeoutNowResponse
	err := r.trans.TimeoutNow(targetID, targetAddr, req, &resp)
	if err != nil {
		r.logger.Error("failed to make TimeoutNow RPC", "target", targetID, "error", err)
		future.respond(err)
		return
	}

	future.respond(nil)
}

// leadershipTransfer 是一个内部方法，用于测试领导权转移
// 它直接向目标节点发送TimeoutNow请求，并通过doneCh返回结果
func (r *Raft) leadershipTransfer(id ServerID, addr ServerAddress, repl *followerReplication, stopCh chan struct{}, doneCh chan error) {
	// 检查是否应该立即停止
	select {
	case <-stopCh:
		doneCh <- nil
		return
	default:
	}

	// 发送TimeoutNow请求给目标节点
	req := &TimeoutNowRequest{
		RPCHeader: r.getRPCHeader(),
	}
	var resp TimeoutNowResponse
	err := r.trans.TimeoutNow(id, addr, req, &resp)
	if err != nil {
		r.logger.Error("failed to make TimeoutNow RPC", "target", id, "error", err)
		doneCh <- err
		return
	}

	doneCh <- nil
}

// processConfigurationChange 处理配置变更请求
func (r *Raft) processConfigurationChange(future *configurationChangeFuture) error {
	// 创建配置变更日志条目
	configuration, err := nextConfiguration(r.getLatestConfiguration(), r.getLastIndex(), future.req)
	if err != nil {
		return err
	}

	// 创建日志条目
	future.log = Log{
		Type: LogConfiguration,
		Data: EncodeConfiguration(configuration),
	}

	// 分发日志条目
	r.dispatchLogs([]*logFuture{&future.logFuture})

	// 等待日志条目被提交
	if err := future.Error(); err != nil {
		return err
	}

	// 响应配置变更请求
	future.respond(nil)
	return nil
}

// processLog 处理单个日志条目
func (r *Raft) processLog(index uint64, future *logFuture) {
	// 应用日志条目到状态机
	if future.log.Type == LogCommand {
		r.fsm.Apply(&future.log)
	}

	// 响应日志条目
	future.respond(nil)
}

// updateLastApplied 更新最后应用的索引
func (r *Raft) updateLastApplied() {
	// 获取当前提交索引
	commitIndex := r.getCommitIndex()

	// 更新最后应用的索引
	r.setLastApplied(commitIndex)
}

// verifyLeader 验证当前节点是否仍然是领导者
func (r *Raft) verifyLeader(v *verifyFuture) {
	// 如果不是领导者，直接返回错误
	if r.getState() != Leader {
		v.respond(ErrNotLeader)
		return
	}

	// 向所有节点发送心跳请求
	for _, server := range r.getLatestConfiguration().Servers {
		if server.ID == r.localID {
			continue
		}

		if server.Suffrage != Voter {
			continue
		}

		// 发送AppendEntries请求
		req := &AppendEntriesRequest{
			RPCHeader:         r.getRPCHeader(),
			Term:              r.getCurrentTerm(),
			Leader:            r.trans.EncodePeer(r.localID, r.localAddr),
			LeaderCommitIndex: r.getCommitIndex(),
		}
		var resp AppendEntriesResponse
		err := r.trans.AppendEntries(server.ID, server.Address, req, &resp)
		if err != nil {
			r.logger.Error("failed to make AppendEntries RPC", "target", server.ID, "error", err)
			continue
		}

		// 如果目标节点的任期更高，转换为跟随者
		if resp.Term > r.getCurrentTerm() {
			r.setState(Follower)
			r.setCurrentTerm(resp.Term)
			v.respond(ErrNotLeader)
			return
		}
	}

	// 如果没有节点返回更高的任期，则仍然是领导者
	v.respond(nil)
}

// processRPC is called to handle an incoming RPC request. This must only be
// called from the main thread.
func (r *Raft) processRPC(rpc RPC) {
	if err := r.checkRPCHeader(rpc); err != nil {
		rpc.Respond(nil, err)
		return
	}

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	case *RequestVoteRequest:
		r.requestVote(rpc, cmd)
	case *RequestPreVoteRequest:
		r.requestPreVote(rpc, cmd)
	case *InstallSnapshotRequest:
		r.installSnapshot(rpc, cmd)
	case *TimeoutNowRequest:
		r.timeoutNow(rpc, cmd)
	case *CollaboratorReplicateRequest:
		r.collaboratorReplicate(rpc, cmd)
	default:
		r.logger.Error("got unexpected command",
			"command", hclog.Fmt("%#v", rpc.Command))

		rpc.Respond(nil, fmt.Errorf(rpcUnexpectedCommandError))
	}
}

// processHeartbeat is a special handler used just for heartbeat requests
// so that they can be fast-pathed if a transport supports it. This must only
// be called from the main thread.
func (r *Raft) processHeartbeat(rpc RPC) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "processHeartbeat"}, time.Now())

	// Check if we are shutdown, just ignore the RPC
	select {
	case <-r.shutdownCh:
		return
	default:
	}

	// Ensure we are only handling a heartbeat
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Error("expected heartbeat, got", "command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// configuration if the entry results in a new configuration. This must only be
// called from the main thread, or from NewRaft() before any threads have begun.
func (r *Raft) processConfigurationLogEntry(entry *Log) error {
	switch entry.Type {
	case LogConfiguration:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)

	case LogAddPeerDeprecated, LogRemovePeerDeprecated:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		conf, err := decodePeers(entry.Data, r.trans)
		if err != nil {
			return err
		}
		r.setLatestConfiguration(conf, entry.Index)
	}
	return nil
}

// requestVote is invoked when we get a request vote RPC call.
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "requestVote"}, time.Now())
	r.observe(*req)

	// Setup a response
	resp := &RequestVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Granted:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Version 0 servers will panic unless the peers is present. It's only
	// used on them to produce a warning message.
	if r.protocolVersion < 2 {
		resp.Peers = encodePeers(r.configurations.latest, r.trans)
	}

	// Check if we have an existing leader [who's not the candidate] and also
	// check the LeadershipTransfer flag is set. Usually votes are rejected if
	// there is a known leader. But if the leader initiated a leadership transfer,
	// vote!
	var candidate ServerAddress
	var candidateBytes []byte
	if len(req.RPCHeader.Addr) > 0 {
		candidate = r.trans.DecodePeer(req.RPCHeader.Addr)
		candidateBytes = req.RPCHeader.Addr
	} else {
		candidate = r.trans.DecodePeer(req.Candidate)
		candidateBytes = req.Candidate
	}

	// For older raft version ID is not part of the packed message
	// We assume that the peer is part of the configuration and skip this check
	if len(req.ID) > 0 {
		candidateID := ServerID(req.ID)
		// if the Servers list is empty that mean the cluster is very likely trying to bootstrap,
		// Grant the vote
		if len(r.configurations.latest.Servers) > 0 && !inConfiguration(r.configurations.latest, candidateID) {
			r.logger.Warn("rejecting vote request since node is not in configuration",
				"from", candidate)
			return
		}
	}
	if leaderAddr, leaderID := r.LeaderWithID(); leaderAddr != "" && leaderAddr != candidate && !req.LeadershipTransfer {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from", candidate,
			"leader", leaderAddr,
			"leader-id", string(leaderID))
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		// Ensure transition to follower
		r.logger.Debug("lost leadership because received a requestVote with a newer term")
		r.setState(Follower)
		r.setCurrentTerm(req.Term)

		resp.Term = req.Term
	}

	// if we get a request for vote from a nonVoter  and the request term is higher,
	// step down and update term, but reject the vote request
	// This could happen when a node, previously voter, is converted to non-voter
	// The reason we need to step in is to permit to the cluster to make progress in such a scenario
	// More details about that in https://github.com/hashicorp/raft/pull/526
	if len(req.ID) > 0 {
		candidateID := ServerID(req.ID)
		if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) {
			r.logger.Warn("rejecting vote request since node is not a voter", "from", candidate)
			return
		}
	}
	// Check if we have voted yet
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote term", "error", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote candidate", "error", err)
		return
	}

	// Check if we've voted in this election before
	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		r.logger.Info("duplicate requestVote for same term", "term", req.Term)
		if bytes.Equal(lastVoteCandBytes, candidateBytes) {
			r.logger.Warn("duplicate requestVote from", "candidate", candidate)
			resp.Granted = true
		}
		return
	}

	// Reject if their term is older
	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIdx,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	// Persist a vote for safety
	if err := r.persistVote(req.Term, candidateBytes); err != nil {
		r.logger.Error("failed to persist vote", "error", err)
		return
	}

	resp.Granted = true
	r.setLastContact()
}

// requestPreVote is invoked when we get a request Pre-Vote RPC call.
func (r *Raft) requestPreVote(rpc RPC, req *RequestPreVoteRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "requestVote"}, time.Now())
	r.observe(*req)

	// Setup a response
	resp := &RequestPreVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Granted:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Check if we have an existing leader [who's not the candidate] and also
	candidate := r.trans.DecodePeer(req.GetRPCHeader().Addr)
	candidateID := ServerID(req.ID)

	// if the Servers list is empty that mean the cluster is very likely trying to bootstrap,
	// Grant the vote
	if len(r.configurations.latest.Servers) > 0 && !inConfiguration(r.configurations.latest, candidateID) {
		r.logger.Warn("rejecting pre-vote request since node is not in configuration",
			"from", candidate)
		return
	}

	if leaderAddr, leaderID := r.LeaderWithID(); leaderAddr != "" && leaderAddr != candidate {
		r.logger.Warn("rejecting pre-vote request since we have a leader",
			"from", candidate,
			"leader", leaderAddr,
			"leader-id", string(leaderID))
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	if req.Term > r.getCurrentTerm() {
		// continue processing here to possibly grant the pre-vote as in a "real" vote this will transition us to follower
		r.logger.Debug("received a requestPreVote with a newer term, grant the pre-vote")
		resp.Term = req.Term
	}

	// if we get a request for a pre-vote from a nonVoter  and the request term is higher, do not grant the Pre-Vote
	// This could happen when a node, previously voter, is converted to non-voter
	if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) {
		r.logger.Warn("rejecting pre-vote request since node is not a voter", "from", candidate)
		return
	}

	// Reject if their term is older
	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting pre-vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Warn("rejecting pre-vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIdx,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	resp.Granted = true
}

// installSnapshot is invoked when we get a InstallSnapshot RPC call.
// We must be in the follower state for this, since it means we are
// too far behind a leader for log replay. This must only be called
// from the main thread.
func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "installSnapshot"}, time.Now())
	// Setup a response
	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		_, _ = io.Copy(io.Discard, rpc.Reader) // ensure we always consume all the snapshot data from the stream [see issue #212]
		rpc.Respond(resp, rpcErr)
	}()

	// Sanity check the version
	if req.SnapshotVersion < SnapshotVersionMin ||
		req.SnapshotVersion > SnapshotVersionMax {
		rpcErr = fmt.Errorf("unsupported snapshot version %d", req.SnapshotVersion)
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring installSnapshot request with older term than current term",
			"request-term", req.Term,
			"current-term", r.getCurrentTerm())
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		// Ensure transition to follower
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// Save the current leader
	if len(req.ID) > 0 {
		r.setLeader(r.trans.DecodePeer(req.RPCHeader.Addr), ServerID(req.ID))
	} else {
		r.setLeader(r.trans.DecodePeer(req.Leader), ServerID(req.ID))
	}

	// Create a new snapshot
	var reqConfiguration Configuration
	var reqConfigurationIndex uint64
	if req.SnapshotVersion > 0 {
		reqConfiguration = DecodeConfiguration(req.Configuration)
		reqConfigurationIndex = req.ConfigurationIndex
	} else {
		reqConfiguration, rpcErr = decodePeers(req.Peers, r.trans)
		if rpcErr != nil {
			r.logger.Error("failed to install snapshot", "error", rpcErr)
			return
		}
		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, req.LastLogIndex, req.LastLogTerm,
		reqConfiguration, reqConfigurationIndex, r.trans)
	if err != nil {
		r.logger.Error("failed to create snapshot to install", "error", err)
		rpcErr = fmt.Errorf("failed to create snapshot: %v", err)
		return
	}

	// Separately track the progress of streaming a snapshot over the network
	// because this too can take a long time.
	countingRPCReader := newCountingReader(rpc.Reader)

	// Spill the remote snapshot to disk
	transferMonitor := startSnapshotRestoreMonitor(r.logger, countingRPCReader, req.Size, true)
	n, err := io.Copy(sink, countingRPCReader)
	transferMonitor.StopAndWait()
	if err != nil {
		sink.Cancel()
		r.logger.Error("failed to copy snapshot", "error", err)
		rpcErr = err
		return
	}

	// Check that we received it all
	if n != req.Size {
		sink.Cancel()
		r.logger.Error("failed to receive whole snapshot",
			"received", hclog.Fmt("%d / %d", n, req.Size))
		rpcErr = fmt.Errorf("short read")
		return
	}

	// Finalize the snapshot
	if err := sink.Close(); err != nil {
		r.logger.Error("failed to finalize snapshot", "error", err)
		rpcErr = err
		return
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// Restore snapshot
	future := &restoreFuture{ID: sink.ID()}
	future.ShutdownCh = r.shutdownCh
	future.init()
	select {
	case r.fsmMutateCh <- future:
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	// Wait for the restore to happen
	if err := future.Error(); err != nil {
		r.logger.Error("failed to restore snapshot", "error", err)
		rpcErr = err
		return
	}

	// Update the lastApplied so we don't replay old logs
	r.setLastApplied(req.LastLogIndex)

	// Update the last stable snapshot info
	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	// Restore the peer set
	r.setLatestConfiguration(reqConfiguration, reqConfigurationIndex)
	r.setCommittedConfiguration(reqConfiguration, reqConfigurationIndex)

	// Clear old logs if r.logs is a MonotonicLogStore. Otherwise compact the
	// logs. In both cases, log any errors and continue.
	if mlogs, ok := r.logs.(MonotonicLogStore); ok && mlogs.IsMonotonic() {
		if err := r.removeOldLogs(); err != nil {
			r.logger.Error("failed to reset logs", "error", err)
		}
	} else if err := r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Error("failed to compact logs", "error", err)
	}

	r.logger.Info("Installed remote snapshot")
	resp.Success = true
	r.setLastContact()
}

// setLastContact is used to set the last contact time to now
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID
}

type preVoteResult struct {
	RequestPreVoteResponse
	voterID ServerID
}

// electSelf is used to send a RequestVote RPC to all peers, and vote for
// ourself. This has the side affecting of incrementing the current term. The
// response channel returned is used to wait for all the responses (including a
// vote for ourself). This must only be called from the main thread.
func (r *Raft) electSelf() <-chan *voteResult {
	// Create a response channel
	respCh := make(chan *voteResult, len(r.configurations.latest.Servers))

	// Increment the term
	newTerm := r.getCurrentTerm() + 1

	r.setCurrentTerm(newTerm)
	// Construct the request
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestVoteRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      newTerm,
		// this is needed for retro compatibility, before RPCHeader.Addr was added
		Candidate:          r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       lastIdx,
		LastLogTerm:        lastTerm,
		LeadershipTransfer: r.candidateFromLeadershipTransfer.Load(),
	}

	// Construct a function to ask for a vote
	askPeer := func(peer Server) {
		r.goFunc(func() {
			defer metrics.MeasureSince([]string{"raft", "candidate", "electSelf"}, time.Now())
			resp := &voteResult{voterID: peer.ID}
			err := r.trans.RequestVote(peer.ID, peer.Address, req, &resp.RequestVoteResponse)
			if err != nil {
				r.logger.Error("failed to make requestVote RPC",
					"target", peer,
					"error", err,
					"term", req.Term)
				resp.Term = req.Term
				resp.Granted = false
			}
			respCh <- resp
		})
	}

	// For each peer, request a vote
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				r.logger.Debug("voting for self", "term", req.Term, "id", r.localID)

				// Persist a vote for ourselves
				if err := r.persistVote(req.Term, req.RPCHeader.Addr); err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return nil

				}
				// Include our own vote
				respCh <- &voteResult{
					RequestVoteResponse: RequestVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      req.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				r.logger.Debug("asking for vote", "term", req.Term, "from", server.ID, "address", server.Address)
				askPeer(server)
			}
		}
	}

	return respCh
}

// preElectSelf is used to send a RequestPreVote RPC to all peers, and vote for
// ourself. This will not increment the current term. The
// response channel returned is used to wait for all the responses (including a
// vote for ourself).
// This must only be called from the main thread.
func (r *Raft) preElectSelf() <-chan *preVoteResult {

	// At this point transport should support pre-vote
	// but check just in case
	prevoteTrans, prevoteTransSupported := r.trans.(WithPreVote)
	if !prevoteTransSupported {
		panic("preElection is not possible if the transport don't support pre-vote")
	}

	// Create a response channel
	respCh := make(chan *preVoteResult, len(r.configurations.latest.Servers))

	// Propose the next term without actually changing our state
	newTerm := r.getCurrentTerm() + 1

	// Construct the request
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestPreVoteRequest{
		RPCHeader:    r.getRPCHeader(),
		Term:         newTerm,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Construct a function to ask for a vote
	askPeer := func(peer Server) {
		r.goFunc(func() {
			defer metrics.MeasureSince([]string{"raft", "candidate", "preElectSelf"}, time.Now())
			resp := &preVoteResult{voterID: peer.ID}

			err := prevoteTrans.RequestPreVote(peer.ID, peer.Address, req, &resp.RequestPreVoteResponse)

			// If the target server do not support Pre-vote RPC we count this as a granted vote to allow
			// the cluster to progress.
			if err != nil && strings.Contains(err.Error(), rpcUnexpectedCommandError) {
				r.logger.Error("target does not support pre-vote RPC, treating as granted",
					"target", peer,
					"error", err,
					"term", req.Term)
				resp.Term = req.Term
				resp.Granted = true
			} else if err != nil {
				r.logger.Error("failed to make requestVote RPC",
					"target", peer,
					"error", err,
					"term", req.Term)
				resp.Term = req.Term
				resp.Granted = false
			}
			respCh <- resp

		})
	}

	// For each peer, request a vote
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				r.logger.Debug("pre-voting for self", "term", req.Term, "id", r.localID)

				// cast a pre-vote for our self
				respCh <- &preVoteResult{
					RequestPreVoteResponse: RequestPreVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      req.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				r.logger.Debug("asking for pre-vote", "term", req.Term, "from", server.ID, "address", server.Address)
				askPeer(server)
			}
		}
	}

	return respCh
}

// persistVote is used to persist our vote for safety.
func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

// setCurrentTerm is used to set the current term in a durable manner.
func (r *Raft) setCurrentTerm(t uint64) {
	// Persist to disk first
	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		panic(fmt.Errorf("failed to save current term: %v", err))
	}
	r.raftState.setCurrentTerm(t)
}

// setState is used to update the current state. Any state
// transition causes the known leader to be cleared. This means
// that leader should be set only after updating the state.
func (r *Raft) setState(state RaftState) {
	r.setLeader("", "")
	oldState := r.raftState.getState()
	r.raftState.setState(state)
	if oldState != state {
		r.observe(state)
	}
}

// pickServer returns the follower that is most up to date and participating in quorum.
// Because it accesses leaderstate, it should only be called from the leaderloop.
func (r *Raft) pickServer() *Server {
	var pick *Server
	var current uint64
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID || server.Suffrage != Voter {
			continue
		}
		state, ok := r.leaderState.replState[server.ID]
		if !ok {
			continue
		}
		nextIdx := atomic.LoadUint64(&state.nextIndex)
		if nextIdx > current {
			current = nextIdx
			tmp := server
			pick = &tmp
		}
	}
	return pick
}

// initiateLeadershipTransfer starts the leadership on the leader side, by
// sending a message to the leadershipTransferCh, to make sure it runs in the
// mainloop.
func (r *Raft) initiateLeadershipTransfer(id *ServerID, address *ServerAddress) LeadershipTransferFuture {
	future := &leadershipTransferFuture{ID: id, Address: address}
	future.init()

	if id != nil && *id == r.localID {
		err := fmt.Errorf("cannot transfer leadership to itself")
		r.logger.Info(err.Error())
		future.respond(err)
		return future
	}

	select {
	case r.leadershipTransferCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	default:
		return errorFuture{ErrEnqueueTimeout}
	}
}

// timeoutNow is what happens when a server receives a TimeoutNowRequest.
func (r *Raft) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	r.setLeader("", "")
	r.setState(Candidate)
	r.candidateFromLeadershipTransfer.Store(true)
	rpc.Respond(&TimeoutNowResponse{}, nil)
}

// setLatestConfiguration stores the latest configuration and updates a copy of it.
func (r *Raft) setLatestConfiguration(c Configuration, i uint64) {
	r.configurations.latest = c
	r.configurations.latestIndex = i
	r.latestConfiguration.Store(c.Clone())
}

// setCommittedConfiguration stores the committed configuration.
func (r *Raft) setCommittedConfiguration(c Configuration, i uint64) {
	r.configurations.committed = c
	r.configurations.committedIndex = i
}

// getLatestConfiguration reads the configuration from a copy of the main
// configuration, which means it can be accessed independently from the main
// loop.
func (r *Raft) getLatestConfiguration() Configuration {
	// this switch catches the case where this is called without having set
	// a configuration previously.
	switch c := r.latestConfiguration.Load().(type) {
	case Configuration:
		return c
	default:
		return Configuration{}
	}
}

// isCoreNode 判断给定的节点ID是否是核心节点
func (r *Raft) isCoreNode(id ServerID) bool {
	return r.coreNodesState.isCoreNode(id)
}

// updateCoreNodeResponse 更新核心节点的响应状态
func (r *Raft) updateCoreNodeResponse(id ServerID, success bool) bool {
	return r.coreNodesState.updateCoreNodeResponse(id, success)
}

// dispatchLogs 将日志条目添加到日志存储中，并开始复制
func (r *Raft) dispatchLogs(applyLogs []*logFuture) {
	now := time.Now()
	defer metrics.MeasureSince([]string{"raft", "leader", "dispatchLog"}, now)

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()

	n := len(applyLogs)
	logs := make([]*Log, n)

	for idx, applyLog := range applyLogs {
		applyLog.dispatch = now
		lastIndex++
		applyLog.log.Index = lastIndex
		applyLog.log.Term = term
		applyLog.log.AppendedAt = now
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)
	}

	// 将日志条目写入本地存储
	if err := r.logs.StoreLogs(logs); err != nil {
		r.logger.Error("failed to commit logs", "error", err)
		for _, applyLog := range applyLogs {
			applyLog.respond(err)
		}
		r.setState(Follower)
		return
	}

	// 更新最后的日志索引和任期
	r.setLastLog(lastIndex, term)

	// 通知复制器有新的日志
	for _, f := range r.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}
}

// checkLeaderLease 检查是否能够在领导者租约时间内联系到多数节点
// 如果不能，需要下台。返回最大的联系时间差
func (r *Raft) checkLeaderLease() time.Duration {
	// 已联系的节点数，自己总是可以联系到
	contacted := 1

	// 获取租约超时时间
	leaseTimeout := r.config().LeaderLeaseTimeout

	// 检查每个跟随者
	var maxDiff time.Duration
	now := time.Now()
	for _, server := range r.getLatestConfiguration().Servers {
		if server.ID == r.localID {
			continue
		}

		if server.Suffrage != Voter {
			continue
		}

		f := r.leaderState.replState[server.ID]
		if f == nil {
			continue
		}

		diff := now.Sub(f.LastContact())
		if diff <= leaseTimeout {
			contacted++
			if diff > maxDiff {
				maxDiff = diff
			}
		} else {
			r.logger.Warn("failed to contact", "server-id", server.ID, "time", diff)
		}
	}

	// 验证是否能联系到多数节点
	quorum := r.quorumSize()
	if contacted < quorum {
		r.logger.Warn("failed to contact quorum of nodes, stepping down")
		r.setState(Follower)
	}
	return maxDiff
}

// quorumSize 返回多数节点的数量
func (r *Raft) quorumSize() int {
	voters := 0
	for _, server := range r.getLatestConfiguration().Servers {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}
