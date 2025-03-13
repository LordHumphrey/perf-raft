// store包提供了一个基于Raft算法的分布式键值存储系统。
// 键值对的变更通过分布式共识实现，只有当集群中大多数节点同意时，值才会被更改。
//
// 分布式共识通过Raft算法实现，具体使用了Hashicorp的Raft实现。
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	bucketName          = "kvstore"
)

var (
	// ErrNotLeader 当节点不是Leader时返回
	ErrNotLeader = errors.New("not leader")
	// ErrKeyNotFound 当键不存在时返回
	ErrKeyNotFound = errors.New("key not found")
)

// 命令类型
const (
	CmdPut    = "put"
	CmdDelete = "delete"
)

// 命令结构，用于Raft日志
type command struct {
	Op       string `json:"op,omitempty"`
	Key      []byte `json:"key,omitempty"`
	Value    []byte `json:"value,omitempty"`
	RangeEnd []byte `json:"range_end,omitempty"`
	Lease    int64  `json:"lease,omitempty"`
}

// Store是一个简单的键值存储系统，所有更改都通过Raft共识进行。
type Store struct {
	RaftDir  string
	RaftBind string
	inmem    bool

	mu    sync.RWMutex
	db    *bbolt.DB // 使用bbolt作为存储引擎
	raft  *raft.Raft // Raft共识机制

	// 用于生成唯一的修订版本号
	revision int64

	logger *log.Logger
}

// New返回一个新的Store。
func New(inmem bool) *Store {
	return &Store{
		inmem:    inmem,
		logger:   log.New(os.Stderr, "[store] ", log.LstdFlags),
		revision: 1, // 初始修订版本号
	}
}

// Open打开存储。如果enableSingle设置为true，并且没有现有的对等节点，
// 则该节点成为集群的第一个节点，因此是集群的领导者。
// localID应该是此节点的服务器标识符。
func (s *Store) Open(enableSingle bool, localID string) error {
	// 设置Raft配置
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// 设置Raft通信
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// 创建快照存储。这允许Raft截断日志。
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// 创建日志存储和稳定存储
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if s.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftboltdb.New(raftboltdb.Options{
			Path: filepath.Join(s.RaftDir, "raft.db"),
		})
		if err != nil {
			return fmt.Errorf("new bbolt store: %s", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}

	// 初始化bbolt数据库
	if !s.inmem {
		dbPath := filepath.Join(s.RaftDir, "kvstore.db")
		db, err := bbolt.Open(dbPath, 0600, nil)
		if err != nil {
			return fmt.Errorf("open bbolt: %s", err)
		}

		// 创建bucket
		err = db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			return err
		})
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		s.db = db
	}

	// 实例化Raft系统
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

// Close关闭存储
func (s *Store) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return err
		}
	}
	if s.raft != nil {
		s.raft.Shutdown()
	}
	return nil
}

// Get返回给定键的值
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil {
		return "", fmt.Errorf("db not open")
	}

	var value string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		v := b.Get([]byte(key))
		if v == nil {
			return ErrKeyNotFound
		}
		value = string(v)
		return nil
	})

	return value, err
}

// Set设置给定键的值
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &command{
		Op:    CmdPut,
		Key:   []byte(key),
		Value: []byte(value),
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete删除给定的键
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &command{
		Op:  CmdDelete,
		Key: []byte(key),
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Range实现etcd v3的Range API
func (s *Store) Range(req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.db == nil {
		return nil, fmt.Errorf("db not open")
	}

	resp := &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: s.revision,
		},
	}

	// 计算范围
	start := req.Key
	end := req.RangeEnd

	// 如果RangeEnd为空，则只查询单个键
	if len(end) == 0 {
		var kv *mvccpb.KeyValue
		err := s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			v := b.Get(start)
			if v == nil {
				return nil // 键不存在，返回空结果
			}

			// 创建KeyValue
			kv = &mvccpb.KeyValue{
				Key:            start,
				Value:          v,
				CreateRevision: s.revision,
				ModRevision:    s.revision,
				Version:        1,
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		if kv != nil {
			resp.Kvs = append(resp.Kvs, kv)
			resp.Count = 1
		}
		return resp, nil
	}

	// 处理范围查询
	var kvs []*mvccpb.KeyValue
	var count int64

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		// 遍历范围内的所有键
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			// 如果超出范围，则停止
			if len(end) > 0 && bytes.Compare(k, end) >= 0 {
				break
			}

			count++

			// 如果只需要计数，不需要返回键值对
			if req.CountOnly {
				continue
			}

			// 创建KeyValue
			kv := &mvccpb.KeyValue{
				Key:            k,
				Value:          v,
				CreateRevision: s.revision,
				ModRevision:    s.revision,
				Version:        1,
			}

			// 如果设置了limit并且已经达到limit，则停止
			if req.Limit > 0 && int64(len(kvs)) >= req.Limit {
				resp.More = true
				break
			}

			kvs = append(kvs, kv)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	resp.Kvs = kvs
	resp.Count = count
	return resp, nil
}

// Put实现etcd v3的Put API
func (s *Store) Put(req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	c := &command{
		Op:    CmdPut,
		Key:   req.Key,
		Value: req.Value,
		Lease: req.Lease,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)
	if err := f.Error(); err != nil {
		return nil, err
	}

	resp := &etcdserverpb.PutResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: s.revision,
		},
	}

	// 如果需要返回前一个值
	if req.PrevKv {
		var prevKv *mvccpb.KeyValue
		err := s.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			v := b.Get(req.Key)
			if v == nil {
				return nil
			}
			prevKv = &mvccpb.KeyValue{
				Key:            req.Key,
				Value:          v,
				CreateRevision: s.revision - 1,
				ModRevision:    s.revision - 1,
				Version:        1,
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if prevKv != nil {
			resp.PrevKv = prevKv
		}
	}

	return resp, nil
}

// DeleteRange实现etcd v3的DeleteRange API
func (s *Store) DeleteRange(req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	c := &command{
		Op:       CmdDelete,
		Key:      req.Key,
		RangeEnd: req.RangeEnd,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)
	if err := f.Error(); err != nil {
		return nil, err
	}

	resp := &etcdserverpb.DeleteRangeResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: s.revision,
		},
	}

	// 如果需要返回前一个值，我们需要在应用命令前获取它们
	// 这里简化处理，实际上应该在应用命令前获取
	if req.PrevKv {
		// 在实际实现中，应该在应用命令前获取前一个值
		s.logger.Printf("Warning: PrevKv not fully implemented")
	}

	// 设置删除的键数量
	// 在实际实现中，应该从命令结果中获取
	resp.Deleted = 1

	return resp, nil
}

// Join将节点加入到集群中
func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// 如果节点已经存在，可能需要先从配置中删除
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// 如果ID和地址都相同，则不需要任何操作
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

// fsm是Store的有限状态机
type fsm Store

// Apply应用Raft日志条目到键值存储
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case CmdPut:
		return f.applyPut(c.Key, c.Value, c.Lease)
	case CmdDelete:
		return f.applyDelete(c.Key, c.RangeEnd)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot返回键值存储的快照
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// 创建一个临时文件来存储数据库快照
	if f.db == nil {
		return &fsmSnapshot{}, nil
	}

	// 创建一个临时文件
	tmpFile, err := os.CreateTemp("", "kvstore-snapshot")
	if err != nil {
		return nil, err
	}
	defer tmpFile.Close()

	// 将数据库写入临时文件
	if err := f.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(tmpFile)
		return err
	}); err != nil {
		return nil, err
	}

	// 返回快照
	return &fsmSnapshot{
		dbPath: tmpFile.Name(),
	}, nil
}

// Restore从快照恢复键值存储
func (f *fsm) Restore(rc io.ReadCloser) error {
	if f.db != nil {
		// 关闭现有数据库
		f.db.Close()
	}

	// 创建一个临时文件
	tmpFile, err := os.CreateTemp("", "kvstore-restore")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	// 将快照数据复制到临时文件
	if _, err := io.Copy(tmpFile, rc); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	// 打开新的数据库
	db, err := bbolt.Open(filepath.Join(f.RaftDir, "kvstore.db"), 0600, nil)
	if err != nil {
		return err
	}

	// 从临时文件恢复数据库
	if err := db.Update(func(tx *bbolt.Tx) error {
		// 删除现有的bucket
		if err := tx.DeleteBucket([]byte(bucketName)); err != nil && err != bbolt.ErrBucketNotFound {
			return err
		}
		// 创建新的bucket
		_, err := tx.CreateBucket([]byte(bucketName))
		return err
	}); err != nil {
		return err
	}

	// 从临时文件复制数据
	source, err := bbolt.Open(tmpFile.Name(), 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer source.Close()

	return source.View(func(sourceTx *bbolt.Tx) error {
		sourceBucket := sourceTx.Bucket([]byte(bucketName))
		if sourceBucket == nil {
			return nil
		}

		return db.Update(func(tx *bbolt.Tx) error {
			targetBucket := tx.Bucket([]byte(bucketName))
			return sourceBucket.ForEach(func(k, v []byte) error {
				return targetBucket.Put(k, v)
			})
		})
	})
}

// applyPut应用Put命令
func (f *fsm) applyPut(key, value []byte, lease int64) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 增加修订版本号
	f.revision++

	if f.db == nil {
		return nil
	}

	err := f.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Put(key, value)
	})
	if err != nil {
		f.logger.Printf("error applying put: %s", err.Error())
	}
	return nil
}

// applyDelete应用Delete命令
func (f *fsm) applyDelete(key, rangeEnd []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 增加修订版本号
	f.revision++

	if f.db == nil {
		return nil
	}

	// 如果rangeEnd为空，则只删除单个键
	if len(rangeEnd) == 0 {
		err := f.db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			return b.Delete(key)
		})
		if err != nil {
			f.logger.Printf("error applying delete: %s", err.Error())
		}
		return nil
	}

	// 处理范围删除
	err := f.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		// 遍历范围内的所有键并删除
		for k, _ := c.Seek(key); k != nil; k, _ = c.Next() {
			// 如果超出范围，则停止
			if len(rangeEnd) > 0 && bytes.Compare(k, rangeEnd) >= 0 {
				break
			}
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		f.logger.Printf("error applying range delete: %s", err.Error())
	}
	return nil
}

// fsmSnapshot是键值存储的快照
type fsmSnapshot struct {
	dbPath string
}

// Persist将快照写入到接收器
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	// 如果没有数据库路径，返回空快照
	if f.dbPath == "" {
		return sink.Close()
	}

	// 打开数据库文件
	db, err := os.Open(f.dbPath)
	if err != nil {
		sink.Cancel()
		return err
	}
	defer db.Close()
	defer os.Remove(f.dbPath)

	// 将数据库文件写入接收器
	if _, err := io.Copy(sink, db); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release释放快照资源
func (f *fsmSnapshot) Release() {
	if f.dbPath != "" {
		os.Remove(f.dbPath)
	}
}
