// http包提供了访问分布式键值存储的HTTP服务器。
// 它还提供了其他节点加入现有集群的端点。
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/hashicorp/etcdraft/store"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// Store是Raft支持的键值存储必须实现的接口。
type Store interface {
	// Get返回给定键的值。
	Get(key string) (string, error)

	// Set设置给定键的值，通过分布式共识。
	Set(key, value string) error

	// Delete删除给定的键，通过分布式共识。
	Delete(key string) error

	// Join将节点加入到集群中。
	Join(nodeID string, addr string) error

	// Range实现etcd v3的Range API
	Range(req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Put实现etcd v3的Put API
	Put(req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)

	// DeleteRange实现etcd v3的DeleteRange API
	DeleteRange(req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)
}

// Service提供HTTP和gRPC服务。
type Service struct {
	httpAddr string
	grpcAddr string

	httpLn net.Listener
	grpcLn net.Listener

	store Store

	// gRPC服务器
	grpcServer *grpc.Server
	// HTTP服务器
	httpServer *http.Server
}

// New返回一个未初始化的服务。
func New(addr string, store Store) *Service {
	// 使用相同的地址，但gRPC端口加1
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatalf("无效的地址格式: %s", err)
	}

	grpcPort := fmt.Sprintf("%d", 1+atoi(port))
	grpcAddr := net.JoinHostPort(host, grpcPort)

	return &Service{
		httpAddr: addr,
		grpcAddr: grpcAddr,
		store:    store,
	}
}

// atoi将字符串转换为整数，忽略错误
func atoi(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// Start启动服务。
func (s *Service) Start() error {
	// 启动HTTP服务
	httpLn, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		return fmt.Errorf("HTTP监听失败: %s", err)
	}
	s.httpLn = httpLn

	// 创建HTTP服务器
	mux := http.NewServeMux()
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/key/", s.handleKeyRequest)
	mux.HandleFunc("/v3/kv/range", s.handleRange)
	mux.HandleFunc("/v3/kv/put", s.handlePut)
	mux.HandleFunc("/v3/kv/deleterange", s.handleDeleteRange)

	s.httpServer = &http.Server{
		Handler: mux,
	}

	// 启动HTTP服务器
	go func() {
		if err := s.httpServer.Serve(httpLn); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	// 启动gRPC服务
	grpcLn, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("gRPC监听失败: %s", err)
	}
	s.grpcLn = grpcLn
	// 增加 gRPC 服务器的最大帧大小
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(100 * 1024 * 1024), // 100MB
		grpc.MaxSendMsgSize(100 * 1024 * 1024), // 100MB
	}
	s.grpcServer = grpc.NewServer(grpcOpts...)
	etcdserverpb.RegisterKVServer(s.grpcServer, &kvServer{store: s.store})

	go func() {
		if err := s.grpcServer.Serve(grpcLn); err != nil {
			log.Fatalf("gRPC serve: %s", err)
		}
	}()

	log.Printf("HTTP服务启动在 %s", s.httpAddr)
	log.Printf("gRPC服务启动在 %s", s.grpcAddr)

	return nil
}

// Close关闭服务。
func (s *Service) Close() {
	if s.httpServer != nil {
		s.httpServer.Shutdown(context.Background())
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpLn != nil {
		s.httpLn.Close()
	}
	if s.grpcLn != nil {
		s.grpcLn.Close()
	}
}

// ServeHTTP允许Service处理HTTP请求。
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else if r.URL.Path == "/v3/kv/range" {
		s.handleRange(w, r)
	} else if r.URL.Path == "/v3/kv/put" {
		s.handlePut(w, r)
	} else if r.URL.Path == "/v3/kv/deleterange" {
		s.handleDeleteRange(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	nodeID, ok := m["id"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(nodeID, remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	switch r.Method {
	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		v, err := s.store.Get(k)
		if err != nil {
			if err == store.ErrKeyNotFound {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}

		b, err := json.Marshal(map[string]string{k: v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, string(b))

	case "POST":
		// 从POST正文读取值。
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.store.Set(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	case "DELETE":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.store.Delete(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

// handleRange处理etcd v3的Range API请求
func (s *Service) handleRange(w http.ResponseWriter, r *http.Request) {
	var req etcdserverpb.RangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	resp, err := s.store.Range(&req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

// handlePut处理etcd v3的Put API请求
func (s *Service) handlePut(w http.ResponseWriter, r *http.Request) {
	var req etcdserverpb.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	resp, err := s.store.Put(&req)
	if err != nil {
		if err == store.ErrNotLeader {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

// handleDeleteRange处理etcd v3的DeleteRange API请求
func (s *Service) handleDeleteRange(w http.ResponseWriter, r *http.Request) {
	var req etcdserverpb.DeleteRangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	resp, err := s.store.DeleteRange(&req)
	if err != nil {
		if err == store.ErrNotLeader {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

// Addr返回HTTP服务监听的地址
func (s *Service) Addr() net.Addr {
	return s.httpLn.Addr()
}

// GRPCAddr返回gRPC服务监听的地址
func (s *Service) GRPCAddr() net.Addr {
	return s.grpcLn.Addr()
}

// kvServer实现etcd v3的KV gRPC服务
type kvServer struct {
	etcdserverpb.UnimplementedKVServer
	store Store
}

// Range实现etcd v3的Range API
func (s *kvServer) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return s.store.Range(req)
}

// Put实现etcd v3的Put API
func (s *kvServer) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return s.store.Put(req)
}

// DeleteRange实现etcd v3的DeleteRange API
func (s *kvServer) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return s.store.DeleteRange(req)
}
