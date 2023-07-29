package main

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	pb "github.com/KDF5000/flease/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport interface {
	Read(addr string, k uint64) (ReadResult, bool)
	Write(addr string, k uint64, v LeaseValue) bool
}

type LeaseManager struct {
	node  uint64
	peers []string
	trans map[string]pb.LeaseClient
	mu    sync.Mutex

	// own lease
	lease Lease
	// lease expire time, sec
	expiredDuration uint32
	// time diff, ns
	e uint32

	exit chan struct{}
}

func NewLeaseManager(expired, e uint32, node uint64, peers string) (*LeaseManager, error) {
	return &LeaseManager{
		node:            node,
		peers:           strings.Split(peers, ","),
		trans:           make(map[string]pb.LeaseClient),
		expiredDuration: expired,
		e:               e,
		exit:            make(chan struct{}),
	}, nil
}

func (m *LeaseManager) Start() {
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			val, ok := m.GetLease()
			if ok && val.Node == m.node {
				logF(m.node, "become or keep leader, val: %+v", val)
			}
		case <-m.exit:
			logF(m.node, "recieve exit sigal")
			timer.Stop()
			return
		}
		timer.Reset(time.Duration(m.expiredDuration/2) * time.Second)
	}
}

func (m *LeaseManager) Stop() {
	close(m.exit)
}

func (m *LeaseManager) Read(k uint64) (*ReadResult, bool) {
	return m.lease.Read(k)
}

func (m *LeaseManager) Write(k uint64, v LeaseValue) bool {
	return m.lease.Write(k, v)
}

func (m *LeaseManager) nextK() uint64 {
	return uint64(time.Now().UnixNano())
}

func (m *LeaseManager) read(k uint64) (*ReadResult, bool) {
	commitCh := make(chan *ReadResult, len(m.peers))
	abortCh := make(chan struct{}, len(m.peers))
	respCh := make(chan struct{}, len(m.peers))
	for _, peer := range m.peers {
		go func(p string) {
			m.mu.Lock()
			trans, ok := m.trans[p]
			if !ok {
				conn, err := grpc.Dial(p, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					abortCh <- struct{}{}
					return
				}
				trans = pb.NewLeaseClient(conn)
				m.trans[p] = trans
			}
			m.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			logF(m.node, "begin to send read request to %s...", p)
			resp, err := trans.Read(ctx, &pb.ReadRequest{K: k})
			defer cancel()
			logF(m.node, "recv read response from %s, err: %v, resp: %+v", p, err, resp)
			if err == nil {
				if resp.Status == pb.Status_COMMIT {
					val := ReadResult{
						K:         k,
						LastWrite: resp.LastWrite,
						Value: LeaseValue{
							Node:      resp.Lease.Node,
							LeaseTime: resp.Lease.LeaseTime,
						},
					}
					commitCh <- &val
				} else {
					abortCh <- struct{}{}
				}
			} else {
				logF(m.node, "failed to send write request to %s", p)
			}

			respCh <- struct{}{}
		}(peer)
	}

	var responses []*ReadResult
	var totalResp int
	for {
		// timeout := time.After(time.Minute)
		select {
		case val := <-commitCh:
			responses = append(responses, val)
			logF(m.node, "receive %d commit read response, expect majority: %d",
				len(responses), int(math.Ceil(float64(len(m.peers)+1)/2)))
			if len(responses) >= int(math.Ceil(float64(len(m.peers)+1)/2)) {
				var maxReadResult *ReadResult
				var maxK uint64
				for _, resp := range responses {
					if resp.LastWrite > maxK {
						maxK = resp.LastWrite
						maxReadResult = resp
					}
				}

				return maxReadResult, true
			}
		case <-abortCh:
			logF(m.node, "recieve abort read")
			return nil, false
		case <-respCh:
			totalResp++
			if totalResp >= len(m.peers) {
				logF(m.node, "receive all read rpc response")
				return nil, false
			}
		}
	}
}

func (m *LeaseManager) write(k uint64, val *LeaseValue) bool {
	commitCh := make(chan struct{}, len(m.peers))
	abortCh := make(chan struct{}, len(m.peers))
	respCh := make(chan struct{}, len(m.peers))
	for _, peer := range m.peers {
		go func(p string) {
			m.mu.Lock()
			trans, ok := m.trans[p]
			if !ok {
				conn, err := grpc.Dial(p, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					abortCh <- struct{}{}
					return
				}
				trans = pb.NewLeaseClient(conn)
				m.trans[p] = trans
			}
			m.mu.Unlock()

			req := pb.WriteRequest{K: k, Lease: &pb.LeaseValue{
				Node:      val.Node,
				LeaseTime: val.LeaseTime,
			}}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			logF(m.node, "begin to send write request to %s...", p)
			resp, err := trans.Write(ctx, &req)
			defer cancel()
			logF(m.node, "recv write response from %s,err: %v, resp: %+v", p, err, resp)
			if err == nil {
				if resp.Status == pb.Status_ABORT {
					abortCh <- struct{}{}
				} else {
					commitCh <- struct{}{}
				}
			}

			respCh <- struct{}{}
		}(peer)
	}

	var commitCnt int
	var totalResp int
	timeout := time.After(time.Minute)
	for {
		select {
		case <-commitCh:
			commitCnt++
			logF(m.node, "write response: %d, majority: %d", commitCnt, int(math.Ceil(float64(len(m.peers)+1)/2)))
			if commitCnt >= int(math.Ceil(float64(len(m.peers)+1)/2)) {
				return true
			}
		case <-abortCh:
			logF(m.node, "receive abort read")
			return false
		case <-timeout:
			logF(m.node, "write timeout")
			return false
		case <-respCh:
			totalResp++
			if totalResp >= len(m.peers) {
				logF(m.node, "receive all write rpc response")
				return false
			}
		}
	}
}

func (m *LeaseManager) GetLease() (*LeaseValue, bool) {
	return m.getLease(m.nextK())
}

func (m *LeaseManager) getLease(k uint64) (*LeaseValue, bool) {
	res, commit := m.read(k)
	if !commit {
		return nil, false
	}

	now := time.Now().UnixNano()
	if res != nil && res.Value.LeaseTime < now && res.Value.LeaseTime+int64(m.e) > now {
		time.Sleep(time.Duration(m.e) * time.Nanosecond)
		return m.getLease(k)
	}

	var lease LeaseValue
	if res == nil || (res.Value.Node == 0 && res.Value.LeaseTime == 0) ||
		res.Value.LeaseTime+int64(m.e) < now || res.Value.Node == m.node {
		lease.Node = m.node
		lease.LeaseTime = now + int64(m.expiredDuration)*int64(time.Second)
		logF(m.node, "refresh or create lease, %+v", lease)
	} else {
		lease = res.Value
		logF(m.node, "keep old lease, %+v", lease)
	}

	logF(m.node, "try to write lease, %+v", lease)
	if m.write(k, &lease) {
		return &lease, true
	}

	return nil, false
}
