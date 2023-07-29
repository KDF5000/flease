package main

import "sync"

type LeaseValue struct {
	Node      uint64
	LeaseTime int64
}

type ReadResult struct {
	K         uint64
	LastWrite uint64
	Value     LeaseValue
}

type Lease struct {
	LastRead  uint64
	LastWrite uint64
	CurrentV  LeaseValue

	mu sync.RWMutex
}

func (l *Lease) Read(k uint64) (*ReadResult, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.LastWrite >= k || l.LastRead >= k {
		return nil, false
	}

	l.LastRead = k
	return &ReadResult{
		K:         k,
		LastWrite: l.LastWrite,
		Value:     l.CurrentV,
	}, true
}

func (l *Lease) Write(k uint64, v LeaseValue) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.LastWrite > k || l.LastRead > k {
		return false
	}

	l.LastWrite = k
	l.CurrentV = v
	return true
}
