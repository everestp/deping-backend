package services

import (
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/everestp/depin-backend/db/repositories"
)

type ActiveNode struct {
	Pubkey    string
	Email     string
	Latitude  float64
	Longitude float64
	LastSeen  time.Time
}

type MemoryRegistry struct {
	mu    sync.RWMutex
	nodes map[string]ActiveNode
}

type SmartScheduler struct {
	registry       *MemoryRegistry
	mu             sync.Mutex
	domainNodeIdx  map[string]int // Key: root domain -> index into onlineNodes slice (round-robin cursor)
}

func NewMemoryRegistry() *MemoryRegistry {
	r := &MemoryRegistry{
		nodes: make(map[string]ActiveNode),
	}
	go r.startEvictionLoop(30 * time.Second)
	return r
}

func NewSmartScheduler(reg *MemoryRegistry) *SmartScheduler {
	return &SmartScheduler{
		registry:      reg,
		domainNodeIdx: make(map[string]int),
	}
}

func (r *MemoryRegistry) TrackHeartbeat(pubkey, email string, lat, lon float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[pubkey] = ActiveNode{
		Pubkey:    pubkey,
		Email:     email,
		Latitude:  lat,
		Longitude: lon,
		LastSeen:  time.Now(),
	}
}

func (r *MemoryRegistry) GetOnlineNodes() []ActiveNode {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var active []ActiveNode
	now := time.Now()
	for _, node := range r.nodes {
		if now.Sub(node.LastSeen) <= 5*time.Minute {
			active = append(active, node)
		}
	}
	return active
}

func (r *MemoryRegistry) startEvictionLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		r.mu.Lock()
		now := time.Now()
		for pubkey, node := range r.nodes {
			if now.Sub(node.LastSeen) > 5*time.Minute {
				delete(r.nodes, pubkey)
			}
		}
		r.mu.Unlock()
	}
}

func cleanDomain(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return strings.ToLower(rawURL)
	}
	return strings.ToLower(parsed.Hostname())
}

// MatchBatch assigns each monitor to an online node using per-domain round-robin.
//
// Key fixes vs old version:
//  1. A node can receive MULTIPLE monitors per tick (removed the "busy" exclusion).
//     The old code had `if _, busy := assignments[node.Pubkey]; busy { continue }`
//     which meant only 1 monitor could ever be assigned per node per tick — every
//     monitor beyond the first was silently dropped.
//  2. Round-robin uses a persistent cursor index, not last-pubkey string comparison.
//     The old `if node.Pubkey != last` check permanently skipped the only node once
//     it had been used once, causing the scheduler to appear dead after ~3-4 minutes.
//  3. domainCounts cap (max 5 per domain per tick) is preserved.
func (s *SmartScheduler) MatchBatch(monitors []*repositories.Monitor) map[string]*repositories.Monitor {
	s.mu.Lock()
	defer s.mu.Unlock()

	assignments := make(map[string]*repositories.Monitor)
	onlineNodes := s.registry.GetOnlineNodes()

	if len(onlineNodes) == 0 {
		return assignments
	}

	n := len(onlineNodes)
	domainCounts := make(map[string]int)

	for _, m := range monitors {
		domain := cleanDomain(m.TargetURL)

		// Cap: at most 5 jobs for the same domain per scheduler tick
		if domainCounts[domain] >= 5 {
			continue
		}

		// Round-robin: advance the cursor for this domain and pick the next node.
		// Works correctly with 1 node (idx stays 0 → same node every time, which
		// is correct when there's only one choice).
		idx := s.domainNodeIdx[domain] % n
		picked := onlineNodes[idx]

		// Advance cursor for next call — wrap via modulo on next entry
		s.domainNodeIdx[domain] = (idx + 1) % n

		assignments[picked.Pubkey] = m
		domainCounts[domain]++
	}

	return assignments
}