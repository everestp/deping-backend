package services

import (
	"math"
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
	registry      *MemoryRegistry
	mu            sync.Mutex
	lastUsedNodes map[string]string // Key: Root Domain -> Value: Last assigned Node Pubkey
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
		lastUsedNodes: make(map[string]string),
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

func calculateDistanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0

	radLat1 := lat1 * math.Pi / 180.0
	radLat2 := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(radLat1)*math.Cos(radLat2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}

func cleanDomain(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return strings.ToLower(rawURL)
	}
	return strings.ToLower(parsed.Hostname())
}

func (s *SmartScheduler) MatchBatch(monitors []*repositories.Monitor) map[string]*repositories.Monitor {
	s.mu.Lock()
	defer s.mu.Unlock()

	assignments := make(map[string]*repositories.Monitor)
	domainCounts := make(map[string]int)
	onlineNodes := s.registry.GetOnlineNodes()

	if len(onlineNodes) == 0 {
		return assignments
	}

	for _, m := range monitors {
		domain := cleanDomain(m.TargetURL)

		if domainCounts[domain] >= 5 {
			continue
		}

		var selectedNode *ActiveNode
		lastAssignedPubkey := s.lastUsedNodes[domain]

		var geographicCluster []ActiveNode
		for _, node := range onlineNodes {
			if _, busy := assignments[node.Pubkey]; busy {
				continue
			}

			if len(geographicCluster) > 0 {
				dist := calculateDistanceKm(geographicCluster[0].Latitude, geographicCluster[0].Longitude, node.Latitude, node.Longitude)
				if dist <= 50.0 {
					geographicCluster = append(geographicCluster, node)
				}
			} else {
				geographicCluster = append(geographicCluster, node)
			}
		}

		if len(geographicCluster) > 0 {
			for _, n := range geographicCluster {
				if n.Pubkey != lastAssignedPubkey {
					target := n
					selectedNode = &target
					break
				}
			}
			if selectedNode == nil {
				selectedNode = &geographicCluster[0]
			}
		}

		if selectedNode != nil {
			assignments[selectedNode.Pubkey] = m
			domainCounts[domain]++
			s.lastUsedNodes[domain] = selectedNode.Pubkey
		}
	}

	return assignments
}
