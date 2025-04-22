package util

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"client/consistency"
)

// Structs for importing raw SLA's from config files
type rawSubSLA struct {
	Rank           int     `json:"rank"`
	Consistency    string  `json:"consistecy"`
	LatencyBound   int     `json:"latency_bound"`
	StalenessBound *int    `json:"staleness_bound,omitempty"`
	Utility        float64 `json:"utility"`
}

type rawSLAFile struct {
	SubSLAs []rawSubSLA `json:"subSLAs"`
}

type Shard struct {
	RangeStart int `json:"start"`
	RangeEnd   int `json:"end"`
	Primary	string `json:"primary"` 
}

type StorageNode struct {
	Id string `json:"nodeId"`
	Address   string `json:"nodeAddress"`
}

type ReplicationConfig struct {
	Nodes  []StorageNode `json:"nodes"`
	Shards []Shard `json:"shards"`
}

// Session should hold state for read-my-writes and monotonic consistency levels
// TODO: Add session-specific state for monotonic reads, etc.
type Session struct {
	DefaultSLA consistency.SLA
	ObjectsWritten map[string]int64
	ObjectsRead map[string]int64
}

func LoadSLAFromFile(path string, id string) (consistency.SLA, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return consistency.SLA{}, fmt.Errorf("error reading SLA file: %w", err)
	}

	var raw rawSLAFile
	if err := json.Unmarshal(data, &raw); err != nil {
		return consistency.SLA{}, fmt.Errorf("error unmarshaling SLA file: %w", err)
	}

	var subSLAs []consistency.SubSLA
	for _, r := range raw.SubSLAs {
		level, err := parseConsistency(r.Consistency)
		if err != nil {
			return consistency.SLA{}, err
		}
		sub := consistency.SubSLA{
			Consistency: level,
			Latency:     consistency.LatencyBound{Duration: time.Duration(r.LatencyBound) * time.Millisecond},
			Utility:     r.Utility,
		}
		if level == consistency.Bounded && r.StalenessBound != nil {
			staleness := time.Duration(*r.StalenessBound) * time.Millisecond
			sub.StalenessBound = &staleness
		}
		subSLAs = append(subSLAs, sub)
	}

	// Sort by rank
	sort.SliceStable(subSLAs, func(i, j int) bool {
		return raw.SubSLAs[i].Rank < raw.SubSLAs[j].Rank
	})

	return consistency.SLA{
		ID:      id,
		SubSLAs: subSLAs,
	}, nil
}

func parseConsistency(s string) (consistency.ConsistencyLevel, error) {
	switch strings.ToLower(s) {
	case "eventual":
		return consistency.Eventual, nil
	case "monotonicreads":
		return consistency.MonotonicReads, nil
	case "readmywrites":
		return consistency.ReadMyWrites, nil
	case "bounded":
		return consistency.Bounded, nil
	case "strong":
		return consistency.Strong, nil
	default:
		return 0, fmt.Errorf("unknown consistency level: %s", s)
	}
}
