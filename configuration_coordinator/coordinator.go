package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"os"
	"bytes"
)

// ========== SLA Definitions ==========

type ConsistencyLevel int

const (
	Eventual ConsistencyLevel = iota
	MonotonicReads
	ReadMyWrites
	Bounded
	Strong
)

type LatencyBound struct {
	Duration time.Duration
}

type SubSLA struct {
	Consistency    ConsistencyLevel
	Latency        LatencyBound
	StalenessBound *time.Duration
	Utility        float64
}

type SLA struct {
	ID      string
	SubSLAs []SubSLA
}

// ========== Report Structures ==========

type UtilityDropReport struct {
	ClientID    string                      `json:"client_id"`
	Region      string                      `json:"region"`
	AvgUtility  float64                     `json:"utility"`
	SLA         SLA                         `json:"sla"`
	ReadHistogram   map[string]int          `json:"histogram"`
	RTTs            map[string]float64      `json:"rtts"`
}

type HistogramEntry struct {
	SubSLA  SubSLA        `json:"sub_sla"`
	Status  string        `json:"status"`
}

type HistogramSummary struct {
	Node    string           `json:"node"`
	Summary []HistogramEntry `json:"summary"`
}

type Shard struct {
	ShardId    int `json:"id"`
	RangeStart int `json:"start"`
	RangeEnd   int `json:"end"`
	Primary	string `json:"primary"` 
	SecondaryIDs []string `json:"secondaryIDs"`
	Secondaries []string
	DefaultRepFreq float64 `json:"defaultRepFreq"`
	ReplicationFreqs map[string]float64
}

type StorageNode struct {
	Id string `json:"nodeId"`
	Address   string `json:"nodeAddress"`
}

type ReplicationConfig struct {
	Nodes  []StorageNode `json:"nodes"`
	Shards []Shard `json:"shards"`
}

// ========== Coordinator State ==========

var (
	mu sync.Mutex

	// Prevent too-frequent action from repeated drops
	lastUtilityDropTime = make(map[string]time.Time)
	lastReplicationUpdate = make(map[string]time.Time)		// last time we adjusted replication for a storage node
	reportCooldown      = 2 * time.Second

	utilityThreshold = 0.4

	GlobalConfig *ReplicationConfig
)

func main() {
	LoadReplicationConfig("../single_shard_config.json")

	http.HandleFunc("/report", reportHandler)

	fmt.Println("Coordinator agent running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func reportHandler(w http.ResponseWriter, r *http.Request) {
	var report UtilityDropReport
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	now := time.Now()
	key := fmt.Sprintf("%s-%s", report.ClientID, report.Region)

	mu.Lock()
	defer mu.Unlock()

	lastTime, seen := lastUtilityDropTime[key]
	if seen && now.Sub(lastTime) < reportCooldown {
		fmt.Printf("[SKIPPED] Recent utility drop already handled for %s in %s\n", report.ClientID, report.Region)
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	lastUtilityDropTime[key] = now

	fmt.Printf("[ALERT] Utility drop detected: client=%s region=%s utility=%.3f\n", report.ClientID, report.Region, report.AvgUtility)

	// Kick off analysis in the background
	go handleReportAnalysis(report)

	w.WriteHeader(http.StatusOK)
}

// This is right now written for a single shard, thought multi-shard support is very similar (shardID should be passed)
func handleReportAnalysis(report UtilityDropReport) {

	var maxKey string
	var maxCount int
	for key, count := range report.ReadHistogram {
		if count > maxCount {
			maxKey = key
			maxCount = count
		}
	}

	if maxKey == "" {
		fmt.Println("[INFO] No histogram pattern found in report")
		return
	}

	// Parse the histogram key
	var summary HistogramSummary
	err := json.Unmarshal([]byte(maxKey), &summary)
	if err != nil {
		fmt.Printf("[WARN] Failed to parse histogram JSON key: %v\n", err)
		return
	}

	fmt.Println(report.RTTs) 
	fmt.Println(summary) 

	// Analyze the summary for problematic subSLAs

	// Check if replication freq can be changed
	for _, entry := range summary.Summary {
		if entry.Status == "Lat_Not_Met" {
			cons := entry.SubSLA.Consistency
			if cons == 1 || cons == 2 || cons == 3 {
				if isPrimaryForShard(summary.Node) {
					fmt.Printf("[RECONFIG CANDIDATE] Node %s failing SLA with Consistency=%d Latency=%v\n",
						report.ClientID, cons, entry.SubSLA.Latency.Duration)
					
					// Find the closest secondary
					var closest string
					minRTT := 1e9 
					for node, rtt := range report.RTTs {
						// skip primary
						if node == summary.Node {
							continue
						}
						if rtt < minRTT {
							minRTT = rtt
							closest = node
						}
					}

					fmt.Println("closest node is %s", closest)
					fmt.Println("Rep Frequency in it is %d", GlobalConfig.Shards[0].ReplicationFreqs[closest])
					
					// Contact the secondary node for setting rep frequency
					currentFreq := GlobalConfig.Shards[0].ReplicationFreqs[closest]
					if closest != "" {
						lastUpdate, seen := lastReplicationUpdate[closest]

						// Don't send many close adjusting requests to a server
						if seen && time.Since(lastUpdate) < time.Duration(currentFreq * 1.5 * float64(time.Second)) {
							fmt.Println("[SKIPPED] Replication update to %s skipped due to cooldown", closest)
							return
						}

						if currentFreq <= 5 {
							fmt.Println("Freq is already too low")
							// TODO: here should consider tracking these cases and if needed move primary
							return
						}

						
						newFreq := currentFreq * 0.5

						fmt.Printf("[RECONFIG] Requesting %s to increase replication freq from %.2f to %.2f\n", closest, currentFreq, newFreq)

						reqBody := map[string]float64{"new_freq": newFreq, "shardID": 0}
						jsonData, _ := json.Marshal(reqBody)

						resp, err := http.Post(fmt.Sprintf("http://%s/adjust_replication", closest), "application/json", bytes.NewBuffer(jsonData))
						if err != nil {
							fmt.Printf("[ERROR] Failed to contact %s for replication update: %v\n", closest, err)
						} else {
							defer resp.Body.Close()
							if resp.StatusCode == http.StatusOK {
								fmt.Println("[SUCCESS] Replication frequency update acknowledged by secondary")
								GlobalConfig.Shards[0].ReplicationFreqs[closest] = newFreq
								lastReplicationUpdate[closest] = time.Now()
							} else {
								fmt.Printf("[WARN] Replication update failed with status: %d\n", resp.StatusCode)
							}
						}
					}

					break	
				}
			}
		}
	}
}

func LoadReplicationConfig(path string) error { 
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var config ReplicationConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	// From the secondary IDs assign the secondary endpoints
	nodeIDToAddress := make(map[string]string)
	for _, node := range config.Nodes {
		nodeIDToAddress[node.Id] = node.Address
	}

	// Populate the Secondaries field for each shard using the SecondaryIDs
	for i := range config.Shards {
		secondaryAddrs := []string{}
		replicationFreqs := make(map[string]float64)

		for _, secID := range config.Shards[i].SecondaryIDs {
			addr, ok := nodeIDToAddress[secID]
			if !ok {
				return fmt.Errorf("could not find address for secondary ID %s", secID)
			}
			secondaryAddrs = append(secondaryAddrs, addr)
			replicationFreqs[addr] = config.Shards[i].DefaultRepFreq
		}

		config.Shards[i].Secondaries = secondaryAddrs
		config.Shards[i].ReplicationFreqs = replicationFreqs
	}

	GlobalConfig = &config

	fmt.Println("Loaded the config and it is:")
	fmt.Println(GlobalConfig)

	return nil 
}

// Assumption: Single Shard for curr implementation now
func isPrimaryForShard(node string) bool {
	return (node == GlobalConfig.Shards[0].Primary)
}