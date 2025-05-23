package api

import (
	"client/consistency"
	"client/monitor"
	"client/util"
	"client/optimizer"
	"fmt"
	"net/http"
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"time"
	"math/rand"
	"sync"
)


var GlobalConfig *util.ReplicationConfig

// =====================
// HTTP Client
// =====================

var httpClient = &http.Client{
    Transport: &http.Transport{
        MaxIdleConns:        100,
        MaxIdleConnsPerHost: 100,
        IdleConnTimeout:     90 * time.Second,
    },
}

// =====================
// Core API Methods
// =====================

// TODO: The session monitoring functions should be implemented
// Default: Each session starts with a default SLA, but the Get reqs in the session could specify their SLA's also 

func BeginSession(sla *consistency.SLA, serverSelectionPolicy util.ServerSelectionPolicy) *util.Session {
	return &util.Session{
		DefaultSLA: sla,
		ServerSelectionPolicy: serverSelectionPolicy,
		ObjectsWritten:	make(map[string]int64),
		ObjectsRead:	make(map[string]int64),
		Utilities:		[]float64{},
	}
}

func EndSession(s *util.Session) {
	// Print Monitoring data 
	fmt.Println("Monitor Utilities are: ")
	fmt.Println(monitor.GetUtilities())

	// 1. Compute average utility
	var total float64
	for _, u := range s.Utilities {
		total += u
	}
	avgUtility := 0.0
	if len(s.Utilities) > 0 {
		avgUtility = total / float64(len(s.Utilities))
	}

	// 2. Count objects accessed
	numReads := len(s.ObjectsRead)
	numWrites := len(s.ObjectsWritten)

	fmt.Printf("Session ended. Avg Utility: %.4f | Reads: %d | Writes: %d\n", avgUtility, numReads, numWrites)

	// 4. Clean up maps/slices to reclaim memory
	s.ObjectsRead = nil
	s.ObjectsWritten = nil
	s.Utilities = nil
}

// ========== Helper Data Structures ==========

type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

var artificialLags = make(map[string]time.Duration)
var lagMu sync.RWMutex

var coldStartRTTCounter int = 0

// ========== GET/PUT Endpoints ==========

// This will update session metadata on write timestamps
func Put(s *util.Session, key string, value string) error {
    shardID := determineShardForKey(key)

	rec := Record{
		Key:   key,
		Value: value,
	}

	recordJson, _ := json.Marshal(rec)
	url := fmt.Sprintf("http://%s/set", GlobalConfig.Shards[shardID].Primary)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(recordJson))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	rtt := time.Since(start)

	// Adjust RTT is there is a lag associated wih Primary
	rtt += getArtificialLag(GlobalConfig.Shards[shardID].Primary)

	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("An error happened invoking the put endpoint of the storage node\n")
		fmt.Printf("%v \n", err)
		return fmt.Errorf("HTTP error: %v", err)
	}

	defer resp.Body.Close()

	// TODO: instead of a map, why not a single timestamp
	var result struct {
		SetTimestamp int64 `json:"put_timestamp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("Failed to decode response: %v", err)
		return fmt.Errorf("Failed to decode response: %v", err)
	}

	// If no error, then update RTT window in monitor
	coldStartRTTCounter++
	if (coldStartRTTCounter > 5) {
		monitor.RecordRTT(GlobalConfig.Shards[shardID].Primary, rtt)
	}
	

	// Update write timestamp of the session
	// fmt.Printf("Set succeeded. Updating session write timestamp: %d\n", result.SetTimestamp)
	s.ObjectsWritten[key] = result.SetTimestamp          

    return nil
}

// Return:Value of the key requested + which subSLA was hit
// Server selection policy from the session is used to choose the destination server
func Get(s *util.Session, key string, sla *consistency.SLA) (string, consistency.SubSLA, error) {

	if (s.ServerSelectionPolicy == util.Pileus) {
		fmt.Println("Doing a Pileus Get:")
		return PileusGet(s, key, sla)
	} else if (s.ServerSelectionPolicy == util.Random) {
		fmt.Println("Doing a Random Get:")
		return randomGet(s, key, sla)
	} else if (s.ServerSelectionPolicy == util.Primary) {
		fmt.Println("Doing a Primary-Only Get:")
		return primaryOnlyGet(s, key, sla)
	} else if (s.ServerSelectionPolicy == util.Closest) {
		fmt.Println("Doing a Closest Get:")
		return closestGet(s, key, sla)
	} else {
		return PileusGet(s, key, sla)
	}
}

func PileusGet(s *util.Session, key string, sla *consistency.SLA) (string, consistency.SubSLA, error) {
	// Determine SLA for the op: use session default if not specified by input
	activeSLA := s.DefaultSLA
	if sla != nil {
		activeSLA = sla
	}

	// Find the storage node that maximizes the utility
	storageNode, targetSubSLA, minReadTSPerSubSLA := optimizer.FindNodeToRead(s, key, activeSLA)
	fmt.Printf("chosen storage node is %v and chosen subsla is %v\n", storageNode, targetSubSLA)
	fmt.Printf("minReadTSPerSubSLA for subslas is %v\n", minReadTSPerSubSLA)

	// Perform the read + calculate exact utility achieved
	val, obj_ts, node_hts, rtt, err := readFromNode(key, storageNode)

	// Calculate and track utility based on get_timestamp and rtt (consistency + latency)
	subAchieved, detailedSubStatus := detectSubSLAHit(obj_ts, node_hts, rtt, targetSubSLA, activeSLA, minReadTSPerSubSLA)
	
	fmt.Println("Detailed Sub Status is when going to %s", storageNode)
	fmt.Println(detailedSubStatus)

	readStatus := monitor.ReadStatus{
		Node:          storageNode,
		SubSLADetails: detailedSubStatus,
	}
	monitor.RecordReadStatus(readStatus)

	// If no sub-sla is achieved
	if subAchieved == nil {
		fmt.Println("No utility could be computed, because gained subSLA was null")
		s.Utilities = append(s.Utilities, 0.0)
		monitor.RecordUtility(0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("no utility could be computed")
	}

	// Update session read utilities
	s.Utilities = append(s.Utilities, subAchieved.Utility)
	monitor.RecordUtility(subAchieved.Utility)

	// Update the read timestamp of the object read
	fmt.Printf("Updating session read timestamp: %d\n", obj_ts)
	s.ObjectsRead[key] = obj_ts

	return val, *subAchieved, err
}

// =====================
// Get Functions for Eval
// =====================

// This is for evaluation purposes
func primaryOnlyGet(s *util.Session, key string, sla *consistency.SLA) (string, consistency.SubSLA, error) {
	activeSLA := s.DefaultSLA
	if sla != nil {
		activeSLA = sla
	}

	if (activeSLA == nil) {
		shardID := determineShardForKey(key)
		// I want the highTS and onjTS back as well
		val, _, _, _, err := readFromNode(key, GlobalConfig.Shards[shardID].Primary)
		return val, consistency.SubSLA{}, err
	}

	shardID := determineShardForKey(key)
	val, _, _, rtt, err := readFromNode(key, GlobalConfig.Shards[shardID].Primary)

	if (err != nil) {
		// Some error happened for the key
		fmt.Println("primary-only read failed with error")
		fmt.Println(err)
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	// If we always go to primary, consistency is always met, the RTT is the only thing to check for the utility checking
	for _, sub := range activeSLA.SubSLAs {
		if rtt <= sub.Latency.Duration {
			subAchieved := &sub // make a copy
			s.Utilities = append(s.Utilities, subAchieved.Utility)
			return val, *subAchieved, err
		}
	}

	// If we have not returned yet, then no sub-SLA is met
	fmt.Println("No utility could be computed for primary-only read")
	s.Utilities = append(s.Utilities, 0.0)
	return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
}

// TODO: utility calculation in these functions should be better generalized
func randomGet(s *util.Session, key string, sla *consistency.SLA) (string, consistency.SubSLA, error) {
	activeSLA := s.DefaultSLA
	if sla != nil {
		activeSLA = sla
	}

	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(GlobalConfig.Nodes))
	randomNode := GlobalConfig.Nodes[randomIndex]
	fmt.Println("Random Node is %s", randomNode.Id)

	shardID := determineShardForKey(key)
	primaryForKey := GlobalConfig.Shards[shardID].Primary

	val, _, node_hts, rtt, err := readFromNode(key, randomNode.Address)
	fmt.Println("RTT was %f", rtt)

	if (err != nil) {
		// Some error happened for the key
		fmt.Println("random read failed with error")
		fmt.Println(err)
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	if (activeSLA.ID == "cart_sla") {
		_, _, minReadTSPerSubSLA := optimizer.FindNodeToRead(s, key, activeSLA)
		fmt.Println(minReadTSPerSubSLA)

		// check node high ts
		for i, sub := range activeSLA.SubSLAs {
			if rtt <= sub.Latency.Duration {
				if (node_hts >= minReadTSPerSubSLA[i]) {
					subAchieved := &sub
					s.Utilities = append(s.Utilities, subAchieved.Utility)
					return val, *subAchieved, err
				}
			}
		}
		
		// If didn't return yet, no sub-SLA was met 
		fmt.Println("None of the utilities for password-checking is met, returning nil: \n")
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	if (activeSLA.ID == "psw_sla") {
		for _, sub := range activeSLA.SubSLAs {
			if (rtt <= sub.Latency.Duration && randomNode.Address == primaryForKey) {
				fmt.Println("Random Node happened to be Primary")
				subAchieved := &sub 
				s.Utilities = append(s.Utilities, subAchieved.Utility)
				return val, *subAchieved, err
			}
			if ( rtt <= sub.Latency.Duration && sub.Consistency == 0) {
				subAchieved := &sub 
				s.Utilities = append(s.Utilities, subAchieved.Utility)
				return val, *subAchieved, err
			} 
		}
	}

	// If we have not returned yet, then no sub-SLA is met
	fmt.Println("No utility could be computed for random read")
	s.Utilities = append(s.Utilities, 0.0)
	return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
}

func closestGet(s *util.Session, key string, sla *consistency.SLA) (string, consistency.SubSLA, error) {
	activeSLA := s.DefaultSLA
	if sla != nil {
		activeSLA = sla
	}

	// Finding the closest server based on the monitoring data
	closestNode, minRTT := monitor.GetLowestAvgRTTNode()
	fmt.Println("Closest Node is %s with minRTT %d", closestNode, minRTT)

	shardID := determineShardForKey(key)
	primaryForKey := GlobalConfig.Shards[shardID].Primary

	val, _, node_hts, rtt, err := readFromNode(key, closestNode)
	fmt.Println("RTT was %f", rtt)

	// TODO: here the retry mechanism should be done
	if (err != nil) {
		// Some error happened for the key
		fmt.Println("closest read failed with error")
		fmt.Println(err)
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	if (activeSLA.ID == "cart_sla") || (activeSLA.ID == "new_sla") {
		_, _, minReadTSPerSubSLA := optimizer.FindNodeToRead(s, key, activeSLA)
		fmt.Println(minReadTSPerSubSLA)

		// check node high ts
		for i, sub := range activeSLA.SubSLAs {
			if rtt <= sub.Latency.Duration {
				if (node_hts >= minReadTSPerSubSLA[i]) {
					subAchieved := &sub
					s.Utilities = append(s.Utilities, subAchieved.Utility)
					return val, *subAchieved, err
				}
			}
		}
		
		// If didn't return yet, no sub-SLA was met 
		fmt.Println("None of the utilities for password-checking is met, returning nil: \n")
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	if (activeSLA.ID == "psw_sla") {
		for _, sub := range activeSLA.SubSLAs {
			if (rtt <= sub.Latency.Duration && closestNode == primaryForKey) {
				fmt.Println("Closest Node happened to be Primary")
				subAchieved := &sub 
				s.Utilities = append(s.Utilities, subAchieved.Utility)
				return val, *subAchieved, err
			}
			if ( rtt <= sub.Latency.Duration && sub.Consistency == 0) {
				subAchieved := &sub 
				s.Utilities = append(s.Utilities, subAchieved.Utility)
				return val, *subAchieved, err
			} 
		}
	}
	
	// If we have not returned yet, then no sub-SLA is met
	fmt.Println("No utility could be computed for closest read")
	s.Utilities = append(s.Utilities, 0.0)
	return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
}

// =====================
// Helper Functions
// =====================

// Loads the sharding and replicaiton config
func LoadReplicationConfig(path string) error { 
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var config util.ReplicationConfig
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
		for _, secID := range config.Shards[i].SecondaryIDs {
			addr, ok := nodeIDToAddress[secID]
			if !ok {
				return fmt.Errorf("could not find address for secondary ID %s", secID)
			}
			secondaryAddrs = append(secondaryAddrs, addr)
		}
		config.Shards[i].Secondaries = secondaryAddrs
	}

	GlobalConfig = &config

	// Also udpate the optimizer with the same config
	optimizer.Init(GlobalConfig)

	fmt.Println("Loaded the config and it is:")
	fmt.Println(GlobalConfig)

	return nil 
}

func determineShardForKey(string_key string) int {
	key, err := strconv.Atoi(string_key)
	if err != nil {
		fmt.Printf("Error happened in getting the int value of the key")
		return -1
	}
   
    for i, shard := range GlobalConfig.Shards {
    
        if key >= shard.RangeStart && key <= shard.RangeEnd {
            fmt.Printf("Key %d belongs to shard #%d: %+v\n", key, i, shard)
            return i
        }
    }
    panic(fmt.Sprintf("No shard found for key: %d", key))
}

// Return Values: value, read_ts of the object, ConditionCode, utility , error (if any)
func readFromNode(key string, storageNode string) (string, int64, int64, time.Duration, error) {
	url := fmt.Sprintf("http://%s/get?key=%s", storageNode, key)

	var lastErr error
	var response struct {
		Key       string `json:"key"`
		Value     string `json:"value"`
		Timestamp int64  `json:"timestamp"`
		HighTS    int64  `json:"highTS"`
	}

	for attempt := 1; attempt <= 3; attempt++ {
		start := time.Now()
		resp, err := http.Get(url)
		rtt := time.Since(start)

		// Adjust RTT with the artificial lag
		rtt += getArtificialLag(storageNode)

		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			fmt.Printf("Attempt %d failed: error invoking GET on %s\n", attempt, storageNode)
			lastErr = fmt.Errorf("HTTP error (attempt %d): %v", attempt, err)
			time.Sleep(100 * time.Millisecond) // optional small delay between retries
			continue
		}

		defer resp.Body.Close()

		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			fmt.Printf("Attempt %d failed: error decoding response\n", attempt)
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// If successful, record metrics and return
		
		coldStartRTTCounter++
		if (coldStartRTTCounter > 5) {
			monitor.RecordRTT(storageNode, rtt)
		}
		monitor.RecordHTS(storageNode, response.HighTS)
		return response.Value, response.Timestamp, response.HighTS, rtt, nil
	}

	// All attempts failed
	return "", -1, -1, 0, lastErr
}

// TODO: This implementation is right now highly tuned for the SLA's we are testing. Generalize this implementation
// I think we have everything to make thi sfunction general!
func detectSubSLAHit(obj_ts int64, node_hts int64, rtt time.Duration, targetSubSLA consistency.SubSLA, activeSLA *consistency.SLA, minReadTSPerSubSLA []int64) (*consistency.SubSLA, []monitor.SubSLAStatus) {
	var statuses []monitor.SubSLAStatus

	if activeSLA.ID == "psw_sla" {
		for _, sub := range activeSLA.SubSLAs {
			status := monitor.SubSLAStatus{SubSLA: sub, Status: "NA"}

			if targetSubSLA.Consistency == 4 { // Strong
				if rtt <= sub.Latency.Duration {
					status.Status = "Met"
					statuses = append(statuses, status)
					subGained := sub
					return &subGained, statuses
				} else {
					status.Status = "Lat_Not_Met"
				}
			} else if targetSubSLA.Consistency == 0 { // Eventual
				if sub.Consistency == 0 {
					if rtt <= sub.Latency.Duration {
						status.Status = "Met"
						statuses = append(statuses, status)
						subGained := sub
						return &subGained, statuses
					} else {
						status.Status = "Lat_Not_Met"
					}
				}
			}
			statuses = append(statuses, status)
		}

		fmt.Println("None of the utilities for password-checking is met, returning nil")
		return nil, statuses
	}

	if activeSLA.ID == "cart_sla" || activeSLA.ID == "dynamic_cart_sla" {
		fmt.Println("detectSubSLAHit for cart_sla")
		fmt.Println(minReadTSPerSubSLA)
		fmt.Printf("node_hts is %d and rtt is %v\n", node_hts, rtt)

		for i, sub := range activeSLA.SubSLAs {
			status := monitor.SubSLAStatus{SubSLA: sub, Status: "NA"}
			if rtt > sub.Latency.Duration {
				status.Status = "Lat_Not_Met"
			} else if node_hts < minReadTSPerSubSLA[i] {
				status.Status = "Consistency_Not_Met"
			} else {
				status.Status = "Met"
				statuses = append(statuses, status)
				subGained := sub
				return &subGained, statuses
			}
			statuses = append(statuses, status)
		}

		fmt.Println("None of the utilities for cart-checking is met, returning nil")
		return nil, statuses
	}

	if activeSLA.ID == "new_sla" {
		fmt.Println(minReadTSPerSubSLA)
		fmt.Printf("node_hts is %d and rtt is %v\n", node_hts, rtt)

		for i, sub := range activeSLA.SubSLAs {
			status := monitor.SubSLAStatus{SubSLA: sub, Status: "NA"}
			if rtt > sub.Latency.Duration {
				status.Status = "Lat_Not_Met"
			} else if node_hts < minReadTSPerSubSLA[i] {
				status.Status = "Consistency_Not_Met"
			} else {
				status.Status = "Met"
				statuses = append(statuses, status)
				subGained := sub
				return &subGained, statuses
			}
			statuses = append(statuses, status)
		}

		fmt.Println("None of the utilities is met, returning nil")
		return nil, statuses
	}

	fmt.Println("SLA ID not recognized, returning nil")
	return nil, statuses
}

// =====================
// Monitoring Functions
// =====================

// Note: We added this to our client-api, but this could also be done in the "beginSession" function
// Start RTT for each node in the replicaiton config
func SendProbes() {
	for _, node := range GlobalConfig.Nodes {
		err := MeasureProbeRTT(node.Address, 2, 5)	// Pass timeout and pingCount to the function as well
		
		if (err != nil) {
			fmt.Println("Error happened sending probes to node: %s\n", node.Address)
		}
		
	} 
}

func MeasureProbeRTT(host string, timeout time.Duration, pingCount int) error {
	url := fmt.Sprintf("http://%s/probe", host)
	fmt.Println("Probing URL:", url)

	// Temporarily set timeout on shared client (optional, but acceptable for controlled use)
	httpClient.Timeout = timeout * time.Second

	// Warm-up phase (not timed)
	warmupCount := 2
	for i := 0; i < warmupCount; i++ {
		resp, err := httpClient.Get(url)
		if err != nil {
			fmt.Printf("Warm-up error pinging %s: %v\n", url, err)
			continue
		}
		resp.Body.Close()
	}

	// Actual RTT measurement phase
	for i := 0; i < pingCount; i++ {
		start := time.Now()
		resp, err := httpClient.Get(url)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("Error pinging %s: %v\n", url, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			monitor.RecordRTT(host, time.Duration(elapsed.Milliseconds()) * time.Millisecond)
		} else {
			return err
		}
	}
	return nil
}

func SetArtificialLat(nodeId string, lag time.Duration) {
	lagMu.Lock()
	defer lagMu.Unlock()

	found := false
	for _, node := range GlobalConfig.Nodes {
		if node.Id == nodeId {
			artificialLags[node.Address] = lag
			found = true
			break
		}
	}
	if !found {
		fmt.Printf("Warning: nodeId %s not found in GlobalConfig.Nodes\n", nodeId)
	}
}

func getArtificialLag(node string) time.Duration {
	lagMu.RLock()
	defer lagMu.RUnlock()
	return artificialLags[node]
}

// =====================
// Debugging Functions
// =====================

func PrintRTTs() {
	for _, node := range GlobalConfig.Nodes {
		fmt.Println(node.Id, monitor.GetRTTs(node.Address))
	}
}

// ==========================================
// Helper Functions for the Pe-Laoding Phase
// ==========================================
func GetPrimaryLatestKey(key string) (value string, obj_ts int64, high_timestamp int64, err error) {
	shardID := determineShardForKey(key)
	val, obj_ts, node_hts, _, err := readFromNode(key, GlobalConfig.Shards[shardID].Primary)
	return val, obj_ts, node_hts, err
}	

func WaitForSecondaries(target_ts int64, target_key string) {
	// Now wait for secondaries to reach the obj_ts
	fmt.Println("Waiting for secondaries to catch up...")

	timeout := time.After(120 * time.Second)	// Give it max 2 minutes
	ticker := time.NewTicker(5 * time.Second)	// Try every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			fmt.Println("Timeout waiting for secondaries to replicate.")
			return
		case <-ticker.C:
			allCaughtUp := true

			shardID := determineShardForKey(target_key)
				for _, secondary := range GlobalConfig.Shards[shardID].Secondaries {
				url := fmt.Sprintf("http://%s/status", secondary)

				resp, err := http.Get(url)
				if err != nil {
					fmt.Printf("Failed to contact secondary %s: %v\n", secondary, err)
					allCaughtUp = false
					continue
				}

				var status map[int]int64
				if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
					fmt.Printf("Invalid response from %s: %v\n", secondary, err)
					allCaughtUp = false
					resp.Body.Close()
					continue
				}
				resp.Body.Close()

				// Get timestamp for the relevant shard
				secTimestamp := status[shardID]
				if secTimestamp < target_ts {
					fmt.Printf("Secondary %s for shard %d not caught up (has %d, want %d)\n", secondary, shardID, secTimestamp, target_ts)
					allCaughtUp = false
				}
			}

			if allCaughtUp {
				fmt.Println("All secondaries have caught up.")
				return
			}
		}
	}
}