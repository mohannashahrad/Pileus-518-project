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

// ========== GET/PUT Endpoints ==========

type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

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
	monitor.RecordRTT(GlobalConfig.Shards[shardID].Primary, rtt)

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
	storageNode, targetSubSLA, _ := optimizer.FindNodeToRead(s, key, activeSLA)
	fmt.Printf("chosen storage node is %v and chosen subsla is %v\n", storageNode, targetSubSLA)
	// fmt.Printf("minReadTSPerSubSLA for subslas is %v\n", minReadTSPerSubSLA)

	// Perform the read + calculate exact utility achieved
	val, obj_ts, node_hts, rtt, err := readFromNode(key, storageNode)

	// Calculate and track utility based on get_timestamp and rtt (consistency + latency)
	subAchieved := detectSubSLAHit(obj_ts, node_hts, rtt, targetSubSLA, activeSLA)

	// If no sub-sla is achieved
	if subAchieved == nil {
		fmt.Println("No utility could be computed, because gained subSLA was null")
		s.Utilities = append(s.Utilities, 0.0)
		s.ObjectsRead[key] = obj_ts
		return val, consistency.SubSLA{}, fmt.Errorf("no utility could be computed")
	}

	// Update session read utilities
	s.Utilities = append(s.Utilities, subAchieved.Utility)

	// Update the read timestamp of the object read
	s.ObjectsRead[key] = obj_ts

	return val, *subAchieved, err
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

	start := time.Now()
	resp, err := http.Get(url)
	rtt := time.Since(start)

	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("Error invoking the storage node's GET endpoint\n")
		return "", -1, -1 , 0, fmt.Errorf("HTTP error: %v", err)
	}
	defer resp.Body.Close()

	// TODO: here should we check the timestamp of the object to make sure consistency was met?
	var response struct {
		Key       string    `json:"key"`
		Value     string 	`json:"value"`
		Timestamp int64     `json:"timestamp"`
		HighTS 	  int64 	`json:"highTS"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", -1, -1, 0, fmt.Errorf("error decoding response: %v", err)
	}

	// If no err, update the RTT window
	monitor.RecordRTT(storageNode, rtt)

	// Extracting the timestamp info returned from the storage node
	object_ts := response.Timestamp
	node_high_ts := response.HighTS

	// fmt.Printf("Returned Object TS is: %d\n", object_ts)
	// fmt.Printf("HighTS of the node responding is: %d\n", node_high_ts)

	monitor.RecordHTS(storageNode, node_high_ts)
	
	return response.Value, object_ts, node_high_ts, rtt, nil
}

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
		fmt.println(err)
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

// This is for evaluation purposes
// TODO: utility calculation is highly tuned for password checking now
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

	val, _, _, rtt, err := readFromNode(key, randomNode.Address)
	fmt.Println("RTT was %f", rtt)

	if (err != nil) {
		// Some error happened for the key
		fmt.Println("random read failed with error")
		fmt.println(err)
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	for _, sub := range activeSLA.SubSLAs {
		if ( rtt <= sub.Latency.Duration && randomNode.Address == primaryForKey) {
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
		
		// TODO: implement for other consistencies

		// else if (rtt <= sub.Latency.Duration) {
		// 	fmt.Println("Should compare sub sonsitency level %d and what we got from the random node")
		// 	return val, consistency.SubSLA{}, fmt.Errorf("subSLA checking not implemented")
		// }
	}

	// If we have not returned yet, then no sub-SLA is met
	fmt.Println("No utility could be computed for random read")
	s.Utilities = append(s.Utilities, 0.0)
	return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
}

// TODO: utility calculation is highly tuned for password checking now
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

	val, _, _, rtt, err := readFromNode(key, closestNode)
	fmt.Println("RTT was %f", rtt)

	// TODO: here the retry mechanism should be done
	if (err != nil) {
		// Some error happened for the key
		fmt.Println("closest read failed with error")
		fmt.println(err)
		s.Utilities = append(s.Utilities, 0.0)
		return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
	}

	for _, sub := range activeSLA.SubSLAs {
		if (rtt <= sub.Latency.Duration && closestNode == primaryForKey) {
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
		
		// TODO: implement for other consistencies
	}

	// If we have not returned yet, then no sub-SLA is met
	fmt.Println("No utility could be computed for closest read")
	s.Utilities = append(s.Utilities, 0.0)
	return val, consistency.SubSLA{}, fmt.Errorf("No subSLA met")
}

// TODO: This implementation is right now highly tuned for the SLA's we are testing. Generalize this implementation
// TOO: client atogether with their sla's should register the utility calculator function
func detectSubSLAHit(obj_ts int64, node_hts int64, rtt time.Duration, targetSubSLA consistency.SubSLA, activeSLA *consistency.SLA) *consistency.SubSLA{
	if (activeSLA.ID == "psw_sla") {
		for _, sub := range activeSLA.SubSLAs {
			// If we targeted strong consistency (contacted primary)
			if (targetSubSLA.Consistency == 4) {
				if (rtt <= sub.Latency.Duration) {
					subGained := sub
					return &subGained
				}
			} else if (targetSubSLA.Consistency == 0) {
				// If aimed for eventual, then strong is not met [//NOTE: this is our assumption]
				if (sub.Consistency == 0 && rtt <= sub.Latency.Duration) {
					subGained := sub
					return &subGained
				}
			}
		}

		// If didn't return yet, no sub-SLA was met 
		fmt.Println("None of the utilities for password-checking is met, returning nil: \n")
		return nil
		
	}

	// TODO: implement the other sub-SLA here 

	// If didn't return yet, no sub-SLA was met 
	fmt.Println("Specific utility computing function is not implemented, returning nil: \n")
	return nil
}

// =====================
// Monitoring Functions
// =====================

// Note: We added this to our client-api, but this could also be done in the "beginSession" function
// Start RTT for each node in the replicaiton config
func SendProbes() {
	for _, node := range GlobalConfig.Nodes {
		rtt, err := MeasureProbeRTT(node.Address, 2, 5)	// Pass timeout and pingCount to the function as well
		
		if (err == nil) {
			monitor.RecordRTT(node.Address, rtt)
		} else {
			fmt.Println("Error happened sending probes to node: %s\n", node.Address)
		}
		
	} 
}

func MeasureProbeRTT(host string, timeout time.Duration, pingCount int) (time.Duration, error) {
	var total float64
	var success int

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
			total += float64(elapsed.Milliseconds())
			success++
		}
	}

	if success == 0 {
		return 0, fmt.Errorf("no successful responses")
	}

	rtt_float := total / float64(success)
	return time.Duration(rtt_float * float64(time.Millisecond)), nil
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