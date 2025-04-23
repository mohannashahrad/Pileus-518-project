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
)


var GlobalConfig *util.ReplicationConfig

// =====================
// Core API Methods
// =====================

// TODO: The session monitoring functions should be implemented
// Default: Each session starts with a default SLA, but the Get reqs in the session could specify their SLA's also 

func BeginSession(sla consistency.SLA) *util.Session {
	return &util.Session{
		DefaultSLA: sla,
		ObjectsWritten:	make(map[string]int64),
		ObjectsRead:	make(map[string]int64),
		Utilities:		[]float64{},
	}
}

// TODO: how should it be implemented?
func EndSession(s *util.Session) {
	// Clean up state
}

// ========== GET/PUT Endpoints ==========

type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

// This will update session metadata on write timestamps
func Put(s *util.Session, key string, value string) error {

	fmt.Printf("Entered the api Put function\n")
    shardID := determineShardForKey(key)
	fmt.Printf("shardId is %d \n", shardID)

	rec := Record{
		Key:   key,
		Value: value,
	}

	recordJson, _ := json.Marshal(rec)
	url := fmt.Sprintf("http://%s/set", GlobalConfig.Shards[shardID].Primary)

	start := time.Now()
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(recordJson))
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
	fmt.Printf("Set succeeded. Updating session write timestamp: %d\n", result.SetTimestamp)
	s.ObjectsWritten[key] = result.SetTimestamp          

    return nil
}

// The return value to the client indicates how well the SLA was met [which sub-SLA was hit]
// TODO: what happens if consistency is met but RTT is unexpectedly a lot?? [what would be the utility]
func Get(s *util.Session, key string, sla *consistency.SLA) (string, util.ConditionCode, error) {
	// Determine SLA for the op: use session default if not specified by input
	activeSLA := s.DefaultSLA
	if sla != nil {
		activeSLA = *sla
	}

	// Find the storage node that maximizes the utility
	storageNode, chosenSubSLA, minReadTSPerSubSLA := optimizer.FindNodeToRead(s, key, &activeSLA)
	fmt.Printf("chosen storage node is %v and chosen subsla is %v\n", storageNode, chosenSubSLA)
	fmt.Printf("minReadTSPerSubSLA for subslas is %v\n", minReadTSPerSubSLA)

	// Perform the read + calculate exact utility achieved
	val, get_timestamp, rtt, err := readFromNode(key, storageNode)

	// Calculate and track utility based on get_timestamp and rtt (consistency + latency)

	// The consistency level we were aiming (Note that it is only possible for the actual level to be better (stronger) than what we chose not weaker)
	// TODO: how to check if utility is better or worse than the 
	target_latency := chosenSubSLA.LatencyBound

	// TODO: complete here for correct utility computation
	if (rtt <= target_latency) {
		s.Utilities = append(s.Utilities, chosenSubSLA.Utility)
	}

	s.ObjectsRead[key] = get_timestamp

	// Condition Code: Sub-SLA hit + weather the latency was met or not
	// TODO: re-implement condition code here
	return val, nil, err
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

	GlobalConfig = &config

	// Also udpate the optimizer with the same config
	optimizer.Init(GlobalConfig)

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
func readFromNode(key string, storageNode string) (string, int64, time.Duration, error) {

	url := fmt.Sprintf("http://%s/get?key=%s", storageNode, key)

	start := time.Now()
	resp, err := http.Get(url)
	rtt := time.Since(start)

	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("Error invoking the storage node's GET endpoint\n")
		return "", -1, 0, fmt.Errorf("HTTP error: %v", err)
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
		return "", -1, 0, fmt.Errorf("error decoding response: %v", err)
	}

	// If no err, update the RTT window
	monitor.RecordRTT(storageNode, rtt)

	// Extracting the timestamp info returned from the storage node
	object_ts := response.Timestamp
	node_high_ts := response.HighTS

	fmt.Printf("Returned Object TS is: %d\n", object_ts)
	fmt.Printf("HighTS of the node responding is: %d\n", node_high_ts)

	monitor.RecordHTS(storageNode, node_high_ts)
	
	return response.Value, object_ts, rtt, nil
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
	fmt.Println(url)

	client := http.Client{
		Timeout: timeout * time.Second,
	}

	// Pinging 5 times
	for i := 0; i < pingCount; i++ {
		start := time.Now()
		resp, err := client.Get(url)
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