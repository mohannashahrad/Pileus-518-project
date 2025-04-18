package api

import (
	"client/consistency"
	// "errors"
	"fmt"
	"net/http"
	"bytes"
	// "io/ioutil"
	"encoding/json"
	"os"
	"strconv"
	// "sync"
)

// TODO: Do we even care about the notion of "Tables" here? [what if we use a pute key-value store]

type Session struct {
	DefaultSLA consistency.SLA
	// TODO: Add session-specific state for monotonic reads, etc.
}

// A condition code returned by Get:
// Indicates how well the SLA was met, including the consistency of the data.
type ConditionCode int

const (
	CC_Success ConditionCode = iota
	CC_Stale
	CC_ConsistencyNotMet
	CC_LatencyExceeded
	CC_SessionError
)

func (cc ConditionCode) String() string {
	switch cc {
	case CC_Success:
		return "OK"
	case CC_Stale:
		return "Stale"
	case CC_ConsistencyNotMet:
		return "ConsistencyNotMet"
	case CC_LatencyExceeded:
		return "LatencyExceeded"
	case CC_SessionError:
		return "SessionError"
	default:
		return "Unknown"
	}
}

type Shard struct {
	RangeStart int `json:"start"`
	RangeEnd   int `json:"end"`
	Primary	string `json:"primary"` 
}

type Config struct {
	Shards []Shard `json:"shards"`
}

var GlobalConfig *Config

// =====================
// Core API Methods
// =====================

// TODO: The session monitoring functions should be implemented
// Default: Each session starts with a default SLA, but the Get reqs in the session could specify their SLA's also 
func BeginSession(sla consistency.SLA) *Session {
	return &Session{
		DefaultSLA: sla,
	}
}

func EndSession(s *Session) {
	// Clean up state
}

// ========== GET/PUT Endpoints ==========

type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

func Put(s *Session, key string, value string) error {

	fmt.Printf("Entered the api Put function\n")
    shardID := determineShardForKey(key)
	fmt.Printf("shardId is %d \n", shardID)

	rec := Record{
		Key:   key,
		Value: value,
	}

	recordJson, _ := json.Marshal(rec)
	url := fmt.Sprintf("http://%s/set", GlobalConfig.Shards[shardID].Primary)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(recordJson))
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("An error happened invoking the put endpoint of the storage node\n")
		fmt.Printf("%v \n", err)
		return fmt.Errorf("HTTP error: %v", err)
	}
    return nil
}

// TODO: implement "Condition Code", how to know which sla was met!
// TODO: here the probablisitic optimization should be implented
// TODO: should I pass sla by pointer?
func Get(s *Session, key string, sla *consistency.SLA) (string, ConditionCode, error) {
	// Determine SLA for the op: use session default if not specified by input
	activeSLA := s.DefaultSLA
	if sla != nil {
		activeSLA = *sla
	}

	// Case 1: If all SubSLAs require Strong consistency, contact the primary directly
	allStrong := true
	for _, sub := range activeSLA.SubSLAs {
		if sub.Consistency != consistency.Strong {
			allStrong = false
			break
		}
	}

	if allStrong {
		fmt.Printf("Contacting the primary for the Get operation\n")

		shardID := determineShardForKey(key)
		url := fmt.Sprintf("http://%s/get?key=%s", GlobalConfig.Shards[shardID].Primary, key)

		resp, err := http.Get(url)
		if err != nil || resp.StatusCode != http.StatusOK {
			fmt.Printf("Error invoking the storage node's GET endpoint\n")
			return "", CC_ConsistencyNotMet, fmt.Errorf("HTTP error: %v", err)
		}
		defer resp.Body.Close()

		var response struct {
			Key       string      `json:"key"`
			Value     string `json:"value"`
			Timestamp int64       `json:"timestamp"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return "", CC_ConsistencyNotMet, fmt.Errorf("error decoding response: %v", err)
		}

		return response.Value, CC_Success, nil
	}

	// TODO: SLA-based selection logic

	// Case 2: If all SubSLAs are at eventual consistency, go to the closest replica

	return "", CC_SessionError, fmt.Errorf("non-Strong SLA logic not implemented yet")
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
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	GlobalConfig = &config
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

// TODO: this should be called by the get endpoint per
	// monitoring data of the latency and high timestamps of the replicas
	// run the probablistic alg in the paper [Figure 8]
// func optimizer()