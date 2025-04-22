package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
	"strconv"
	"net/http"
	"pileus/redis"
	"pileus/util"
)

// each storage node is co-located with a local redis instance
var localStore redis.Client
var storageID string
var shard_range_start int
var shard_range_end int
var primaryShard util.Shard		// Note: Assumption that each storage node is primary for 1 shard for now
var secondaryShards []util.Shard

// How to invoke: go run storage_server.go <storage_id>
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run storage_server.go <storage-id> <replication-config-path>")
		os.Exit(1)
	}
	storageID := os.Args[1]
	configPath := os.Args[2]

	fmt.Println("Starting storage node with ID:", storageID)
	fmt.Println("Using replication config:", configPath)

	// Load the key/shard ranges that this node is primary/secondary for (using storageID)
	// Note: I think in the paper's evaluation only one shard is assumed
	initShards(configPath)

	opts := redis.DefaultOptions
	opts.Address = "localhost:6379" // connect to local Redis
	// shard range is also set in opts of the redisClient
	opts.ShardRangeStart = shard_range_start
	opts.ShardRangeEnd = shard_range_end

	var err error
	localStore, err = redis.NewClient(opts)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/replicate", replicationHandler)

	fmt.Println("Storage node listening on :8080")
	http.ListenAndServe(":8080", nil)
}

func initShards(configPath string) {
	var err error

	conf, err := util.LoadConfig(configPath)
	if err != nil {
		panic(err)
	}

	for _, shard := range conf.Shards {
    
		// Find the shard that the storage node is the primary for
		// TODO: for this version, we assume that each node is the primary for 1 shard
        if storageID == shard.PrimaryID {
			shard_range_start = shard.RangeStart
			shard_range_end = shard.RangeEnd
			shard.AmIPrimary = true
			shard.AmISecondary = false
			shard.HighTS= 0.0

			primaryShard = shard
        }

		// Also find the shards that the storage node is secondary for
		if util.Contains(shard.Secondaries, storageID) {
			shard.AmIPrimary = false
			shard.AmISecondary = true
			shard.HighTS= 0.0
			secondaryShards = append(secondaryShards, shard)
		}
    }

	// Then set-up tasks for the secondary shards replication pulls
	fmt.Printf("Secondary for shards: %+v\n", secondaryShards)

	for _, shard := range secondaryShards {
		go func(shard util.Shard) {
			// TODO: the frequency of the replication pulling should be tuned
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
	
			for range ticker.C {
				err := pullFromPrimary(&shard)
				if err != nil {
					fmt.Printf("Replication error from primary %s: %v\n", shard.Primary, err)
				}
			}
		}(shard)
	}

}

func handleSet(w http.ResponseWriter, r *http.Request) {
	var rec util.Record
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Attempt to store the key-value pair
	obj_ts, err := localStore.Set(rec.Key, rec.Value)

	// Update HighTS if successful
	if err == nil {
		primaryShard.HighTS = obj_ts
		fmt.Println("Primary shard is updated to %v\n:", primaryShard)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]int64{
		"put_timestamp": obj_ts,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	numericKey, err := util.KeyToInt(key)
	if err != nil {
		return 
	}
	
	var record redis.VersionedValue
	found, err := localStore.Get(key, &record)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}

	// Find HighTS of the shard for the requested key [should either be the primary shard or the secondary shard]
	var shardHighTS int64
	if primaryShard.RangeStart <= numericKey && numericKey <= primaryShard.RangeEnd {
		shardHighTS = primaryShard.HighTS
	} else {
		for _, shard := range secondaryShards {
			if shard.RangeStart <= numericKey && numericKey <= shard.RangeEnd {
				shardHighTS = shard.HighTS
				break
			}
		}
	}

	
	// Return: Key,Value + Obj timestamp + Shard/Node High Timestamp
	response := struct {
		Key       string `json:"key"`
		Value     any    `json:"value"`
		Timestamp int64  `json:"timestamp"`
		HighTS	  int64  `json:"highTS"`
	}{
		Key:       key,
		Value:     record.Value,
		Timestamp: record.Timestamp,
		HighTS: shardHighTS,
	}

	json.NewEncoder(w).Encode(response)
}

// Endpoint for replciation between storage nodes (pull-based)
// only invoked for shards that the current storage node is primary for
func replicationHandler(w http.ResponseWriter, r *http.Request) {
	sinceStr := r.URL.Query().Get("since")
	startKeyStr := r.URL.Query().Get("start")
	endKeyStr := r.URL.Query().Get("end")

	sinceTS, err := strconv.ParseInt(sinceStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid timestamp", http.StatusBadRequest)
		return
	}
	sinceTime := time.UnixMilli(sinceTS)

	startKey, err := strconv.Atoi(startKeyStr)
	if err != nil {
		http.Error(w, "Invalid start range", http.StatusBadRequest)
		return
	}

	endKey, err := strconv.Atoi(endKeyStr)
	if err != nil {
		http.Error(w, "Invalid end range", http.StatusBadRequest)
		return
	}

	// scan the shard for the timestamps > sinceTime
	// TODO: this should be improved, right now very expensive
	updates := localStore.ScanUpdatedKeys(sinceTime, startKey, endKey)

	w.Header().Set("Content-Type", "application/json")

	type Response struct {
		Updates []util.Record `json:"updates"`
		Version int64    `json:"version"`
	}

	var resp Response

	// If no updates, still share the HTS of the shard
	// NOTE: this is based on the assumption that each node is primary for a shard for now [and this endpoint is only called regarding the same shard that the current node is primary for]
	if len(updates) == 0 {
		resp = Response{
			Updates: nil,
			Version: primaryShard.HighTS, // TODO: according to 4.3, should this be the node's timestamp itself?
		}
	} else {
		resp = Response{
			Updates: updates,
			Version: -1,
		}
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func pullFromPrimary(shard *util.Shard) error {
	url := fmt.Sprintf("http://%s/replicate?since=%d&start=%d&end=%d", shard.HighTS, shard.RangeStart, shard.RangeEnd)
	fmt.Println(url)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println(resp);
		return fmt.Errorf("non-200 from primary: %d", resp.StatusCode)
	}

	// Assuming that key and values are being returned with timestamps from the shard primary
	var response struct {
		Updates     []struct {
			Key       string `json:"key"`
			Value     string `json:"value"`
			Timestamp int64  `json:"timestamp"`
		} `json:"updates"`
		Version int64 `json:"version"` 
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}

	// If there are no updates, then just update the HighTS
	if len(response.Updates) == 0 {
		shard.HighTS = response.Version
	} else {
		// Apply the updates and HS of the shard
		for _, update := range response.Updates {
			fmt.Printf("update recieved is %v", update)

			vv := redis.VersionedValue{
				Value:     update.Value,
				Timestamp: update.Timestamp,
			}
			err := localStore.SetVersioned(update.Key, vv)
			if err != nil {
				fmt.Printf("Error setting key %s: %v\n", update.Key, err)
				continue
			}
		
			// Updating the shard HighTS
			if update.Timestamp > shard.HighTS {
				fmt.Printf("Updating the shard HighTs to %d\n", update.Timestamp)
				shard.HighTS = update.Timestamp
			}
		}
	}

	return nil
}

// TODO: change the pull mechanism so that it's per shard
// for each shard then also share the high timestamp