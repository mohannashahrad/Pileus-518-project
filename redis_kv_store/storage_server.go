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
var secondaryShards []util.SecondaryShardInfo

// How to invoke: go run storage_server.go <storage_id>
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run storage.go <storage-id>")
		os.Exit(1)
	}
	storageID = os.Args[1]
	fmt.Println("Starting storage node with ID:", storageID)

	// Load the key/shard ranges that this node is primary for (using storageID)
	setShardRange()

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

func setShardRange() {
	var err error

	conf, err := util.LoadConfig("../sharding_config.json")
	if err != nil {
		panic(err)
	}

	for _, shard := range conf.Shards {
    
		// Find the shard that the storage node is the primary for
        if storageID == shard.PrimaryID {
			shard_range_start = shard.RangeStart
			shard_range_end = shard.RangeEnd
        }

		// Also find the shards that the storage node is secondary for
		if util.Contains(shard.Secondaries, storageID) {

			secondaryShards = append(secondaryShards, util.SecondaryShardInfo{
				Primary:  shard.Primary,
				RangeStart: shard.RangeStart,
				RangeEnd:   shard.RangeEnd,
				HighTS: 0,
			})
		}
    }

	// Then set-up tasks for the secondary shards replication pulls
	fmt.Printf("Secondary shards: %+v\n", secondaryShards)

	for _, shard := range secondaryShards {
		go func(shard util.SecondaryShardInfo) {
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
	// The store.set call checks wwather put for the key is allowed on this node [e.g. if the node is primary for the shard]
	err := localStore.Set(rec.Key, rec.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	
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
	
	response := struct {
		Key       string `json:"key"`
		Value     any    `json:"value"`
		Timestamp int64  `json:"timestamp"`
	}{
		Key:       key,
		Value:     record.Value,
		Timestamp: record.Timestamp,
	}

	json.NewEncoder(w).Encode(response)
}

// Endpoint for replciation between storage nodes (pull-based)
// only invoked for shards that the current storage node is primary for
func replicationHandler(w http.ResponseWriter, r *http.Request) {
	sinceStr := r.URL.Query().Get("since")
	sinceTS, err := strconv.ParseInt(sinceStr, 10, 64) // timestamp in ms
	if err != nil {
		http.Error(w, "Invalid timestamp", http.StatusBadRequest)
		return
	}

	sinceTime := time.UnixMilli(sinceTS)

	// scan the shard for the timestamps > sinceTime
	// TODO: this should be improved, right now very expensive
	updates := localStore.ScanUpdatedKeys(sinceTime)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(updates)

}

func pullFromPrimary(shard *util.SecondaryShardInfo) error {
	url := fmt.Sprintf("http://%s/replicate?since=%d", shard.Primary, shard.HighTS)
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
	var updates []struct {
		Key       string `json:"key"`
		Value     string `json:"value"`
		Timestamp int64  `json:"timestamp"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&updates); err != nil {
		return err
	}

	// Apply the updates and HS of the shard
	for _, update := range updates {
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
	
		if update.Timestamp > shard.HighTS {
			fmt.Printf("Updating the shard HighTs to %d\n", update.Timestamp)
			shard.HighTS = update.Timestamp
		}
	}

	return nil
}