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
	"os/signal"
	"syscall"
	"github.com/google/uuid"
)

// each storage node is co-located with a local redis instance
var localStore redis.Client
var storageID string
var configPath string
var shard_range_start int
var shard_range_end int
var primaryShard util.Shard		// Note: Assumption that each storage node is primary for 1 shard for now
var secondaryShards []util.Shard

// How to invoke: go run storage_server.go <storage_id>  <replication-config-path>
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run storage_server.go <storage-id> <replication-config-path>")
		os.Exit(1)
	}
	storageID = os.Args[1]
	configPath = os.Args[2]

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

	err = localStore.FlushAll()
	if err != nil {
		fmt.Println("Failed to flush Redis:", err)
		os.Exit(1)
	}
	fmt.Println("Redis flushed successfully")

	// preload the store with data
	preloadKeys(10000)

	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/replicate", replicationHandler)
	http.HandleFunc("/probe", handleProbe)
	http.HandleFunc("/status", sendLatestStatus)
	http.HandleFunc("/adjust_replication", adjustReplicationHandler)

	// Shutdown Signal Handler: For storing the high timestamp information (on Redis)
	go handleShutdown()

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
			// If it does not exist, return 0
			shard.HighTS=loadHighTSFromSnapshot(shard.ShardId)
			primaryShard = shard
        }

		// Also find the shards that the storage node is secondary for
		if util.Contains(shard.Secondaries, storageID) {
			shard.AmIPrimary = false
			shard.AmISecondary = true
			fmt.Println("loading the shard highTS from secondary\n")
			shard.HighTS= loadHighTSFromSnapshot(shard.ShardId)
			secondaryShards = append(secondaryShards, shard)
		}
    }

	// Then set-up tasks for the secondary shards replication pulls
	fmt.Printf("Secondary for shards: %+v\n", secondaryShards)

	fmt.Printf("Secondary shards after snapshot loading: %+v\n", secondaryShards)
	fmt.Printf("Primary shard after snapshot loading: %+v\n", primaryShard)

	for i := range secondaryShards {
		shard := &secondaryShards[i]
		// go func(shard *util.Shard) {
		// 	// ticker := time.NewTicker(20 * time.Second)
		// 	fmt.Println("Setting rep freq to ", shard.ReplicationFrequencySeconds)
		// 	ticker := time.NewTicker(time.Duration(shard.ReplicationFrequencySeconds) * time.Second)
		// 	defer ticker.Stop()
	
		// 	for range ticker.C {
		// 		err := pullFromPrimary(shard)
		// 		if err != nil {
		// 			fmt.Printf("Replication error from primary %s: %v\n", shard.Primary, err)
		// 		}
		// 	}
		// }(shard)

		go func(shard *util.Shard) {
			for {
				freq := shard.ReplicationFrequencySeconds
				fmt.Printf("Sleeping for %d seconds before pulling updates...\n", freq)
	
				timer := time.NewTimer(time.Duration(freq) * time.Second)
				<-timer.C
	
				err := pullFromPrimary(shard)
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
	// TODO: Is this even correct?
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

func adjustReplicationHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ShardID int `json:"shardID"`
		NewFreq float64 `json:"new_freq"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	for i := range secondaryShards {
		if secondaryShards[i].ShardId == req.ShardID {
			fmt.Printf("Updating replication frequency for shard %d to %d seconds\n", req.ShardID, req.NewFreq)
			secondaryShards[i].ReplicationFrequencySeconds = req.NewFreq
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
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
		fmt.Println("No updates to send just sharing the HighTS\n")
		resp = Response{
			Updates: nil,
			Version: primaryShard.HighTS, // TODO: according to 4.3, should this be the node's timestamp itself?
		}
	} else {
		fmt.Println("There exists updates to share with secondaries\n")
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
	url := fmt.Sprintf("http://%s/replicate?since=%d&start=%d&end=%d", shard.Primary, shard.HighTS, shard.RangeStart, shard.RangeEnd)
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
			fmt.Printf("update recieved is %v\n", update)

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

// This endpoint is called when the primary want to check how up-to-date the secondaries are
func sendLatestStatus(w http.ResponseWriter, r *http.Request) {
	// Make a Map of shardID -> high timestamp
	status := make(map[int]int64) 
	
	for _, shard := range secondaryShards {
		status[shard.ShardId] = shard.HighTS
	}
	json.NewEncoder(w).Encode(status)
}

func handleProbe(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Before shutting down, persist the shard hightimestam information
func handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	fmt.Println("\nReceived shutdown signal. Saving HighTS to disk...")

	snapshotHighTS()

	os.Exit(0)
}

func snapshotHighTS() {
	snapshot := make(map[int]int64)

	// Add primary shard
	if (primaryShard.AmIPrimary) {
		snapshot[primaryShard.ShardId] = primaryShard.HighTS
	}
	

	// Add secondary shards
	for _, shard := range secondaryShards {
		if (shard.AmISecondary) {
			snapshot[shard.ShardId] = shard.HighTS
		}	
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		fmt.Println("Failed to serialize HighTS snapshot:", err)
		return
	}

	err = os.WriteFile("high_ts_snapshot.json", data, 0644)
	if err != nil {
		fmt.Println("Failed to write HighTS snapshot to file:", err)
	}
}

// TODO: here we should double check this aligns with existence of these keys in Redis
func loadHighTSFromSnapshot(shardId int) int64 {
	data, err := os.ReadFile("high_ts_snapshot.json")
	if err != nil {
		fmt.Println("No existing HighTS snapshot found.")
		return 0
	}

	var snapshot map[int]int64
	if err := json.Unmarshal(data, &snapshot); err != nil {
		fmt.Println("Failed to parse HighTS snapshot:", err)
		return 0
	}

	if ts, ok := snapshot[shardId]; ok {
		return ts
	}

	return 0
}

func preloadKeys(count int) {
	fmt.Println("Preloading Redis with deterministic key-value pairs")

	namespace := uuid.NewSHA1(uuid.NameSpaceDNS, []byte("pileus"))

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%04d", i)
		// Generate a deterministic UUID based on the key name
		value := uuid.NewMD5(namespace, []byte(fmt.Sprintf("%04d", i))).String()

		vv := redis.VersionedValue{
			Value:     value,
			Timestamp: -1,
		}
		err := localStore.SetVersioned(key, vv)
		if err != nil {
			fmt.Printf("Failed to preload key %s: %v\n", key, err)
		}
	}
}