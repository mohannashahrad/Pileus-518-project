package main

import (
	"encoding/json"
	"fmt"
	"os"
	"net/http"
	"pileus/redis"
	"pileus/util"
)

// each storage node is co-located with a local redis instance
var localStore redis.Client
var storageID string
var shard_range_start int
var shard_range_end int

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

	fmt.Println("Storage node listening on :8080")
	http.ListenAndServe(":8080", nil)
}

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func setShardRange() {
	var err error

	conf, err := util.LoadConfig("../sharding_config.json")
	if err != nil {
		panic(err)
	}

	for _, shard := range conf.Shards {
    
        if storageID == shard.PrimaryID {
			shard_range_start = shard.RangeStart
			shard_range_end = shard.RangeEnd
        }
    }

}

func handleSet(w http.ResponseWriter, r *http.Request) {
	var rec Record
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
	var val string
	found, err := localStore.Get(key, &val)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	json.NewEncoder(w).Encode(Record{Key: key, Value: val})
}
