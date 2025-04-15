package main

import (
	"fmt"
	"os"
	"strconv"
	"math/rand"
	"encoding/json"
	"pileus/redis"
	"github.com/google/uuid"
)

type Record struct {
    Key   int `json:"key"`
    Value string `json:"value"`
}

// The logic for sharding based on the shard_config
type Shard struct {
	RangeStart int `json:"start"`
	RangeEnd   int `json:"end"`
	PrimaryAddress	string `json:"primary"` 
	Primary	redis.Client `json:"-"`	
}

type Config struct {
	Shards []Shard `json:"shards"`
}

// Define the Global varibales: config, redis.Client
var Node0_Store redis.Client
var GlobalConfig *Config

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	// Initialize Redis clients for each shard
	for i, shard := range config.Shards {
	
		opts := redis.DefaultOptions
		opts.Address = shard.PrimaryAddress

		client, err := redis.NewClient(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to redis at %s: %w", shard.PrimaryAddress, err)
		}

		config.Shards[i].Primary = client
	}

	return &config, nil
}

func main() {

	// Load the sharding config 
	var err error

	GlobalConfig, err = LoadConfig("sharding_config.json")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Loaded config: %+v\n", GlobalConfig)

	putWorkload(20)
}

func putWorkload(count int) error {
	fmt.Printf("Entered the putworkalod function\n")

    for i := 1; i <= count; i++ {
		// Randomly generate the key and value
        key := rand.Intn(6000)
		value := uuid.New().String()

        rec := Record{
            Key:   key,
            Value: value,
        }

		fmt.Printf("Record: %+v\n", rec)

        err := sendPut(rec)
        if err != nil {
            return fmt.Errorf("failed to sendPut for key %s: %w", key, err)
        }
    }
    return nil
}

func sendPut(rec Record) error {
	fmt.Printf("Entered the sendPut function\n")
    shardID := determineShardForKey(rec.Key)
	fmt.Printf("shardId is %d \n", shardID)

	var err error
	err = GlobalConfig.Shards[shardID].Primary.Set(strconv.Itoa(rec.Key), rec.Value)
    if err != nil {
		fmt.Printf("Failed with the write\n")
		fmt.Printf("%w \n", err)
        return fmt.Errorf("failed to store key %d: %w", rec.Key, err)
    }
    return nil
}

func determineShardForKey(key int) int {
    fmt.Printf("Entered determineShardForKey with key %d\n", key)
   
    for i, shard := range GlobalConfig.Shards {
    
        if key >= shard.RangeStart && key <= shard.RangeEnd {
            fmt.Printf("Key %d belongs to shard #%d: %+v\n", key, i, shard)
            return i
        }
    }
    panic(fmt.Sprintf("No shard found for key: %d", key))
}
