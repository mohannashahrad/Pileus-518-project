package main

import (
	"fmt"
	"github.com/google/uuid"
	"client/util"
	"client/api"
	"client/consistency"
)

// TODO: this records thing should be changed
type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

var GlobalSLAs = map[string]consistency.SLA{}

func main() {

	var err error
	err = api.LoadReplicationConfig("../single_shard_config.json")
	if err != nil {
		fmt.Printf("An error happened loading the replication configuration.\n")
		panic(err)
	}

	loadStaticSLAs()

	preloadData(10000)

}

func loadStaticSLAs() {
	var sla consistency.SLA
	var err error
	
	sla, err = util.LoadSLAFromFile("consistency/samples/psw_cloudlab.json", "psw_sla")
	if err != nil {
		panic(err)
	}
	GlobalSLAs[sla.ID] = sla

	sla, err = util.LoadSLAFromFile("consistency/samples/web_application.json", "web_sla")
	if err != nil {
		panic(err)
	}
	GlobalSLAs[sla.ID] = sla

	sla, err = util.LoadSLAFromFile("consistency/samples/shopping_cart.json", "cart_sla")
	if err != nil {
		panic(err)
	}
	GlobalSLAs[sla.ID] = sla

	sla, err = util.LoadSLAFromFile("consistency/samples/strong.json", "strong_sla")
	if err != nil {
		panic(err)
	}
	GlobalSLAs[sla.ID] = sla

	sla, err = util.LoadSLAFromFile("consistency/samples/readMyWrites.json", "read_my_write_sla")
	if err != nil {
		panic(err)
	}
	GlobalSLAs[sla.ID] = sla
}

// Write 10K key, value pairs to primary [and wait until it's replicated everywhere]
func preloadData(keySpace int) {
	fmt.Println("Starting preload of 10K keys to primary...")

	sla := GlobalSLAs["strong_sla"] 

	s := api.BeginSession(&sla, util.Primary) 

	i := 1
	for ; i <= keySpace; i++ {
		key := fmt.Sprintf("%04d", i)
		value := uuid.New().String()

		fmt.Printf("key=%s value=%s: \n", key, value)
		err := api.Put(s, key, value)
		if err != nil {
			fmt.Printf("Failed to put key=%s: %v\n", key, err)
		}
		if i%1000 == 0 {
			fmt.Printf("Preloaded %d keys...\n", i)
		}
	}

	api.EndSession(s)

	// TODO: handle multi-shards for this later
	// Wait for replication to complete on secondaries
	waitForPreLoadingReplication(fmt.Sprintf("%04d", i-1))

	fmt.Println("Finished preloading keys.")
}

func waitForPreLoadingReplication(lastKey string) {
	// First get the high timestamp of the primary (the TS of the last object written)
	_, obj_ts, high_timestamp, err := api.GetPrimaryLatestKey(lastKey)

	if (err != nil) {
		fmt.Printf("Failed to get the last key from primary: %v\n", err)
	}

	if (obj_ts != high_timestamp ) {
		fmt.Printf("Latest obj timestamp is not the same as the node timestamp")
	}

	api.WaitForSecondaries(obj_ts, lastKey)
}