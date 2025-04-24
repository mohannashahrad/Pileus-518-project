package main

import (
	"fmt"
	"time"
	// "math/rand"
	"github.com/google/uuid"
	"client/consistency"
	"client/util"
	"client/api"
)

// TODO: this records thing should be changed
type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

var GlobalSLAs = map[string]consistency.SLA{}


func main() {

	// Load the sharding config [this is done on the api-side for checking the put/get]
	var err error
	err = api.LoadReplicationConfig("../single_shard_config.json")
	if err != nil {
		fmt.Printf("An error happened loading the replication configuration.\n")
		panic(err)
	}

	// Load the static SLAs
	loadStaticSLAs()
	fmt.Printf("Cart SLA: %+v\n", GlobalSLAs["cart_sla"])
	fmt.Printf("**************************************\n")
	fmt.Printf("Web Applicaiton SLA: %+v\n", GlobalSLAs["web_sla"])
	fmt.Printf("**************************************\n")
	fmt.Printf("Password SLA: %+v\n", GlobalSLAs["psw_sla"])

	// Before sending the workloads, send monitoring probes to the nodes to get RTT 
	// TODO: we can use probes also for HighTS to begin [for each shard]
	api.SendProbes()

	// fmt.Println("Checking the RTT's after sending init probes\n")
	// api.PrintRTTs()

	// TODO: this should be changed to a more realistic workload like YCSB

	// Uncomment based on the type of the experiment you want to check
	
	// password_checking_putWorkload(10, util.Pileus)
	// password_checking_putWorkload(10, util.Random)
	// password_checking_putWorkload(10, util.Primary)
	password_checking_putWorkload(10, util.Closest)
}

// Start a session and do a bunch of puts in the same session
func password_checking_putWorkload(count int, expType util.ServerSelectionPolicy) error {
	fmt.Printf("Entered the putworkalod function\n")

	// Start the session
	// We set the type of the exp in the session
	s := api.BeginSession(GlobalSLAs["psw_sla"], expType)
	
	// Do 10 puts
	for i := 1; i <= count; i++ {
        key := fmt.Sprintf("%04d", i)
		value := uuid.New().String()
        api.Put(s, key, value)
    }

	time.Sleep(2 * time.Second)

	// Do 10 reads
	get_sla := GlobalSLAs["psw_sla"]
	
	// Change this for testing purposes
	for i := 1; i <= count; i++ {
		key := fmt.Sprintf("%04d", i)
		val, subSLAGained, err := api.Get(s, key, &get_sla)
		if err != nil {
			fmt.Printf("Get error for key %s: %v (subSLAGained: %v)\n", key, err, subSLAGained)
		} else {
			fmt.Printf("Read key=%s, value=%s, subSLAGained=%v\n", key, string(val), subSLAGained)
		}
	}

	// Get the Utilities of the session
	fmt.Println(s.Utilities)

	// Terminate the session
	api.EndSession(s)

	return nil
}

func loadStaticSLAs() {
	var sla consistency.SLA
	var err error
	
	sla, err = util.LoadSLAFromFile("consistency/samples/password_checking.json", "psw_sla")
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

// Runs a ycsb worklaod as part of the client
// func ycsb_experiment() {

// }
