package main

import (
	"fmt"
	"time"
	// "math/rand"
	// "github.com/google/uuid"
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
	err = api.LoadReplicationConfig("../sharding_config.json")
	if err != nil {
		fmt.Printf("An error happened loading the replication configuration.\n")
		panic(err)
	}

	// Load the static SLAs
	var sla consistency.SLA
	
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

	fmt.Printf("Cart SLA: %+v\n", GlobalSLAs["cart_sla"])
	fmt.Printf("**************************************\n")
	fmt.Printf("Web Applicaiton SLA: %+v\n", GlobalSLAs["web_sla"])
	fmt.Printf("**************************************\n")
	fmt.Printf("Password SLA: %+v\n", GlobalSLAs["psw_sla"])

	password_checking_putWorkload(20)
}

// Start a session and do a bunch of puts in the same session
func password_checking_putWorkload(count int) error {
	fmt.Printf("Entered the putworkalod function\n")

	// Start the session
	s := api.BeginSession(GlobalSLAs["psw_sla"])
	
	// Randomly generate the key and value
	// for i := 1; i <= count; i++ {
    //     key := fmt.Sprintf("%04d", rand.Intn(1000))
	// 	value := uuid.New().String()
    //     api.Put(s, key, value)
    // }

	//simple test of strong reads [which should go to the primary]
	api.Put(s, "0001", "test1")
	
	// Change this for testing purposes
	get_sla := GlobalSLAs["read_my_write_sla"]

	val, cc, err := api.Get(s, "0001", &get_sla)
	if err != nil {
		fmt.Printf("Get error for key %s: %v (CC: %v)\n", "0001", err, cc)
	} else {
		fmt.Printf("Read key=%s, value=%s, CC=%v\n", "0001", string(val), cc)
	}

	time.Sleep(1 * time.Second)
	api.Put(s, "0002", "test2")

	val, cc, err = api.Get(s, "0003", &get_sla)
	if err != nil {
		fmt.Printf("Get error for key %s: %v (CC: %v)\n", "0001", err, cc)
	} else {
		fmt.Printf("Read key=%s, value=%s, CC=%v\n", "0001", string(val), cc)
	}
	
	// Terminate the session
	api.EndSession(s)

	return nil
}
