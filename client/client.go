package main

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/google/uuid"
	"client/consistency"
	"client/monitor"
	"client/util"
	"client/api"
	"bufio"
	"os"
	"strings"
)

type Record struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

type DynamicReconfigurationConfig struct {
	ClientID      string `json:"client_id"`
	Region        string `json:"region"`
	CoordinatorURL string `json:"reconfiguration_coordinator_url"`
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

	// Load node info for reconfiguration
	configuration_config, err := loadClientConfigByRegion("clients_config.json", "clem")
	if err != nil {
		fmt.Println("Failed to load client config: %v\n", err)
	}

	// Before sending the workloads, send monitoring probes to the nodes to get RTT 
	// TODO: we can use probes also for HighTS to begin [for each shard]
	api.SendProbes()

	fmt.Println("Checking the RTT's after sending init probes\n")
	api.PrintRTTs()

	// Start periodic monitoring report [every 10 seconds]
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			monitor.SendUtilityReport(
				configuration_config.ClientID,
				configuration_config.Region,
				GlobalSLAs["cart_sla"],				// change this based on the SLA testing
				configuration_config.CoordinatorURL,
			)
		}
	}()

	start := time.Now()

	// ================ Password Checking Experiemnts ================== 

	// Adjust the workload based on the exp type
	// replay_workload_from_log("ycsb/Fig12/2k/<name of the log>.log", util.Pileus, "psw_sla")
	// replay_workload_from_log("ycsb/Fig12/2k/<name of the log>.log", util.Random, "psw_sla")
	// replay_workload_from_log("ycsb/Fig12/2k/<name of the log>.log", util.Primary, "psw_sla")
	// replay_workload_from_log("ycsb/Fig12/2k/<name of the log>.log", util.Closest, "psw_sla")


	// ================ Shopping Cart Experiemnts ====================== 

	// Adjust the workload based on the exp type
	// replay_workload_from_log("ycsb/Fig11/2k/r50w50sec2.log", util.Pileus, "cart_sla")
	//replay_workload_from_log("ycsb/Fig11/skewed_rmw_test.log", util.Random, "cart_sla")

	// ================ Adaptabiliy to Network Latency Experiemnts ================== 
	// replay_workload_with_artificial_latency("ycsb/Fig13/utahClient.log", util.Pileus, "psw_sla")

	// ================ Dynamic Reconfiguration Experiemnts ================== 
	replay_workload_from_log("ycsb/Fig11/dynamic_reconfig.log", util.Pileus, "psw_sla")

	duration := time.Since(start)
	fmt.Printf("Workload execution took %v\n", duration)
}

func replay_workload_from_log(workloadFile string, expType util.ServerSelectionPolicy, slaName string) error {
	// 400 operations per session
	const opsPerSession = 400
	var avgUtilityList []float64

	file, err := os.Open(workloadFile)
	if err != nil {
		return fmt.Errorf("failed to open workload file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var ops []string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) != "" {
			ops = append(ops, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read workload file: %v", err)
	}

	fmt.Printf("Read %d operations from log\n", len(ops))

	// Replay operations in sessions of 400 ops each
	sla := GlobalSLAs[slaName]
	for i := 0; i < len(ops); i += opsPerSession {
		end := i + opsPerSession
		if end > len(ops) {
			end = len(ops)
		}

		// Start session
		s := api.BeginSession(&sla, expType)

		for _, op := range ops[i:end] {
			parts := strings.Fields(op)
			if len(parts) < 2 {
				fmt.Printf("Skipping malformed line: %s\n", op)
				continue
			}

			switch parts[0] {
			case "READ":
				key := parts[1]
				fmt.Printf("Do a READ\n")
				val, subSLAGained, err := api.Get(s, key, &sla)
				if err != nil {
					fmt.Printf("Get error for key %s: %v (subSLAGained: %v)\n", key, err, subSLAGained)
				} else {
					fmt.Printf("Read key=%s, value=%s, subSLAGained=%v\n", key, string(val), subSLAGained)
				}
			case "WRITE":
				if len(parts) < 3 {
					fmt.Printf("Skipping malformed WRITE line: %s\n", op)
					continue
				}
				key := parts[1]
				value := parts[2]
				fmt.Printf("Do a WRITE\n")
				api.Put(s, key, value)
			default:
				fmt.Printf("Unknown operation type: %s\n", parts[0])
			}
		}

		fmt.Println("Session complete. Utilities:", s.Utilities)
		var sessionAvg float64
		if len(s.Utilities) > 0 {
			var sum float64
			for _, u := range s.Utilities {
				sum += u
			}
			sessionAvg = sum / float64(len(s.Utilities))
		}

		avgUtilityList = append(avgUtilityList, sessionAvg)

		api.EndSession(s)
	}

	// Report the avg of all session utilities
	if len(avgUtilityList) > 0 {
		var sum float64
		for _, vg := range avgUtilityList {
			sum += vg
		}
		avg := sum / float64(len(avgUtilityList))
		fmt.Printf("Average vg across %d sessions: %f\n", len(avgUtilityList), avg)
	}

	return nil
}

func replay_workload_with_artificial_latency(workloadFile string, expType util.ServerSelectionPolicy, slaName string) error {
	// 400 operations per session
	const opsPerSession = 400
	var avgUtilityList []float64
	session_counter := 0

	file, err := os.Open(workloadFile)
	if err != nil {
		return fmt.Errorf("failed to open workload file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var ops []string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) != "" {
			ops = append(ops, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read workload file: %v", err)
	}

	fmt.Printf("Read %d operations from log\n", len(ops))

	// Replay operations in sessions of 400 ops each
	sla := GlobalSLAs[slaName]
	for i := 0; i < len(ops); i += opsPerSession {
		end := i + opsPerSession
		if end > len(ops) {
			end = len(ops)
		}

		// Start session
		session_counter++

		// Adjust artifical latency based on the session_counter
		if (session_counter == 4) {
			// Add lag to RTT of primary 
			fmt.Printf("Session %d: Injecting 60ms latency to clem\n", session_counter)
			api.SetArtificialLat("clem", 60*time.Millisecond)
		}
		if (session_counter == 8) {
			// Add lag to RTT of local
			fmt.Printf("Session %d: Injecting 60ms latency to utah\n", session_counter)
			api.SetArtificialLat("utah", 60*time.Millisecond)
			
		}
		if (session_counter == 12) {
			// Make local RTT normal
			fmt.Printf("Session %d: Removing latency from utah\n", session_counter)
			api.SetArtificialLat("utah", 0*time.Millisecond)
			
		}
		if (session_counter == 16) {
			// Make primary RTT normal
			fmt.Printf("Session %d: Removing latency from clem\n", session_counter)
			api.SetArtificialLat("clem", 0*time.Millisecond)
		}

		s := api.BeginSession(&sla, expType)

		for _, op := range ops[i:end] {
			parts := strings.Fields(op)
			if len(parts) < 2 {
				fmt.Printf("Skipping malformed line: %s\n", op)
				continue
			}

			switch parts[0] {
			case "READ":
				key := parts[1]
				fmt.Printf("Do a READ\n")
				val, subSLAGained, err := api.Get(s, key, &sla)
				if err != nil {
					fmt.Printf("Get error for key %s: %v (subSLAGained: %v)\n", key, err, subSLAGained)
				} else {
					fmt.Printf("Read key=%s, value=%s, subSLAGained=%v\n", key, string(val), subSLAGained)
				}
			case "WRITE":
				if len(parts) < 3 {
					fmt.Printf("Skipping malformed WRITE line: %s\n", op)
					continue
				}
				key := parts[1]
				value := parts[2]
				fmt.Printf("Do a WRITE\n")
				api.Put(s, key, value)
			default:
				fmt.Printf("Unknown operation type: %s\n", parts[0])
			}
		}

		fmt.Println("Session complete. Utilities:", s.Utilities)
		var sessionAvg float64
		if len(s.Utilities) > 0 {
			var sum float64
			for _, u := range s.Utilities {
				sum += u
			}
			sessionAvg = sum / float64(len(s.Utilities))
		}

		avgUtilityList = append(avgUtilityList, sessionAvg)

		api.EndSession(s)
	}

	// Report the avg of all session utilities
	if len(avgUtilityList) > 0 {
		var sum float64
		for _, vg := range avgUtilityList {
			sum += vg
		}
		avg := sum / float64(len(avgUtilityList))
		fmt.Printf("Average vg across %d sessions: %f\n", len(avgUtilityList), avg)
	}

	return nil
}

// Start a session and do a bunch of puts in the same session
func password_checking_putWorkload(count int, expType util.ServerSelectionPolicy) error {
	fmt.Printf("Entered the putworkalod function\n")

	// Start the session
	// We set the type of the exp in the session
	psw_sla := GlobalSLAs["psw_sla"]
	s := api.BeginSession(&psw_sla, expType)
	
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
}

func loadClientConfigByRegion(path, region string) (*DynamicReconfigurationConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %w", err)
	}
	defer file.Close()

	// Parse the entire config as a map from region name to config struct
	var allConfigs map[string]DynamicReconfigurationConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&allConfigs); err != nil {
		return nil, fmt.Errorf("could not decode config JSON: %w", err)
	}

	// Lookup the one we want
	config, ok := allConfigs[region]
	if !ok {
		return nil, fmt.Errorf("region %q not found in config", region)
	}
	return &config, nil
}