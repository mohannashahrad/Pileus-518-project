package monitor

import (
	"sync"
	"time"
	"fmt"
	"encoding/json"
	"client/consistency"
	"bytes"
	"net/http"
)

// Size of the sliding window
// TODO: what should be the size of the sliding window to be more reactive
const maxSamples = 100
const utilityDropThreshold = 0.6


type SubSLAStatus struct {
	Node   string `json:"node"`
	SubSLA consistency.SubSLA `json:"sub_sla"`
	Status string `json:"status"` // "Met", "Lat_Not_Met", or "Consistency_Not_Met"
}

type ReadStatus struct {
	Node   string `json:"node"`
	SubSLADetails []SubSLAStatus `json:"summary"`
}

type RTTWindow struct {
	samples []time.Duration
	index   int
	full    bool
	mu      sync.Mutex
}

type UtilityWindow struct {
	samples []float64
	index   int
	full    bool
	mu      sync.Mutex
}

type ReadStatusWindow struct {
	mu      sync.Mutex
	entries []ReadStatus
	index   int
	full    bool
}

// Monitor also needs a mutex on modifying the map of all nodes [map changing might not be thread-safe]
// Right now this is a one-per-client monitoring strategy.
type Monitor struct {
	nodeRTTs map[string]*RTTWindow 		// Map of node -> RTT window
	nodeHTS map[string]*int64 			// Map of node -> High Timestamp
	utilities *UtilityWindow
	readHistogram map[string]int
	
	// Config for coordinator communication
	clientID       string
	region         string
	sla            consistency.SLA
	coordinatorURL string
	doCoordination bool
	lastUtilityReport time.Time

	mu   sync.RWMutex
}

var globalMonitor = &Monitor{
	nodeRTTs: make(map[string]*RTTWindow),
	nodeHTS: make(map[string]*int64),
	utilities: &UtilityWindow{samples: make([]float64, maxSamples)}, 
	readHistogram: make(map[string]int),
	lastUtilityReport: time.Time{},
}

func SetDynamicConfigData(clientID, region string, sla consistency.SLA, coordinatorURL string, doCoordination bool) {
	globalMonitor.mu.Lock()
	defer globalMonitor.mu.Unlock()

	globalMonitor.clientID = clientID
	globalMonitor.region = region
	globalMonitor.sla = sla
	globalMonitor.coordinatorURL = coordinatorURL
	globalMonitor.doCoordination = doCoordination
}

// RecordRTT is called by the API layer to track RTTs.
func RecordRTT(node string, rtt time.Duration) {
	fmt.Printf("Recording RTT: node=%s, rtt=%v\n", node, rtt)

	globalMonitor.mu.Lock()
	defer globalMonitor.mu.Unlock()

	window, exists := globalMonitor.nodeRTTs[node]
	
	// If it does not exist then make a window for the node
	if !exists {
		window = &RTTWindow{samples: make([]time.Duration, maxSamples)}
		globalMonitor.nodeRTTs[node] = window
	}

	window.mu.Lock()
	defer window.mu.Unlock()

	window.samples[window.index] = rtt
	window.index = (window.index + 1) % maxSamples
	if window.index == 0 {
		window.full = true
	}
}

func RecordHTS(node string, hts int64) {
	globalMonitor.mu.Lock()
	defer globalMonitor.mu.Unlock()

	globalMonitor.nodeHTS[node] = &hts
}

// Record the utility gained after communicating with a "storageNode"
func RecordUtility(utility float64) {
	globalMonitor.mu.Lock()
	window := globalMonitor.utilities
	clientID := globalMonitor.clientID
	region := globalMonitor.region
	active_sla := globalMonitor.sla
	coordinatorURL := globalMonitor.coordinatorURL
	globalMonitor.mu.Unlock()

	window.mu.Lock()
	window.samples[window.index] = utility
	window.index = (window.index + 1) % maxSamples
	if window.index == 0 {
		window.full = true
	}
	window.mu.Unlock()

	// Now safe to call GetAverageUtility (no lock is held)
	if (globalMonitor.doCoordination && GetAverageUtility() < utilityDropThreshold) {
		fmt.Println("utility dropped, take action")
		SendUtilityDropReport(clientID, region, active_sla, coordinatorURL)
	}
}

func RecordReadStatus(status ReadStatus) {
	globalMonitor.mu.Lock()
	
	keyBytes, err := json.Marshal(status)
	if err != nil {
		fmt.Println("Error marshaling ReadStatus:", err)
		return
	}
	key := string(keyBytes)

	globalMonitor.readHistogram[key]++

	globalMonitor.mu.Unlock()

	// PrintReadHistogram()
}

/*
	The following functions are getters of the data used in the routing algorithm
*/

// GetRTTs returns a copy of the RTT samples for a node
func GetRTTs(node string) []time.Duration {
	globalMonitor.mu.RLock()
	window, exists := globalMonitor.nodeRTTs[node]
	globalMonitor.mu.RUnlock()
	if !exists {
		return nil
	}

	window.mu.Lock()
	defer window.mu.Unlock()

	var result []time.Duration
	if window.full {
		result = append(result, window.samples[window.index:]...)
		result = append(result, window.samples[:window.index]...)
	} else {
		result = append(result, window.samples[:window.index]...)
	}
	return result
}

func GetHTS(node string) int64 {
	globalMonitor.mu.RLock()
	defer globalMonitor.mu.RUnlock()

	ptr, exists := globalMonitor.nodeHTS[node]
	if !exists || ptr == nil {
		return 0
	}
	return *ptr
}

func GetUtilities() []float64 {
	globalMonitor.mu.RLock()  
	defer globalMonitor.mu.RUnlock()

	window := globalMonitor.utilities

	window.mu.Lock() 
	defer window.mu.Unlock()

	var result []float64
	if window.full {
		result = append(result, window.samples[window.index:]...)
		result = append(result, window.samples[:window.index]...)
	} else {
		result = append(result, window.samples[:window.index]...)
	}
	return result
}

func GetAvgRTT(node string) time.Duration {
	globalMonitor.mu.RLock()
	window, exists := globalMonitor.nodeRTTs[node]
	globalMonitor.mu.RUnlock()
	if !exists {
		return 0 // Or some sentinel value like time.Duration(-1)
	}

	window.mu.Lock()
	defer window.mu.Unlock()

	var total time.Duration
	var count int

	if window.full {
		for _, sample := range window.samples {
			total += sample
		}
		count = maxSamples
	} else {
		for i := 0; i < window.index; i++ {
			total += window.samples[i]
		}
		count = window.index
	}

	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}

func GetRTTPerNode() map[string]float64 {
	rtts := make(map[string]float64)

	globalMonitor.mu.RLock()
	for node, window := range globalMonitor.nodeRTTs {
		window.mu.Lock()

		var total time.Duration
		var count int
		if window.full {
			for _, sample := range window.samples {
				total += sample
			}
			count = len(window.samples)
		} else {
			for i := 0; i < window.index; i++ {
				total += window.samples[i]
			}
			count = window.index
		}

		window.mu.Unlock()

		if count > 0 {
			avg := total / time.Duration(count)
			rtts[node] = float64(avg.Milliseconds())
		}
	}
	globalMonitor.mu.RUnlock()

	return rtts
}

func GetAverageUtility() float64 {
	globalMonitor.mu.Lock()
	defer globalMonitor.mu.Unlock()

	window := globalMonitor.utilities

	window.mu.Lock()
	defer window.mu.Unlock()

	var total float64
	var count int

	if window.full {
		count = len(window.samples)
	} else {
		count = window.index
	}

	for i := 0; i < count; i++ {
		total += window.samples[i]
	}

	if count == 0 {
		return 1.0
	}

	return total / float64(count)
}

func GetLowestAvgRTTNode() (string, time.Duration) {
	globalMonitor.mu.RLock()
	defer globalMonitor.mu.RUnlock()

	var minNode string
	var minRTT time.Duration = -1 // sentinel value to indicate uninitialized state

	for node, window := range globalMonitor.nodeRTTs {
		window.mu.Lock()

		var total time.Duration
		var count int

		if window.full {
			for _, sample := range window.samples {
				total += sample
			}
			count = maxSamples
		} else {
			for i := 0; i < window.index; i++ {
				total += window.samples[i]
			}
			count = window.index
		}

		window.mu.Unlock()

		if count == 0 {
			continue // skip nodes with no data
		}

		avgRTT := total / time.Duration(count)
		if minRTT < 0 || avgRTT < minRTT {
			minRTT = avgRTT
			minNode = node
		}
	}

	return minNode, minRTT
}

// TODO: this maybe won't capture the recent changes/increases to the RTT's
func ProbabilityOfRTTBelow(node string, threshold time.Duration, optimistic bool) float64 {
	globalMonitor.mu.RLock()
	window, exists := globalMonitor.nodeRTTs[node]
	globalMonitor.mu.RUnlock()

	// if RTT for the node doesn't exists yet, assume it is fast
	if !exists {
		if optimistic {
			return 1.0
		}

		// if no RTT avaialable and we are not optimistic, return 0
		return 0.0
	}

	window.mu.Lock()
	defer window.mu.Unlock()

	var total int
	var count int

	samples := window.samples
	limit := maxSamples
	if !window.full {
		limit = window.index
	}

	for i := 0; i < limit; i++ {
		if samples[i] <= threshold {
			count++
		}
		total++
	}

	// If no RTT in the window: same as above
	if total == 0 {
		if optimistic {
			return 1.0
		}
		return 0.0
	}

	// Return the proportion of RTT's less than threshold over all exisiting RTT's
	return float64(count) / float64(total)
}

func PrintReadHistogram() {
	globalMonitor.mu.RLock()
	defer globalMonitor.mu.RUnlock()

	fmt.Println("ReadStatus Histogram:")

	for k, count := range globalMonitor.readHistogram {
		var status ReadStatus
		if err := json.Unmarshal([]byte(k), &status); err != nil {
			fmt.Println("Error unmarshaling key:", err)
			continue
		}
		jsonBytes, _ := json.MarshalIndent(status, "", "  ")
		fmt.Printf("Count: %d\n%s\n\n", count, string(jsonBytes))
	}
}


// ================== Data Structure and Function to Communicate with Coordinator ========

type UtilityDropReport struct {
	ClientID    string                      `json:"client_id"`
	Region      string                      `json:"region"`
	AvgUtility  float64                     `json:"utility"`
	SLA         consistency.SLA             `json:"sla"`
	ReadHistogram   map[string]int          `json:"histogram"`
	RTTs            map[string]float64      `json:"rtts"`
}

func SendUtilityDropReport(clientID string, region string, sla consistency.SLA, coordinatorURL string) {
	globalMonitor.mu.Lock()

	now := time.Now()
	
	// Controlling how frequent we send this to the coordinator
	if now.Sub(globalMonitor.lastUtilityReport) < 2*time.Second {
		globalMonitor.mu.Unlock()
		fmt.Println("Skipped utility report due to rate limit")
		return
	}
	globalMonitor.lastUtilityReport = now

	// Clone data
	histCopy := make(map[string]int)
	for k, v := range globalMonitor.readHistogram {
		histCopy[k] = v
	}

	globalMonitor.utilities.mu.Lock()
	var sum float64
	var count int
	for _, u := range globalMonitor.utilities.samples {
		if u != 0 {
			sum += u
			count++
		}
	}
	globalMonitor.utilities.mu.Unlock()
	globalMonitor.mu.Unlock()

	avgUtility := 0.0
	if count > 0 {
		avgUtility = sum / float64(count)
	}

	report := UtilityDropReport{
		ClientID:   	clientID,
		Region:     	region,
		AvgUtility: 	avgUtility,
		SLA:        	sla,
		ReadHistogram:  histCopy,
		RTTs: 			GetRTTPerNode(),
	}

	payload, err := json.Marshal(report)
	if err != nil {
		fmt.Println("Failed to marshal utility drop report:", err)
		return
	}

	resp, err := http.Post(coordinatorURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println("Failed to send utility drop report:", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Utility drop report sent (status %d)\n", resp.StatusCode)
}
