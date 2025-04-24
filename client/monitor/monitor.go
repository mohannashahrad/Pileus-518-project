package monitor

import (
	"sync"
	"time"
)

// Size of the sliding window
// TODO: what should be the size of the sliding window to be more reactive
const maxSamples = 100

// TODO: look at how the sliding window is implemented
type RTTWindow struct {
	samples []time.Duration
	index   int
	full    bool
	mu      sync.Mutex
}

// Monitor also needs a mutex on modifying the map of all nodes [map changing might not be thread-safe]
type Monitor struct {
	nodeRTTs map[string]*RTTWindow 	// Map of node -> RTT window
	nodeHTS map[string]*int64 		// Map of node -> High Timestamp
	mu   sync.RWMutex
}

var globalMonitor = &Monitor{
	nodeRTTs: make(map[string]*RTTWindow),
	nodeHTS: make(map[string]*int64),
}

// RecordRTT is called by the API layer to track RTTs.
func RecordRTT(node string, rtt time.Duration) {
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