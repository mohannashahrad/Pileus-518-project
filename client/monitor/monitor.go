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
	data map[string]*RTTWindow // Map of node -> RTT window
	mu   sync.RWMutex
}

var globalMonitor = &Monitor{
	data: make(map[string]*RTTWindow),
}

// RecordRTT is called by the API layer to track RTTs.
func RecordRTT(node string, rtt time.Duration) {
	globalMonitor.mu.Lock()
	defer globalMonitor.mu.Unlock()

	window, exists := globalMonitor.data[node]
	
	// If it does not exist then make a window for the node
	if !exists {
		window = &RTTWindow{samples: make([]time.Duration, maxSamples)}
		globalMonitor.data[node] = window
	}

	window.mu.Lock()
	defer window.mu.Unlock()

	window.samples[window.index] = rtt
	window.index = (window.index + 1) % maxSamples
	if window.index == 0 {
		window.full = true
	}
}

// GetRTTs returns a copy of the RTT samples for a node
func GetRTTs(node string) []time.Duration {
	globalMonitor.mu.RLock()
	window, exists := globalMonitor.data[node]
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
