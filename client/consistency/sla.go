package consistency

import "time"

type ConsistencyLevel int

/*
	Eventual    	ConsistencyLevel = 0
	MonotonicReads  ConsistencyLevel = 1
	ReadMyWrites    ConsistencyLevel = 2
	Bounded 		ConsistencyLevel = 3
	Strong 			ConsistencyLevel = 4
*/
const (
	Eventual ConsistencyLevel = iota
	MonotonicReads
	ReadMyWrites
	Bounded
	Strong
)

// Maximum allowed latency.
type LatencyBound struct {
	Duration time.Duration
}

// A single consistency-latency-utility specification 
type SubSLA struct {
	Consistency ConsistencyLevel
	Latency     LatencyBound
	StalenessBound *time.Duration
	Utility     float64
}

// Ordered from most to least preferred
type SLA struct {
	ID      string
	SubSLAs []SubSLA
}