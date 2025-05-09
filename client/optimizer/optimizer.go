package optimizer

import (
	"fmt"
	"client/consistency"
	"client/util"
	"client/monitor"
	"time"
	"strconv"
)

type SubUtility struct {
	Utility float32
	Node    string
}

var replicationConfig *util.ReplicationConfig

// This is called from the client-side api/lib to init the replication data on the optimizer side
func Init(config *util.ReplicationConfig) {
	replicationConfig = config
	fmt.Printf("Optimizor: Replication Config is Set to %v\n", replicationConfig)
}

// FindNodeToRead selects the node with the highest utility for the given key and SLA
// The last return value is the list of min_read_timestamp for all sub_sla's [used for utility calculation] 
func FindNodeToRead(s *util.Session, key string, sla *consistency.SLA) (string, consistency.SubSLA, []int64) {

	var chosenNode string
	var chosenSubSLA consistency.SubSLA
	var minTSPerSubSLA []int64	// this holds the minReadTimestamp for each sub-SLA

	maxUtility := float32(-1)

	for _, sub := range sla.SubSLAs {
		subUtility, minReadTS := ComputeUtilityForSubSLA(s, key, &sub)
		minTSPerSubSLA = append(minTSPerSubSLA, minReadTS)

		// TODO: handle stale nodes [when the utility is zero] [could be an optimization]
		// if subUtility.Utility <= 0 { ... }

		if subUtility.Utility > maxUtility {
			maxUtility = subUtility.Utility
			chosenSubSLA = sub
			chosenNode = subUtility.Node
		}
	}

	return chosenNode, chosenSubSLA, minTSPerSubSLA
}

// Returns the best node for a given SubSLA
func ComputeUtilityForSubSLA(s *util.Session, key string, sub *consistency.SubSLA) (SubUtility, int64) {
	var chosen string
	var maxProb float64 = -1

	// Only filter those nodes that satisfy the consistency
	nodes, minReadTS := SelectNodesForConsistency(s, key, sub.Consistency, sub.StalenessBound)

	for _, node := range nodes {
		prob := monitor.ProbabilityOfRTTBelow(node, sub.Latency.Duration, true) // the last input to the function is being optmistic in the probability calculation

		if prob > maxProb {
			maxProb = prob
			chosen = node
		} else if prob == maxProb {	// Break ties with lower average RTT
			if monitor.GetAvgRTT(node) < monitor.GetAvgRTT(chosen) {
				chosen = node
			}
		}
	}

	// calculate utility of the sub-sla = weight * probability of meeting latency goal
	utility := float32(sub.Utility * maxProb)

	return SubUtility{
		Utility: utility,
		Node:    chosen,
	}, minReadTS
}

// returns nodes that can serve a given consistency requirement
func SelectNodesForConsistency(session *util.Session, key string, level consistency.ConsistencyLevel, bound *time.Duration) ([]string, int64) {
	var selected []string
	var minReadTS int64

	// TODO: implement the helper functions for other consistency levels below
	switch level {
		case consistency.Strong:
			selected = append(selected, SelectNodesForStrongConsistency(key)...)

			// For strong consistency, minReadTS is not defined per client [we always go to primary]
			// TODO: Is this right?
			minReadTS = -1 

		// last preceding Put(key) in the same session
		case consistency.ReadMyWrites:
			nodes, requiredReadTS := SelectNodesForReadMyWrites(session, key)
			selected = append(selected, nodes...)
			minReadTS = requiredReadTS
		
		// last preceding GET(key) in the same session
		case consistency.MonotonicReads:
			nodes, requiredReadTS := SelectNodesForMonotonicReads(session, key)
			selected = append(selected, nodes...)
			minReadTS = requiredReadTS

		case consistency.Bounded:
			nodes, requiredReadTS := SelectNodesForBoundedStaleness(session, key, bound)
			selected = append(selected, nodes...)
			minReadTS = requiredReadTS

		case consistency.Eventual:
			selected = append(selected, SelectNodesForEventualConsistency(key)...)
			minReadTS = 0.0

		default:
			selected = append(selected, SelectNodesForStrongConsistency(key)...)
			minReadTS = -1 
	}

	return selected, minReadTS
}

// Always return the primary for the key
func SelectNodesForStrongConsistency(key string) []string {
	var selected []string

	numericKey, err := strconv.Atoi(key)
	if err != nil {
		fmt.Printf("Error: Could not convert key to numeric value.")
		return selected
	}

	// Find the primary for the key and return
	for _, shard := range replicationConfig.Shards {
		if (numericKey >= shard.RangeStart && numericKey <= shard.RangeEnd) {
			selected = append(selected, shard.Primary)
			return selected
		}
	}

	// If no shard found, return empty
	fmt.Println("Error: Did not find a shard which the key belongs to!")
	return selected
}

// Return all storage nodes
func SelectNodesForEventualConsistency(key string) []string {
	var selected []string

	// Add all storage nodes addresses
	for _, node := range replicationConfig.Nodes {
			selected = append(selected, node.Address)
		}
	return selected
}

// Nodes that have value written by the last preceding Put(key) in the same session
func SelectNodesForReadMyWrites(session *util.Session, key string) ([]string, int64) {
	fmt.Printf("entered SelectNodesForReadMyWrites \n")
	var selected []string
	var minHighTS int64

	// Get the last time key was written in this session
	if ts, ok := session.ObjectsWritten[key]; ok {
		minHighTS = ts
	} else {
		minHighTS = 0
	}
	fmt.Printf("Last write is %d\n",  session.ObjectsWritten[key])
	fmt.Printf("minHighTS is set to %d \n", minHighTS)

	numericKey, err := strconv.Atoi(key)
	if err != nil {
		fmt.Printf("Error: Could not convert key to numeric value.\n")
		return selected, minHighTS
	}

	primary := ""
	// Add primary of shard
	for _, shard := range replicationConfig.Shards {
		if numericKey >= shard.RangeStart && numericKey <= shard.RangeEnd {
			primary = shard.Primary
			selected = append(selected, primary)
			break
		}
	}

	// Also add secondaries that are sufficiently up-to-date
	for _, node := range replicationConfig.Nodes {
		if node.Address == primary {
			continue 
		}

		highTS := monitor.GetHTS(node.Address)
		fmt.Printf("Node highTS is %d \n", highTS)

		if highTS >= minHighTS {
			selected = append(selected, node.Address)
		}
	}

	fmt.Printf("returnung the nodes %v\n", selected)
	return selected, minHighTS
}

func SelectNodesForMonotonicReads(session *util.Session, key string) ([]string, int64) {
	fmt.Printf("entered SelectNodesForMonotonicReads \n")
	var selected []string
	var minHighTS int64

	// Get the last time key was read in this session
	if ts, ok := session.ObjectsRead[key]; ok {
		minHighTS = ts
	} else {
		minHighTS = 0
	}
	fmt.Printf("Last read is %d\n",  session.ObjectsRead[key])
	fmt.Printf("minHighTS is set to %d \n", minHighTS)

	numericKey, err := strconv.Atoi(key)
	if err != nil {
		fmt.Printf("Error: Could not convert key to numeric value.\n")
		return selected, minHighTS
	}

	primary := ""
	// Add primary of shard
	for _, shard := range replicationConfig.Shards {
		if numericKey >= shard.RangeStart && numericKey <= shard.RangeEnd {
			primary = shard.Primary
			selected = append(selected, primary)
			break
		}
	}

	// Also add secondaries that are sufficiently up-to-date
	for _, node := range replicationConfig.Nodes {
		if node.Address == primary {
			continue 
		}

		highTS := monitor.GetHTS(node.Address)
		fmt.Printf("Node highTS is %d \n", highTS)

		if highTS >= minHighTS {
			selected = append(selected, node.Address)
		}
	}

	fmt.Printf("returnung the nodes %v\n", selected)
	return selected, minHighTS
}

// The input bound is in Milliseconds
func SelectNodesForBoundedStaleness(session *util.Session, key string, bound *time.Duration) ([]string, int64) {
	fmt.Printf("entered SelectNodesForBoundedStaleness \n")
	var selected []string
	var minHighTS int64

	// Get the last time key was written in this session
	minHighTS = time.Now().Add(-*bound).UnixMilli()

	fmt.Println("Curr time is", time.Now().UnixMilli())
	fmt.Printf("minHighTS is set to %d \n", minHighTS)

	numericKey, err := strconv.Atoi(key)
	if err != nil {
		fmt.Printf("Error: Could not convert key to numeric value.\n")
		return selected, minHighTS
	}

	primary := ""
	// Add primary of shard
	for _, shard := range replicationConfig.Shards {
		if numericKey >= shard.RangeStart && numericKey <= shard.RangeEnd {
			primary = shard.Primary
			selected = append(selected, primary)

			primaryHighTS := monitor.GetHTS(primary)
			fmt.Printf("Primary highTS is %d \n", primaryHighTS)
			break
		}
	}


	// Also add secondaries that are sufficiently up-to-date
	for _, node := range replicationConfig.Nodes {
		if node.Address == primary {
			continue 
		}

		highTS := monitor.GetHTS(node.Address)
		fmt.Printf("Node highTS is %d \n", highTS)

		if highTS >= minHighTS {
			selected = append(selected, node.Address)
		}
	}

	fmt.Printf("returnung the nodes %v\n", selected)
	return selected, minHighTS
}