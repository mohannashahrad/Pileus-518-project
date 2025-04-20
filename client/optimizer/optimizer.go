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
func FindNodeToRead(key string, sla *consistency.SLA) (string, consistency.SubSLA) {

	var chosenNode string
	var chosenSubSLA consistency.SubSLA
	maxUtility := float32(-1)

	for _, sub := range sla.SubSLAs {
		subUtility := ComputeUtilityForSubSLA(key, &sub)

		// TODO: handle stale nodes [when the utility is zero] -> Look at pileus code for this
		// if subUtility.Utility <= 0 { ... }

		if subUtility.Utility > maxUtility {
			maxUtility = subUtility.Utility
			chosenSubSLA = sub
			chosenNode = subUtility.Node
		}
	}

	return chosenNode, chosenSubSLA
}

// Returns the best node for a given SubSLA
func ComputeUtilityForSubSLA(key string, sub *consistency.SubSLA) SubUtility {
	var chosen string
	var maxProb float64 = -1

	// Only filter those nodes that satisfy the consistency
	nodes := SelectNodesForConsistency(key, sub.Consistency, sub.StalenessBound)

	// TODO: implement the RTT functions in the monitor
	for _, node := range nodes {
		// the last input to the function is being optmistic in the probability calculation [unless otherwise is known by witnessing high RTTs]
		prob := monitor.ProbabilityOfRTTBelow(node, sub.Latency.Duration, true)

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
	}
}

// returns nodes that can serve a given consistency requirement
func SelectNodesForConsistency(key string, level consistency.ConsistencyLevel, bound *time.Duration) []string {
	var selected []string

	// TODO: implement the helper functions for other consistency levels below
	switch level {
		case consistency.Strong:
			selected = append(selected, SelectNodesForStrongConsistency(key)...)

		// case consistency.ReadMyWrites:
		// 	selected = append(selected, SelectNodesForReadMyWrites(key))

		// case consistency.Bounded:
		// 	selected = append(selected, SelectNodesForBoundedStaleness(key))

		case consistency.Eventual:
			selected = append(selected, SelectNodesForEventualConsistency(key)...)

		default:
			selected = append(selected, SelectNodesForStrongConsistency(key)...)
	}

	return selected
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
