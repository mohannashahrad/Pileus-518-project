package optimizer

import (
	"client/consistency"
	// import "client/monitor" if you're going to ping timestamps later
)

type NodeUtility struct {
	Utility float32
	Node    string
}

// FindNodeToRead selects the node with the highest utility for the given key and SLA
func FindNodeToRead(key string, sla *consistency.SLA) (string, consistency.SubSLA) {

	var chosenNode string
	var chosenSubSLA consistency.SubSLA
	maxUtility := float32(-1)

	for _, sub := range sla.SubSLAs {
		nodeUtility := ComputeUtilityForSubSLA(key, &sub)

		// TODO: handle stale nodes [when the utility is zero] -> Look at pileus code for this
		// if nodeUtility.Utility <= 0 { ... }

		if nodeUtility.Utility > maxUtility {
			maxUtility = nodeUtility.Utility
			chosenSubSLA = sub
			chosenNode = nodeUtility.Node
		}
	}

	return chosenNode, chosenSubSLA
}

// Returns the best node for a given SubSLA
func ComputeUtilityForSubSLA(key string, sub *consistency.SubSLA) NodeUtility {
	var chosen string
	var maxProb float64 = -1

	// Only filter those nodes that satisfy the consistency
	nodes := SelectNodesForConsistency(key, sub.Consistency, sub.Bound)

	// TODO: implement the RTT functions in the monitor
	for _, node := range nodes {
		prob := monitor.ProbabilityOfRTTBelow(node, int64(sub.Latency))

		if prob > maxProb {
			maxProb = prob
			chosen = node
		} else if prob == maxProb {
			// Break ties with lower average RTT
			if monitor.getAvgRTT(node) < monitor.getAvgRTT(chosen) {
				chosen = node
			}
		}
	}

	// Utility = weight * probability of meeting latency goal
	utility := float32(sub.Utility * maxProb)

	return NodeUtility{
		Utility: utility,
		Node:    chosen.Node,
	}
}

// returns nodes that can serve a given consistency requirement
func SelectNodesForConsistency(key string, level consistency.Level, bound int) []string {
	var selected []string

	// TODO: Add other levels if needed
	// TODO: implement the helper functions below
	switch level {
		case consistency.Strong:
			selected = append(selected, SelectNodesForStrongConsistency(key))

		// case consistency.ReadMyWrites:
		// 	selected = append(selected, SelectNodesForReadMyWrites(key))

		// case consistency.Bounded:
		// 	selected = append(selected, SelectNodesForBoundedStaleness(key))

		case consistency.Eventual:
			selected = append(selected, SelectNodesForEventualConsistency(key))

		default:
			selected = append(selected, SelectNodesForStrongConsistency(key))
	}

	return selected
}

// Always return the primary for the key
func SelectServersForStrongConsistency(key string) []string {
	var selected []string

	// find the primary for the key and return
	// there should be a function somewhere that given a key return shard/primary for the key

	return selected
}

// Return all set of nodes
func SelectNodesForEventualConsistency(key string) []string {
	var selected []string
	return selected

	// TODO: in the config we should have a section where is all the storage nodes, and this should be loaded
}
