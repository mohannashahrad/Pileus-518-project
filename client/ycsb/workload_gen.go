package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/google/uuid"
)

func main() {
	// Password-checking SLA Workloads
	YCSB_workload_gen("Fig12/2k/r50w50primary.log", 2000, 10000, 0.5, 1337)
	YCSB_workload_gen("Fig12/2k/r50w50sec1.log", 2000, 10000, 0.5, 1338)
	YCSB_workload_gen("Fig12/2k/r50w50sec2.log", 2000, 10000, 0.5, 1339)
	YCSB_workload_gen("Fig12/2k/r50w50client.log", 2000, 10000, 0.5, 1340)

	// Workload for artificial latency experiment
	YCSB_workload_gen("Fig13/utahClient.log", 8000, 10000, 0.5, 1341)

	// Skewed worklaod for ReadMyWrites Testing
	Skewed_workload_gen("Fig11/skewed_rmw_test.log", 2000, 10000, 0.5, 0.7, 1343)

}

// Generates a YCSB-like workload, tunable with
	// size = Number of operations in the worklaod
	// Size of the keyspace
	// Proportion of the read operations (Default for Eval = 50%)
func YCSB_workload_gen(workload_name string, size int, keySpace int, readProportion float32, seed int64) error {
	fmt.Printf("Generating workload of size %d with read proportion %f and %d unique keys\n", size, readProportion, keySpace)

	f, err := os.Create(workload_name)
	if err != nil {
		return fmt.Errorf("could not create workload log file: %w", err)
	}
	defer f.Close()

	r := rand.New(rand.NewSource(seed))

	for i := 0; i < size; i++ {
		key := fmt.Sprintf("%04d", r.Intn(keySpace))

		if r.Float32() < readProportion {
			_, err := fmt.Fprintf(f, "READ %s\n", key)
			if err != nil {
				return fmt.Errorf("failed to write read op: %w", err)
			}
		} else {
			value := uuid.New().String()
			_, err := fmt.Fprintf(f, "WRITE %s %s\n", key, value)
			if err != nil {
				return fmt.Errorf("failed to write write op: %w", err)
			}
		}
	}

	fmt.Println("Workload written to %s", workload_name)
	return nil
}

func Skewed_workload_gen(workload_name string, size int, keySpace int, readProportion float32, readAfterWriteFraction float32, seed int64) error {
	
	fmt.Printf("Generating workload of size %d with read proportion %f and %d unique keys\n", size, readProportion, keySpace)

	f, err := os.Create(workload_name)
	if err != nil {
		return fmt.Errorf("could not create workload log file: %w", err)
	}
	defer f.Close()

	r := rand.New(rand.NewSource(seed))

	numReads := int(float32(size) * readProportion)
	numWrites := size - numReads
	numReadMyWrites := int(float32(numWrites) * readAfterWriteFraction)
	numRegularReads := numReads - numReadMyWrites

	ops := make([]string, 0, size)
	writeIndices := []int{}
	writeKeys := []string{}
	readMyWriteOps := []string{}

	// Step 1: Generate writes and record positions and keys
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("%04d", r.Intn(keySpace))
		value := uuid.New().String()
		ops = append(ops, fmt.Sprintf("WRITE %s %s\n", key, value))

		if len(writeKeys) < numReadMyWrites {
			writeIndices = append(writeIndices, len(ops)-1)
			writeKeys = append(writeKeys, key)
			readMyWriteOps = append(readMyWriteOps, fmt.Sprintf("READ %s\n", key))
		}
	}

	// Step 2: Generate regular random reads
	for i := 0; i < numRegularReads; i++ {
		key := fmt.Sprintf("%04d", r.Intn(keySpace))
		ops = append(ops, fmt.Sprintf("READ %s\n", key))
	}

	// Step 3: Place read-my-writes
	earlyReads := numReadMyWrites / 3
	remainingReads := []string{}

	for i, readOp := range readMyWriteOps {
		if i < earlyReads {
			// Insert within 1 to 10 ops after the write
			insertAfter := writeIndices[i] + 1 + r.Intn(10)
			if insertAfter > len(ops) {
				insertAfter = len(ops)
			}
			ops = append(ops[:insertAfter], append([]string{readOp}, ops[insertAfter:]...)...)
			// Update indices for any remaining ops
			for j := i + 1; j < len(writeIndices); j++ {
				if writeIndices[j] >= insertAfter {
					writeIndices[j]++
				}
			}
		} else {
			// Save for random shuffling later
			remainingReads = append(remainingReads, readOp)
		}
	}

	// Step 4: Shuffle and insert remaining read-my-writes randomly
	r.Shuffle(len(remainingReads), func(i, j int) {
		remainingReads[i], remainingReads[j] = remainingReads[j], remainingReads[i]
	})
	for _, readOp := range remainingReads {
		insertPos := r.Intn(len(ops) + 1)
		ops = append(ops[:insertPos], append([]string{readOp}, ops[insertPos:]...)...)
	}

	// Step 5: Write all operations to file
	for _, op := range ops {
		_, err := f.WriteString(op)
		if err != nil {
			return fmt.Errorf("failed to write op: %w", err)
		}
	}

	fmt.Printf("Workload written to %s\n", workload_name)
	return nil
}