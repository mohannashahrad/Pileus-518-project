package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
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
	generateRMWWorkload("Fig11/skewed_rmw.log", 1000, 1343)
	generateRMWWorkload("monotonic.log", 100, 1344)

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

func generateRMWWorkload(filename string, keySpaceSize int, seed int64) {
	var numOperations = 2000
	var sessionSize = 400
	var rmwRatio = 0.6

	r := rand.New(rand.NewSource(seed))
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("failed to create file: %v\n", err)
		return
	}
	defer file.Close()

	for session := 0; session < numOperations/sessionSize; session++ {
		ops := make([]string, sessionSize)
		occupied := make([]bool, sessionSize)

		numReads := sessionSize / 2
		numWrites := sessionSize - numReads
		rmwCount := int(float64(numWrites) * rmwRatio)

		writeIndices := make([]int, 0, numWrites)
		writeKeys := make([]int, 0, numWrites)
		writeValues := make([]string, 0, numWrites)

		// 1. Generate and place writes randomly
		for i := 0; i < numWrites; i++ {
			var index int
			for {
				index = r.Intn(sessionSize)
				if !occupied[index] {
					break
				}
			}
			key := r.Intn(keySpaceSize)
			val := uuid.New().String()
			formattedKey := fmt.Sprintf("%04d", key)

			ops[index] = fmt.Sprintf("WRITE %s %s", formattedKey, val)
			occupied[index] = true

			writeIndices = append(writeIndices, index)
			writeKeys = append(writeKeys, key)
			writeValues = append(writeValues, val)
		}

		// 2. Place reads for a subset of writes (RMW reads), within 6 positions after the write
		usedReadSlots := map[int]bool{}
		for i := 0; i < rmwCount; i++ {
			writeIdx := writeIndices[i]
			key := writeKeys[i]
			formattedKey := fmt.Sprintf("%04d", key)

			var readIdx int
			for attempts := 0; attempts < 10; attempts++ {
				offset := r.Intn(6) + 1 // 1 to 6
				readIdx = writeIdx + offset
				if readIdx < sessionSize && !occupied[readIdx] && !usedReadSlots[readIdx] {
					break
				}
			}

			if readIdx < sessionSize && !occupied[readIdx] {
				ops[readIdx] = fmt.Sprintf("READ %s", formattedKey)
				occupied[readIdx] = true
				usedReadSlots[readIdx] = true
			}
		}

		// 3. Fill in remaining reads with random keys
		for i := 0; i < sessionSize; i++ {
			if !occupied[i] {
				key := r.Intn(keySpaceSize)
				formattedKey := fmt.Sprintf("%04d", key)
				ops[i] = fmt.Sprintf("READ %s", formattedKey)
				occupied[i] = true
			}
		}

		// 4. Write session to file
		for _, op := range ops {
			fmt.Fprintln(file, op)
		}
	}

	fmt.Printf("Workload saved to %s\n", filename)
}

func generateMonotonicReadWorkload(filename string, seed int64) {
	var numOperations = 2000
	var sessionSize = 400
	var keySpaceSize = 1000
	var monotonicRatio = 0.6 // 60% of reads will stress the same key

	r := rand.New(rand.NewSource(seed))
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("failed to create file: %v\n", err)
		return
	}
	defer file.Close()

	for session := 0; session < numOperations/sessionSize; session++ {
		ops := make([]string, sessionSize)
		occupied := make([]bool, sessionSize)

		numReads := sessionSize / 2
		numWrites := sessionSize - numReads

		// Choose a key to stress with repeated reads and interleaved writes
		monotonicKey := r.Intn(keySpaceSize)
		formattedKey := fmt.Sprintf("%04d", monotonicKey)
		repeatedReadCount := int(float64(numReads) * monotonicRatio)

		// Step 1: Assign interleaved reads and writes for the monotonic key
		readWriteIndices := make([]int, 0)
		for i := 0; i < repeatedReadCount; i++ {
			var idx int
			for {
				idx = r.Intn(sessionSize)
				if !occupied[idx] {
					break
				}
			}
			readWriteIndices = append(readWriteIndices, idx)
		}
		// Sort to simulate time progression (e.g., read, write, read, write...)
		sort.Ints(readWriteIndices)
		readIsNext := true
		for _, idx := range readWriteIndices {
			if readIsNext {
				ops[idx] = fmt.Sprintf("READ %s", formattedKey)
			} else {
				val := uuid.New().String()
				ops[idx] = fmt.Sprintf("WRITE %s %s", formattedKey, val)
			}
			occupied[idx] = true
			readIsNext = !readIsNext
		}

		// Step 2: Fill remaining writes randomly (non-monotonic)
		for i := 0; i < numWrites-repeatedReadCount/2; i++ {
			var index int
			for {
				index = r.Intn(sessionSize)
				if !occupied[index] {
					break
				}
			}
			key := r.Intn(keySpaceSize)
			val := uuid.New().String()
			formatted := fmt.Sprintf("%04d", key)
			ops[index] = fmt.Sprintf("WRITE %s %s", formatted, val)
			occupied[index] = true
		}

		// Step 3: Fill remaining reads randomly
		for i := 0; i < sessionSize; i++ {
			if !occupied[i] {
				key := r.Intn(keySpaceSize)
				formatted := fmt.Sprintf("%04d", key)
				ops[i] = fmt.Sprintf("READ %s", formatted)
				occupied[i] = true
			}
		}

		// Step 4: Write the session to file
		for _, op := range ops {
			fmt.Fprintln(file, op)
		}
	}

	fmt.Printf("Monotonic-read workload saved to %s\n", filename)
}
