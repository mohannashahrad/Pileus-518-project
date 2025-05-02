package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/google/uuid"
)

func main() {
	YCSB_workload_gen("read50write50.log", 300000, 10000, 0.5)
}

// Generates a YCSB-like workload, tunable with
	// size = Number of operations in the worklaod
	// Size of the keyspace
	// Proportion of the read operations (Default for Eval = 50%)
func YCSB_workload_gen(workload_name string, size int, keySpace int, readProportion float32) error {
	fmt.Printf("Generating workload of size %d with read proportion %f and %d unique keys\n", size, readProportion, keySpace)

	f, err := os.Create(workload_name)
	if err != nil {
		return fmt.Errorf("could not create workload log file: %w", err)
	}
	defer f.Close()

	r := rand.New(rand.NewSource(1337))

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
