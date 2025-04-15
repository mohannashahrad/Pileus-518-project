package main

import (
	"fmt"
	"pileus/redis"
)

type foo struct {
	Bar string
}

func main() {
	options := redis.DefaultOptions 
	options.Address = "130.127.133.5:6379"

	// Create client
	client, err := redis.NewClient(options)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Store, retrieve, print and delete a value
	interactWithStore(client)
}

// interactWithStore stores, retrieves, prints and deletes a value.
// It's completely independent of the store implementation.
func interactWithStore(store redis.Client) {
	// Store value
	val := foo{
		Bar: "baz",
	}
	err := store.Set("foo1", val)
	if err != nil {
		panic(err)
	}

	// Retrieve value
	retrievedVal := new(foo)
	found, err := store.Get("foo1", retrievedVal)
	if err != nil {
		panic(err)
	}
	if !found {
		panic("Value not found")
	}

	fmt.Printf("foo: %+v\n", *retrievedVal) // Prints `foo: {Bar:baz}`

	// Delete value
	err = store.Delete("foo1")
	if err != nil {
		panic(err)
	}
}
