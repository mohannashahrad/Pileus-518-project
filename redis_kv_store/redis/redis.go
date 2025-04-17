package redis

import (
	"fmt"
	"context"
	"time"
	"github.com/redis/go-redis/v9"
	"pileus/encoding"
	"pileus/util"
)

// For incoming (k,v) pairs, we also store the timestamp/version
type VersionedValue struct {
	Value     any    `json:"value"`
	Timestamp int64 `json:"timestamp"`
}

var defaultTimeout = 2 * time.Second

// Client is a gokv.Store implementation for Redis.
type Client struct {
	c       *redis.Client
	timeOut time.Duration
	codec   encoding.Codec
	ShardRangeStart int
	ShardRangeEnd   int
}

// Set stores the given value for the given key.(The key must not be "" and the value must not be nil.)
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// It also checks weather the node is the primary for the given key
func (c Client) Set(k string, v any) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	// Check if the node is primary for the given key
	numericKey, err := util.KeyToInt(k)
	if err != nil {
		return fmt.Errorf("invalid key format: %v", err)
	}

	if numericKey < c.ShardRangeStart || numericKey > c.ShardRangeEnd {
		return fmt.Errorf("key '%s' with numeric value %d is out of shard range [%d, %d]",
			k, numericKey, c.ShardRangeStart, c.ShardRangeEnd)
	}

	record := VersionedValue{
		Value: v,
		Timestamp: time.Now().UTC().UnixNano() / int64(time.Millisecond),
	}

	data, err := c.codec.Marshal(record)
	if err != nil {
		return err
	}

	tctx, cancel := context.WithTimeout(context.Background(), c.timeOut)
	defer cancel()

	err = c.c.Set(tctx, k, string(data), 0).Err()
	if err != nil {
		return err
	}
	return nil
}

// This function is only called during the replication phase, where secondaries pull data from primaries
// NOTE: Here we don't check the key range constraints anymore, because this is only callled by secondary storage nodes (the inital puts from clients, do not hit this function)
func (c Client) SetVersioned(k string, vv VersionedValue) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	data, err := c.codec.Marshal(vv)
	if err != nil {
		return err
	}

	tctx, cancel := context.WithTimeout(context.Background(), c.timeOut)
	defer cancel()

	return c.c.Set(tctx, k, string(data), 0).Err()
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (c Client) Get(k string, v any) (found bool, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	tctx, cancel := context.WithTimeout(context.Background(), c.timeOut)
	defer cancel()

	dataString, err := c.c.Get(tctx, k).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	return true, c.codec.Unmarshal([]byte(dataString), v)
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (c Client) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	tctx, cancel := context.WithTimeout(context.Background(), c.timeOut)
	defer cancel()

	_, err := c.c.Del(tctx, k).Result()
	return err
}

func (c *Client) ScanUpdatedKeys(since time.Time) []util.Record {
	var updates []util.Record

	ctx, cancel := context.WithTimeout(context.Background(), c.timeOut)
	defer cancel()

	iter := c.c.Scan(ctx, 0, "*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		var vv VersionedValue
		found, err := c.Get(key, &vv)
		if err != nil || !found {
			continue
		}
		if vv.Timestamp > since.UnixMilli() {
			updates = append(updates, util.Record{
				Key:       key,
				Value:     vv.Value,
				Timestamp: vv.Timestamp,
			})
		}
	}
	if err := iter.Err(); err != nil {
		fmt.Printf("Error scanning keys: %v\n", err)
	}

	return updates
}

// Close closes the client.
// It must be called to release any open resources.
func (c Client) Close() error {
	return c.c.Close()
}

// Options are the options for the Redis client.
type Options struct {	
	Address string  		// Optional ("localhost:6379" by default).
	ShardRangeStart int
	ShardRangeEnd int
	Password string 		// Optional ("" by default).	
	DB int 					// Optional (0 by default).
	Timeout *time.Duration	// Optional (2 * time.Second by default).
	Codec encoding.Codec	// Optional (encoding.JSON by default).
}

// DefaultOptions is an Options object with default values.
// Address: "localhost:6379", Password: "", DB: 0, Timeout: 2 * time.Second, Codec: encoding.JSON
var DefaultOptions = Options{
	Address: "localhost:6379",
	ShardRangeStart: 0,
	ShardRangeEnd: 0,
	Timeout: &defaultTimeout,
	Codec:   encoding.JSON,
	// No need to set Password or DB because their Go zero values are fine for that.
}

// NewClient creates a new Redis client.
// You must call the Close() method on the client when you're done working with it.
func NewClient(options Options) (Client, error) {
	result := Client{}

	// Set default values
	if options.Address == "" {
		options.Address = DefaultOptions.Address
	}
	if options.ShardRangeStart == 0 {
		options.ShardRangeStart = DefaultOptions.ShardRangeStart
	}
	if options.ShardRangeEnd == 0 {
		options.ShardRangeEnd = DefaultOptions.ShardRangeEnd
	}
	if options.Timeout == nil {
		options.Timeout = DefaultOptions.Timeout
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	client := redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB,
	})

	tctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := client.Ping(tctx).Err()
	if err != nil {
		return result, err
	}

	result.c = client
	result.timeOut = *options.Timeout
	result.codec = options.Codec
	result.ShardRangeStart = options.ShardRangeStart
	result.ShardRangeEnd = options.ShardRangeEnd

	return result, nil
}
