package util

import (
	"errors"
	"os"
	"encoding/json"
	"regexp"
	"strconv"
)

type Record struct {
	Key       string    `json:"key"`
	Value     any       `json:"value"`
	Timestamp int64
}

// All data shards that the node is primary or secondary for are stored as shards
type Shard struct {
	ShardId    int `json:"id"`
	RangeStart int `json:"start"`
	RangeEnd   int `json:"end"`
	Primary	string `json:"primary"` 
	PrimaryID string `json:"primaryID"` 
	Secondaries []string `json:"secondaryIDs"`
	ReplicationFrequencySeconds float64  `json:"defaultRepFreq"`
	AmIPrimary bool
	AmISecondary bool
	HighTS  int64 // highest known timestamp from this shard
}

type Config struct {
	Shards []Shard `json:"shards"`
}

type ShardHighTSSnapshot struct {
	Shards map[int]int64   `json:"shards"`
}

var keyNumericRegex = regexp.MustCompile(`\d+`)

// CheckKeyAndValue returns an error if k == "" or if v == nil
func CheckKeyAndValue(k string, v any) error {
	if err := CheckKey(k); err != nil {
		return err
	}
	return CheckVal(v)
}

// CheckKey returns an error if k == ""
func CheckKey(k string) error {
	if k == "" {
		return errors.New("The passed key is an empty string, which is invalid")
	}
	return nil
}

// CheckVal returns an error if v == nil
func CheckVal(v any) error {
	if v == nil {
		return errors.New("The passed value is nil, which is not allowed")
	}
	return nil
}

func KeyToInt(k string) (int, error) {
	match := keyNumericRegex.FindString(k)
	if match == "" {
		return 0, errors.New("no numeric part found in key")
	}
	return strconv.Atoi(match)
}

func LoadConfig(path string) (*Config, error) { 
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func Contains(s []string, e string) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}
