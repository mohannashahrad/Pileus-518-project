package util

import (
	"errors"
	"os"
	"encoding/json"
	"regexp"
	"strconv"
)

type Shard struct {
	RangeStart int `json:"start"`
	RangeEnd   int `json:"end"`
	Primary	string `json:"primary"` 
	PrimaryID string `json:"primaryID"` 
}

type Config struct {
	Shards []Shard `json:"shards"`
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
