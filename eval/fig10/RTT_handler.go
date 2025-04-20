package main

import (
	"fmt"
	"net/http"
	"time"
)

var nodes = map[string]string{
	"US_EAST": "http://<IP_US_EAST>:8080",
	"US_WEST": "http://<IP_US_WEST>:8080",
	"ASIA":    "http://<IP_ASIA>:8080",
	"EU":      "http://<IP_EU>:8080",
}

const currentNode = "US_EAST" // Change this on each EC2 instance
const pingCount = 5

func measureRTT(url string) (float64, error) {
	var total float64
	var success int

	client := http.Client{
		Timeout: 2 * time.Second,
	}

	for i := 0; i < pingCount; i++ {
		start := time.Now()
		resp, err := client.Get(url)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("Error pinging %s: %v\n", url, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			total += float64(elapsed.Milliseconds())
			success++
		}
	}

	if success == 0 {
		return 0, fmt.Errorf("no successful responses")
	}
	return total / float64(success), nil
}

func main() {
	fmt.Printf("Measuring RTT from %s\n", currentNode)

	for region, url := range nodes {
		if region == currentNode {
			continue
		}
		avgRTT, err := measureRTT(url)
		if err != nil {
			fmt.Printf("RTT to %s (%s): failed (%v)\n", region, url, err)
		} else {
			fmt.Printf("RTT to %s (%s): %.2f ms\n", region, url, avgRTT)
		}
	}
}