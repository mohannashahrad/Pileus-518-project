The EC@ servers are put in the following regions: 

1. US_EAST -> Virginia
2. UW_WEST -> California
3. ASIA -> Tokyo
4. EU -> Frankfurt

All instances are t2.micro for this expriment as we just want the RTT. 
On each node install go and make sure port 8080 is open in the inbound rules of the instance.

Result is: 

- EAST - WEST:  144.80, 146.20 [Avg: 145.5ms]
- EAST - EU: 182.80, 182.20 [Avg: 182.2 ms]
- EAST - Asia: 300.20, 291.60 [Avg: 295.9 ms]
- West - EU:  302.40, 300.80 [Avg: 301.6ms]
- West - ASIA: 215.40, 214.20 [Avg: 214.8ms]
- EU - ASIA: 448.20, 447.00 [Avg: 447.6ms]

For more similarity to their results and based on the RTT's in Fig10 of the paper, we should select:
[closest setup to what they have] 
primary: EU
seondary: ASIA 
secondary: east
client: west