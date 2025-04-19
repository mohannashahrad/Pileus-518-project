# Pileus-518-Project

To run the basic key-value store on multiple nodes, follow these steps:

1. **Start Redis on each storage node**  
   On each storage node, run the Redis Docker container using:  
   `sudo docker run -d --rm --name redis -p 6379:6379 redis`

2. **Run the storage node server**  
   On each node, run the `storage_server.go` file to expose the `SET` and `GET` endpoints to clients.  
   Go to the `redis_kv_store` directory and run `go run storage_server.go <store_id>`.  

   - `<store_id>` is the unique identifier for the storage node.  
   - These IDs should match the configuration in the [sharding_config](./sharding_config.json).  

   For example, in the current setup we are using two sites: `clem_0` and `clem_1`.

3. **Run the client**  
   Once all storage nodes are running, run the client to send a simple write-only workload based on the sharding configuration. Go to the `client` sub-directory and run `go run client.go`.  

   - You can adjust the size of the workload directly in the code.  
   - Eventually, client workloads will be more complex and configurable using YCSB workloads for our experiments.

**ToDo's** 

- [ ] Make Redis persistant on nodes so that we hit disk and not just memory
- [x] Implement replication agents [pull-based]
   - double-check it works fine
- [ ] Implement the client-side API 
- [ ] Comeplete the implementation of Get function
   - [ ] Handling the condition code + unavailable codes
- [x] Implement different consistencies and the SLA definitions
- [ ] Session-Monitoring functions in the client-side API

- [ ] Recreating Figure 3 [ Avergae Observed Latency for Consistency Choices ]
   - takeaway: latency differs in different consistency levels + with a single consistency choice latency varies client by client

- [ ]Implementing Monitors [co-located with clients]
   - [x] Monitoring the RTT + RTT sliding window
   - [ ] Monitoring the timestamp lag of the nodes

- [ ] Implement Storage metadata on each storage node

- [ ] Implement min_acceptable read timestamp for different consistency levels

- [ ] Implement the server selection algorithm
   - [ ] PNodeCons
   - [ ] PNodeLat
   - [ ] PNodeSLA

Eval:
- [ ] Test different replication windows and utility changes
- [ ] Size of the sliding window impacts reactivity to the load or server responsiveness