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
   Once all storage nodes are running, run the client to send different YCSB workloads based on the sharding configuration. Go to the `client` sub-directory and run `go run client.go`.  

   - You can adjust the size of the workload directly in the code.  
