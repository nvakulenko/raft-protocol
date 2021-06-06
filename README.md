# raft-protocol

This is Java implementation of Raft protocol.

Documentation is here: https://raft.github.io/raft.pdf

# Start 
docker-compose up --scale raft-node=3 CLUSTER_NODES=localhost:8080,localhost:8081,localhost:8082
set nodeIds
As I can see so far: We can directly specify list of nodes or implement service discovery