# Raft-protocol

This is Java implementation of Raft protocol.

Full documentation can be found here: https://raft.github.io/raft.pdf
Implemented by pseudocode described in https://www.cl.cam.ac.uk/teaching/2021/ConcDisSys/dist-sys-notes.pdf

# Implementation details
The application is running using docker-compose. There are 3 nodes in the cluster. 
Each node is a Spring Boot application with:
* gRPC interface for Raft protocol for internal interactions between nodes
* REST API for external client interaction with the System

## REST API for Clients usage
Append message URL:
`GET http://localhost:{node_port}/raft/append/{message}`
Get the node state:
`GET http://localhost:{node_port}/node/state`
node_port is in range 8080-8082

# Start Cluster
`docker-compose --build up`

Now 3 nodes available for interaction on:
`http://localhost:[8080-8082]`

# Disconnect/connect one node from cluster
For testing split-brain problem one node can be disconnected from cluster with help of the command:
`docker network disconnect grpc-network node-[1-3]`
And connected with:
`docker network connect grpc-network node-[1-3]`