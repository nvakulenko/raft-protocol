# raft-protocol

This is Java implementation of Raft protocol.

Documentation is here: https://raft.github.io/raft.pdf

# Start 
docker-compose build
docker-compose up

# Disconnect one node from cluster
docker network disconnect grpc-network node-1
docker network connect grpc-network node-1