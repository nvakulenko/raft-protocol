package ua.edu.ucu.ds.service;


import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ua.edu.ucu.RaftProtocolGrpc;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class NodeRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRegistry.class);

    @Value("${raft.nodes}")
    private String nodes;

    private Map<Integer, RaftProtocolGrpc.RaftProtocolBlockingStub> nodeClients = new HashMap<>();

    @PostConstruct
    public void init() {
        LOGGER.info("Register nodes: " + nodes);

        String[] nodes = this.nodes.split("#");
        for (String node : nodes) {
            LOGGER.info("Register node: " + node);

            String[] nodeParams = node.split(":");
            Integer nodeId = Integer.valueOf(nodeParams[0]);
            String host = nodeParams[1];
            Integer port = Integer.valueOf(nodeParams[2]);

            nodeClients.put(nodeId, createGrpcClient(host, port));
        }
    }

    public RaftProtocolGrpc.RaftProtocolBlockingStub getNodeGrpcClient(Integer nodeId) {
        return nodeClients.get(nodeId);
    }

    public Map<Integer, RaftProtocolGrpc.RaftProtocolBlockingStub> getNodeClients() {
        return nodeClients;
    }

    public Integer getNodesCount() {
        return nodeClients.size();
    }

    private RaftProtocolGrpc.RaftProtocolBlockingStub createGrpcClient(String host, Integer port) {
        ManagedChannelBuilder<?> channelBuilder =
                ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext();
        return RaftProtocolGrpc.newBlockingStub(channelBuilder.build());
    }
}
