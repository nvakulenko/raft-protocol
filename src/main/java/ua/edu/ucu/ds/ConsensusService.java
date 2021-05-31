package ua.edu.ucu.ds;

import org.springframework.stereotype.Service;

@Service
public class ConsensusService {

    // default status is FOLLOWER
    private NodeStatus nodeStatus = NodeStatus.FOLLOWER;

    public NodeStatus getNodeStatus() {
        return nodeStatus;
    }
}
