package ua.edu.ucu.ds;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.FOLLOWER;
import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.LEADER;

@RestController
public class ClientRestController {

    @Autowired
    private ReplicationService replicationService;

    @Autowired
    private TheNodeStatus theNodeStatus;

    @GetMapping("/raft/append/{msg}")
    public String appendMessage(@PathVariable String msg) {
        StringBuilder result = new StringBuilder();
        TheNodeStatus.NodeRole currentRole = theNodeStatus.currentRole;

        result.append("Current node status is: ").append(currentRole.toString());
        if (LEADER.equals(currentRole)) {
            boolean replicateLog = replicationService.replicateLog(msg);
            result.append("\n Msg is replicated: ").append(replicateLog);
        }

        if (FOLLOWER.equals(currentRole)) {
            result.append("\n Can not replicate. Current leader id is").append(theNodeStatus.currentLeader);
        }
        return result.toString();
    }

    @GetMapping("/node/state")
    public String getNodeState() {
        StringBuilder result = new StringBuilder();
        result.append("Current node role: ")
                .append(theNodeStatus.currentRole.toString())
                .append("\r\n Current leader id: ")
                .append(theNodeStatus.currentLeader)
                .append("\r\n Current commit length: ")
                .append(theNodeStatus.commitLength)
                .append("\r\n Current log: ")
                .append(theNodeStatus.log);

        return result.toString();
    }
}
