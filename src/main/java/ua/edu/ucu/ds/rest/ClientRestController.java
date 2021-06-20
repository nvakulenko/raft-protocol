package ua.edu.ucu.ds.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import ua.edu.ucu.ds.service.ReplicationService;
import ua.edu.ucu.ds.service.TheNodeStatus;

import static ua.edu.ucu.ds.service.TheNodeStatus.NodeRole.FOLLOWER;
import static ua.edu.ucu.ds.service.TheNodeStatus.NodeRole.LEADER;

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
            replicationService.replicateLogToFollowers(msg);
            result.append("\n Message is replicated!");
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
                .append(System.lineSeparator())
                .append("Current term: ")
                .append(theNodeStatus.currentTerm)
                .append(System.lineSeparator())
                .append("Current leader id: ")
                .append(theNodeStatus.currentLeader)
                .append(System.lineSeparator())
                .append("Current commit length: ")
                .append(theNodeStatus.commitLength)
                .append(System.lineSeparator())
                .append("Current log: ")
                .append(theNodeStatus.log);

        return result.toString();
    }
}
