package ua.edu.ucu.ds;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class TheNodeStatus {
    // todo: get on startup the node Id
    public Integer nodeId;

    public Integer currentTerm = 0; // persist
    public Integer votedFor = null; // nodeId // persist

    public class LogEntry {
        // message that we want to deliver through total order broadcast,
        public String msg;
        // term property contains the term number in which it was broadcast
        public Integer term;
    }

    public ArrayList<LogEntry> log; // type // persist
    public Integer commitLength = 0; // persist

    public enum NodeRole {
        LEADER, FOLLOWER, CANDIDATE;
    }

    public NodeRole currentRole = NodeRole.FOLLOWER;
    public Integer currentLeader = null; // nodeId
    public List<Integer> votesReceived = Collections.emptyList(); // nodeIds
    public Map<Integer, Integer> sentLength = new HashMap(); // <NodeId, sentLength>
    public Integer ackedLength = 0; // type???

    public LogEntry appendLog(String msg) {
        // append the record (msg : msg, term : currentTerm) to log
        // ackedLength[nodeId] := log.length
        LogEntry result = new LogEntry();
        result.msg = msg;
        result.term = currentTerm;
        ackedLength = log.size();
        return result;
    }
}
