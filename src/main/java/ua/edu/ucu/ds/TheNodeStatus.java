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

    public static class LogEntry {
        // message that we want to deliver through total order broadcast,
        public String msg;
        // term property contains the term number in which it was broadcast
        public Integer term;

        public LogEntry(Integer term, String msg) {
            this.term = term;
            this.msg = msg;
        }
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
    public Map<Integer, Integer> ackedLength = new HashMap<>(); // type???

    public void appendLog(String msg) {
        // append the record (msg : msg, term : currentTerm) to log
        // ackedLength[nodeId] := log.length
        LogEntry result = new LogEntry(currentTerm, msg);
        log.add(result);
        ackedLength.put(nodeId, log.size());
    }

    public List<LogEntry> getLogEntries(int startIndex) {
        ArrayList<LogEntry> entries = new ArrayList<>();

        for (int i = startIndex; i < log.size() - 1; i++) {
            entries.add(log.get(i));
        }
        return entries;
    }
}
