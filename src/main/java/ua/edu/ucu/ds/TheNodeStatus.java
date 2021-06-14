package ua.edu.ucu.ds;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;

@Component
public class TheNodeStatus {

    @Value("${raft.nodeid}")
    public volatile Integer nodeId;
    public volatile Integer currentTerm = 0; // persist
    public volatile Integer votedFor = null; // nodeId // persist

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

    public ArrayList<LogEntry> log = new ArrayList<>(); // type // persist
    public volatile Integer commitLength = 0; // persist

    public enum NodeRole {
        LEADER, FOLLOWER, CANDIDATE;
    }

    public volatile NodeRole currentRole = NodeRole.FOLLOWER;
    public volatile Integer currentLeader = null; // nodeId
    public volatile Instant lastLeaderAppendTime;
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

    public List<LogEntry> getLogTail(int startIndex) {
        if (log.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i < log.size() - 1; i++) {
            entries.add(log.get(i));
        }
        return entries;
    }
}
