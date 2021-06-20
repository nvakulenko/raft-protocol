package ua.edu.ucu.ds.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.edu.ucu.AppendEntriesRequest;
import ua.edu.ucu.AppendEntriesResponse;
import ua.edu.ucu.ds.service.TheNodeStatus.LogEntry;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static ua.edu.ucu.ds.service.TheNodeStatus.NodeRole.*;

@Component
public class ReplicationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);
    private static final long REPLICATION_DELAY = 8000L;

    @Autowired
    private NodeRegistry nodeRegistry;
    @Autowired
    private TheNodeStatus theNodeStatus;

    private ExecutorService executor;
    private Integer quorum;

    @PostConstruct
    public void init() {
        executor = Executors.newFixedThreadPool(nodeRegistry.getNodesCount());
        quorum = (nodeRegistry.getNodesCount() + 1) % 2;

        TimerTask task = new TimerTask() {
            public void run() {
                if (LEADER.equals(theNodeStatus.currentRole)) {
                    LOGGER.info("Replication task performed on node {} - time {}: " +
                            theNodeStatus.nodeId, Instant.now());
                    replicateLogToFollowers();
                }
            }
        };
        Timer timer = new Timer("Replication timer");
        timer.schedule(task, 15000, REPLICATION_DELAY);
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {

        // print node status
        TheNodeStatus.NodeRole currentRole = theNodeStatus.currentRole;
        Integer nodeId = theNodeStatus.nodeId;
        Integer currentLeader = theNodeStatus.currentLeader;
        Integer currentTerm = theNodeStatus.currentTerm;
        Integer commitLength = theNodeStatus.commitLength;

        LOGGER.info("Node id {} state: Node role is: {}. Current leader: {}. Current term: {}. Commit length {}" +
                " Received AppendEntriesRequest {}", nodeId, currentRole, currentLeader, currentTerm, commitLength, request);

        AppendEntriesResponse response = null;
        switch (currentRole) {
            case LEADER:
                response = appendEntriesAsLeader(request);
                break;
            case FOLLOWER:
                response = appendEntriesAsFollower(request);
                break;
            case CANDIDATE:
                response = AppendEntriesResponse.newBuilder().build();
                break;
        }

        return response;
    }

    private AppendEntriesResponse appendEntriesAsLeader(AppendEntriesRequest request) {
        // generate heart beats and replicate log
        List<AppendEntriesRequest.LogEntry> entriesList = request.getEntriesList();
        boolean isReplicated = replicateLogToFollowers(entriesList.get(0).getMsg());

        // return response
        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                .setTerm(theNodeStatus.currentTerm)
                .setSuccess(isReplicated)
                .build();
        return response;
    }

    private AppendEntriesResponse appendEntriesAsFollower(AppendEntriesRequest request) {
        // wait for heartbeats
        // if I am not the LEADER -> redirect to LEADER

        // if is heartbeat -> update last leader ping time
        // If there are no new messages, entries is the empty list.
        // LogRequest messages with entries = [] serve as heartbeats, letting
        // followers know that the leader is still alive.
        Integer currentLeader = theNodeStatus.currentLeader;
        if (currentLeader != null && currentLeader == request.getLeaderId()) {
            theNodeStatus.lastLeaderAppendTime = Instant.now();
        }

        // Follower checks consistency of the received log entry
        // check first - prevLogIndex, prevLogTerm = check previous pointer
        // return SUCCESS or MISMATCH
        // Leader tries to send 2 previous log entries
        // if prevLogTerm is older then a current one - trust the message and rewrite log entries
        // while reject -> try to replicate - відмотування назад з кроком -1 поки не знайдеться точка синхронізації
        // т. ч. всі фоловери синхронізуються з новим лідером

        int term = request.getTerm();
        int leaderId = request.getLeaderId();
        // accept the new leader
        if (term > theNodeStatus.currentTerm) {
            theNodeStatus.currentTerm = term;
            theNodeStatus.votedFor = null;
            theNodeStatus.currentRole = FOLLOWER;
            theNodeStatus.currentLeader = leaderId;

            LOGGER.info("New LEADER {} is accepted by FOLLOWER node {}, current term {}",
                    theNodeStatus.currentLeader, theNodeStatus.nodeId, theNodeStatus.currentTerm);
        }

        if (term == theNodeStatus.currentTerm && CANDIDATE.equals(theNodeStatus.currentRole)) {
            theNodeStatus.currentRole = FOLLOWER;
            theNodeStatus.currentLeader = leaderId;

            LOGGER.info("New LEADER {} is accepted by CANDIDATE node {}, current term {}",
                    theNodeStatus.currentLeader, theNodeStatus.nodeId, theNodeStatus.currentTerm);
        }

        int logLength = theNodeStatus.log.size();
        boolean logOk = (logLength >= request.getPrevLogIndex()) &&
                (request.getPrevLogIndex() == 0 || request.getPrevLogTerm() == theNodeStatus.log.get(request.getPrevLogIndex() - 1).term);
        LOGGER.trace("Log is {} for {} node {}", logOk, theNodeStatus.currentRole, theNodeStatus.nodeId);

        AppendEntriesResponse response = null;
        if (term == theNodeStatus.currentTerm && logOk) {
            appendEntries(request.getPrevLogIndex(), request.getLeaderCommit(), request.getEntriesList());
            // AppendEntries(logLength, leaderCommit, entries)
            Integer ack = theNodeStatus.log.size(); //logLength + request.getEntriesCount();
            // send (LogResponse, nodeId, currentTerm, ack, true) to leaderId
            response = AppendEntriesResponse.newBuilder()
                    .setSuccess(true)
                    .setAck(ack)
                    .setTerm(theNodeStatus.currentTerm)
                    .build();
        } else {
            // send (LogResponse, nodeId, currentTerm, 0, false) to leaderId
            response = AppendEntriesResponse.newBuilder()
                    .setSuccess(false)
                    .setAck(0)
                    .setTerm(theNodeStatus.currentTerm)
                    .build();
        }

        LOGGER.info("Send AppendEntriesResponse {}", response);
        return response;
    }

    private void appendEntries(int logLength, int leaderCommit, List<AppendEntriesRequest.LogEntry> entries) {

        LOGGER.trace("Append entries logLength {} leaderCommit {} entries {} entries.size {}", logLength, leaderCommit, entries, entries.size());

        if (entries.size() > 0 && theNodeStatus.log.size() > logLength) {
            if (theNodeStatus.log.get(logLength).term != entries.get(0).getTerm()) {
                // log := hlog[0], log[1], . . . , log[logLength − 1]i
                LOGGER.info("Log remains the same on node {}", theNodeStatus.nodeId);
            }
        }

        LOGGER.trace("logLength {} + entries.size() {} > theNodeStatus.log.size() {}", logLength, entries.size(), theNodeStatus.log.size());

        if (logLength + entries.size() > theNodeStatus.log.size()) {
            for (int i = theNodeStatus.log.size() - logLength; i <= entries.size() - 1; i++) {
                AppendEntriesRequest.LogEntry logEntry = entries.get(i);
                TheNodeStatus.LogEntry entry = new TheNodeStatus.LogEntry(logEntry.getTerm(), logEntry.getMsg());
                theNodeStatus.log.add(entry);
                LOGGER.info("Append entry on node {} with id {} to log {}", theNodeStatus.nodeId, i, entry);
            }
        }

        if (leaderCommit > theNodeStatus.commitLength) {
            theNodeStatus.commitLength = leaderCommit;
        }

        LOGGER.info("After appendEntries on node {}: log size: {}; commit length: {}",
                theNodeStatus.nodeId, theNodeStatus.log.size(), theNodeStatus.commitLength);
    }

    public synchronized boolean replicateLogToFollowers(String msg) {
        theNodeStatus.appendLog(msg);
        return replicateLogToFollowers();
    }

    public synchronized boolean replicateLogToFollowers() {
        // 2 - notify Followers in parallel
        // 2.1 - OK -> write to StateMachine and return response
        // 2.2 - NOT OK -> return error
        try {
            // ??? do we need to wait to all nodes or for quorum???
            int followersCount = nodeRegistry.getNodesCount();
            CountDownLatch countDownLatch = new CountDownLatch(followersCount);
            List<Future<Boolean>> futures = nodeRegistry.getNodeClients().keySet().stream().map(
                    followerId -> {
                        return executor.submit(() -> {
                            try {
                                return replicateLogToFollower(buildRequest(followerId), followerId);
                            } catch (Throwable e) {
                                LOGGER.error(e.getLocalizedMessage(), e);
                                return false;
                            } finally {
                                countDownLatch.countDown();
                            }
                        });
                    }).collect(Collectors.toList());

            LOGGER.info("Wait for " + followersCount + " followers");
            countDownLatch.await();
            LOGGER.info("Received response from " + followersCount + " followers");
            long successResponses = futures.stream()
                    .filter(in -> {
                        try {
                            return in.isDone() && in.get();
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getLocalizedMessage(), e);
                            return false;
                        } catch (ExecutionException e) {
                            LOGGER.error(e.getLocalizedMessage(), e);
                            return false;
                        }
                    }).count();
            return successResponses == followersCount;
        } catch (InterruptedException e) {
            LOGGER.error(e.getLocalizedMessage());
            return false;
        }
    }

    private boolean replicateLogToFollower(AppendEntriesRequest appendEntriesRequest, Integer followerId) {
        try {
            LOGGER.info("Replicate to: {}, LOG: {}", followerId, appendEntriesRequest);
            AppendEntriesResponse response =
                    nodeRegistry.getNodeGrpcClient(followerId).appendEntries(appendEntriesRequest);

            LOGGER.info("Received response from node {}, success is {}, leader for node is {}",
                    followerId,
                    response.getSuccess(),
                    appendEntriesRequest.getLeaderId());

            Integer currentTerm = theNodeStatus.currentTerm;
            if (response.getTerm() == currentTerm && LEADER.equals(theNodeStatus.currentRole)) {
                Integer ack = response.getAck();
                Integer ackedLength = theNodeStatus.ackedLength.get(followerId) == null ? 0 : theNodeStatus.ackedLength.get(followerId);
                if (response.getSuccess() && ack >= ackedLength) {
                    theNodeStatus.sentLength.put(followerId, ack);
                    theNodeStatus.ackedLength.put(followerId, ack);
                    commitLogEntries();
                } else if (theNodeStatus.sentLength.get(followerId) != null
                        && theNodeStatus.sentLength.get(followerId) > 0) {
                    theNodeStatus.sentLength.put(followerId, theNodeStatus.sentLength.get(followerId) - 1);
                    replicateLogToFollower(buildRequest(followerId), followerId);
                }
            } else if (response.getTerm() > currentTerm) {
                theNodeStatus.currentTerm = response.getTerm();
                theNodeStatus.currentRole = FOLLOWER;
                theNodeStatus.votedFor = null;
            }
            return response.getSuccess();
        } catch (Throwable e) {
            LOGGER.error(e.getLocalizedMessage());
            return false;
        }
    }

    private void commitLogEntries() {
        Integer minAcks = quorum;
        List<Integer> ready = new ArrayList<>();

        for (int i = 0; i < theNodeStatus.log.size(); i++) {
            if (acknowledgedNodes(i) >= minAcks) {
                ready.add(i + 1);
            }
        }

        if (ready.size() > 0) {
            OptionalInt maxReady = ready.stream().mapToInt(in -> in).max();
            if (maxReady.getAsInt() > theNodeStatus.commitLength &&
                    theNodeStatus.log.get(maxReady.getAsInt() - 1).term == theNodeStatus.currentTerm) {
                for (int i = theNodeStatus.commitLength; i < maxReady.getAsInt(); i++) {
                    // TODO deliver message to application
                    LOGGER.info("Log with index {} is committed", i);
                }
                theNodeStatus.commitLength = maxReady.getAsInt();
            }
        }
    }

    private int acknowledgedNodes(int length) {
        //    define acks(length) = |{n ∈ nodes | ackedLength[n] ≥ length}|
        Map<Integer, Integer> ackedLength = theNodeStatus.ackedLength;
        if (!ackedLength.isEmpty()) {
            return (int) ackedLength.keySet().stream().filter(key -> ackedLength.get(key) >= length).count();
        }
        return 0;
    }

    private AppendEntriesRequest buildRequest(Integer followerId) {
        // N.B. При реплікації Лідер пробує знайти на якому індексі лога FOLLOWER & LEADER можуть синхронізуватися,
        // Leader keeps nextIndex for each follower

        Integer i = theNodeStatus.sentLength.get(followerId) == null ? 0 : theNodeStatus.sentLength.get(followerId);
        List<LogEntry> entries = theNodeStatus.getLogTail(i);

        Integer prevLogTerm = 0;
        Integer prevLogIndex = i;
        if (i > 0) {
            prevLogTerm = theNodeStatus.log.get(i - 1).term;
        }

        return AppendEntriesRequest.newBuilder()
                .setPrevLogTerm(prevLogTerm)
                .setLeaderId(theNodeStatus.nodeId)
                .setPrevLogIndex(prevLogIndex) // or i???
                .setLeaderCommit(theNodeStatus.commitLength)
                .setTerm(theNodeStatus.currentTerm)
                .addAllEntries(
                        entries.stream().map(
                                entry -> AppendEntriesRequest.LogEntry.newBuilder()
                                        .setTerm(entry.term)
                                        .setMsg(entry.msg)
                                        .build())
                                .collect(Collectors.toList()))
                .build();
    }
}
