package ua.edu.ucu.ds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.edu.ucu.AppendEntriesRequest;
import ua.edu.ucu.AppendEntriesResponse;
import ua.edu.ucu.ds.TheNodeStatus.LogEntry;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.FOLLOWER;
import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.LEADER;

@Component
public class ReplicationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);

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
    }

    public boolean replicateLog(String msg) {
        theNodeStatus.appendLog(msg);
        return replicateLog();
    }

    public boolean replicateLog() {
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
                ready.add(i);
            }
        }

        if (ready.size() > 0) {
            OptionalInt maxReady = ready.stream().mapToInt(in -> in).max();
            if (maxReady.getAsInt() > theNodeStatus.commitLength &&
                theNodeStatus.log.get(maxReady.getAsInt() - 1).term == theNodeStatus.currentTerm) {
                for (int i = theNodeStatus.commitLength; i < maxReady.getAsInt() - 1; i++) {
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
            return (int)ackedLength.keySet().stream().filter(key -> ackedLength.get(key) >= length).count();
        }
        return 0;
    }

    private AppendEntriesRequest buildRequest(Integer followerId) {
        // N.B. При реплікації Лідер пробує знайти на якому індексі лога FOLLOWER & LEADER можуть синхронізуватися,
        // Leader keeps nextIndex for each follower

        Integer i = theNodeStatus.sentLength.get(followerId) == null ? 0 : theNodeStatus.sentLength.get(followerId);
        List<LogEntry> entries = theNodeStatus.getLogTail(i);

        Integer prevLogTerm = 0;
        if (i > 0) {
            prevLogTerm = theNodeStatus.log.get(i - 1).term;
        }

        return AppendEntriesRequest.newBuilder()
                .setPrevLogTerm(prevLogTerm)
                .setLeaderId(theNodeStatus.nodeId)
                .setPrevLogIndex(i == 0 ? 0 : i - 1) // or i???
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
