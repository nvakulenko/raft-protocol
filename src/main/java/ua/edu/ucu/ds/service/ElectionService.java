package ua.edu.ucu.ds.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.edu.ucu.RequestVoteRequest;
import ua.edu.ucu.RequestVoteResponse;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ua.edu.ucu.ds.service.TheNodeStatus.NodeRole.*;

@Component
public class ElectionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionService.class);
    public static final int LEADER_SUSPECTED_TIMEOUT_IN_SECONDS = 10;

    @Autowired
    private NodeRegistry nodeRegistry;
    @Autowired
    private ReplicationService replicationService;

    private ExecutorService executor;

    @PostConstruct
    public void init() {
        executor = Executors.newFixedThreadPool(nodeRegistry.getNodesCount());
    }

    @Autowired
    private TheNodeStatus theNodeStatus;

    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        LOGGER.info("Received RequestVoteRequest: " + request.toString());

        // from request
        Integer cId = request.getCandidateId();
        Integer cTerm = request.getTerm();
        Integer cLogLength = request.getLastLogIndex();
        Integer cLogTerm = request.getLastLogTerm();

        LOGGER.info("Received RequestVoteRequest.cLogTerm: " + cLogTerm);

        // at nodeId
        Integer myLogTerm = theNodeStatus.log.isEmpty() ?
                0 : theNodeStatus.log.get(theNodeStatus.log.size() - 1).term;
        Boolean logOk =
                (cLogTerm > myLogTerm) ||
                        (cLogTerm == myLogTerm && cLogLength >= theNodeStatus.log.size());

        LOGGER.info("(cLogTerm {} > myLogTerm {} ) ||" +
                "(cLogTerm == myLogTerm && cLogLength {} >= theNodeStatus.log.size()) {}; >> theNodeStatus.commitLength {}",
                cLogTerm, myLogTerm, cLogLength, theNodeStatus.log.size(), theNodeStatus.commitLength);
        Boolean termOk =
                (cTerm > theNodeStatus.currentTerm) ||
                        (cTerm == theNodeStatus.currentTerm &&
                                (theNodeStatus.votedFor == null || theNodeStatus.votedFor == cId));

        LOGGER.info("(cTerm {} > theNodeStatus.currentTerm {}) || " +
                "(cTerm {} == theNodeStatus.currentTerm {} &&" +
                        "(theNodeStatus.votedFor {} == null || theNodeStatus.votedFor == cId {} ))",
                cTerm, theNodeStatus.currentTerm, cTerm, theNodeStatus.currentTerm, theNodeStatus.votedFor, cId);

        LOGGER.info("LogOk: {}, TermOk {}", logOk, termOk);

        RequestVoteResponse response = null;
        if (logOk && termOk) {
            theNodeStatus.currentTerm = cTerm;
            theNodeStatus.currentRole = FOLLOWER;
            theNodeStatus.votedFor = cId;
            //??? not in pseudocode but should be here
            theNodeStatus.currentLeader = cId;

            response = RequestVoteResponse.newBuilder()
                    .setTerm(theNodeStatus.currentTerm)
                    .setVoteGranted(true)
                    .build();
        } else {
            response = RequestVoteResponse.newBuilder()
                    .setTerm(theNodeStatus.currentTerm)
                    .setVoteGranted(false)
                    .build();
        }

        return response;
    }

    public void initElection() {
        // or leader is suspects
        checkLeaderIsSuspected();

        if (theNodeStatus.currentLeader == null) {
            theNodeStatus.currentTerm = theNodeStatus.currentTerm + 1;
            theNodeStatus.currentRole = CANDIDATE;
            theNodeStatus.votedFor = theNodeStatus.nodeId;
            CopyOnWriteArrayList<Integer> votes = new CopyOnWriteArrayList<Integer>();
            votes.add(theNodeStatus.nodeId);
            theNodeStatus.votesReceived = votes;

            Integer lastTerm = 0;
            if (theNodeStatus.log.size() > 0) {
                lastTerm = theNodeStatus.log.get(theNodeStatus.log.size() - 1).term;
            }

            RequestVoteRequest voteRequest = RequestVoteRequest.newBuilder()
                    .setCandidateId(theNodeStatus.nodeId)
                    .setTerm(theNodeStatus.currentTerm)
                    .setLastLogTerm(lastTerm)
                    .setLastLogIndex(theNodeStatus.log.size())
                    .build();

            sendVoteRequestToCluster(voteRequest);
        }
    }

    private void checkLeaderIsSuspected() {
        Instant lastLeaderAppendTime = theNodeStatus.lastLeaderAppendTime;
        boolean isLeaderSuspected = FOLLOWER.equals(theNodeStatus.currentRole) &&
                lastLeaderAppendTime != null &&
                Duration.between(lastLeaderAppendTime, Instant.now()).getSeconds() > LEADER_SUSPECTED_TIMEOUT_IN_SECONDS;
        if (isLeaderSuspected) {
            LOGGER.info("Leader {} is suspected, reset leader to null on node {}",
                    theNodeStatus.currentLeader, theNodeStatus.nodeId);
            theNodeStatus.currentLeader = null;
            theNodeStatus.votedFor = null;
        }
    }

    private void sendVoteRequestToCluster(RequestVoteRequest voteRequest) {
        try {
            Integer nodesCount = nodeRegistry.getNodesCount();
            CountDownLatch countDownLatch = new CountDownLatch(nodesCount);
            nodeRegistry.getNodeClients().keySet().stream().forEach(
                    followerId ->
                            executor.execute(() -> {
                                try {
                                    Optional<RequestVoteResponse> voteResponse = sendRequest(voteRequest, followerId);
                                    processResponse(followerId, voteResponse);
                                } catch (Throwable e) {
                                    LOGGER.error(e.getLocalizedMessage(), e);
                                } finally {
                                    countDownLatch.countDown();
                                }
                            }));

            LOGGER.info("Wait for " + (nodesCount) + " followers");
            countDownLatch.await();
            LOGGER.info("Received election response from " + (nodesCount) + " followers");
        } catch (InterruptedException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
        }
    }

    private Optional<RequestVoteResponse> sendRequest(RequestVoteRequest voteRequest, Integer followerId) {
        try {
            LOGGER.info("Vote attempt of node {} to: {}, LOG: {}", theNodeStatus.nodeId, followerId, voteRequest);
            RequestVoteResponse voteResponse =
                    nodeRegistry.getNodeGrpcClient(followerId).requestVote(voteRequest);
            LOGGER.info("Follower {} has granted vote {} with term {}",
                    followerId,
                    voteResponse.getVoteGranted(),
                    voteResponse.getTerm());
            return Optional.of(voteResponse);
        } catch (Throwable e) {
            LOGGER.error(e.getLocalizedMessage());
        }
        return Optional.empty();
    }

    private void processResponse(Integer voterId, Optional<RequestVoteResponse> voteResponse) {
        if (voteResponse.isEmpty()) return;

        boolean granted = voteResponse.get().getVoteGranted();
        Integer term = voteResponse.get().getTerm();

        if (CANDIDATE.equals(theNodeStatus.currentRole) && term == theNodeStatus.currentTerm && granted) {
            theNodeStatus.votesReceived.add(voterId);
            if (theNodeStatus.votesReceived.size() >= (nodeRegistry.getNodesCount() + 1) / 2) {
                theNodeStatus.currentRole = LEADER;
                theNodeStatus.currentLeader = theNodeStatus.nodeId;
                // cancel election timer
                // replicate log after election
                nodeRegistry.getNodeClients().keySet().forEach(nodeId ->
                {
                    theNodeStatus.sentLength.put(nodeId, theNodeStatus.log.size());
                    theNodeStatus.ackedLength.put(nodeId, 0);
                    // replicate log
                    replicationService.replicateLogToFollowers();
                    // ReplicateLog(nodeId, follower)
                });
            }
        } else if (term > theNodeStatus.currentTerm) {
            theNodeStatus.currentTerm = term;
            theNodeStatus.currentRole = FOLLOWER;
            theNodeStatus.votedFor = null;
            // cancel election timer
        }
    }
}
