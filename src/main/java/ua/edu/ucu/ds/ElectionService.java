package ua.edu.ucu.ds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.edu.ucu.RequestVoteRequest;
import ua.edu.ucu.RequestVoteResponse;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.*;

@Component
public class ElectionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionService.class);

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

    public void initElection() {
        // or leader is suspects
        if (theNodeStatus.currentLeader == null || leaderIsSuspected()) {
            theNodeStatus.currentTerm = theNodeStatus.currentTerm + 1;
            theNodeStatus.currentRole = CANDIDATE;
            theNodeStatus.votedFor = theNodeStatus.nodeId;
            ArrayList<Integer> votes = new ArrayList<>();
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
                    .setLastLogIndex(theNodeStatus.log.size() == 0 ? 0 : theNodeStatus.log.size() - 1) // ???
                    .build();

            sendVoteRequestToCluster(voteRequest);
        }
    }

    private boolean leaderIsSuspected() {
        Instant lastLeaderAppendTime = theNodeStatus.lastLeaderAppendTime;
        return lastLeaderAppendTime != null && lastLeaderAppendTime.compareTo(lastLeaderAppendTime) > 10000;
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
            LOGGER.info("Received response from " + (nodesCount) + " followers");
        } catch (InterruptedException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
        }
    }

    private Optional<RequestVoteResponse> sendRequest(RequestVoteRequest voteRequest, Integer followerId) {
        int i = 1;
        // make 2 attempts ???
        while (i <= 1) {
            try {
                LOGGER.info("Vote attempt #{} to: {}, LOG: {}", i, followerId, voteRequest);
                RequestVoteResponse voteResponse =
                        nodeRegistry.getNodeGrpcClient(followerId).requestVote(voteRequest);
                LOGGER.info("Follower {} has granted vote {} with term {}",
                        followerId,
                        voteResponse.getVoteGranted(),
                        voteResponse.getTerm());
                return Optional.of(voteResponse);
            } catch (Throwable e) {
                LOGGER.error(e.getLocalizedMessage(), e);
            }
            i++;
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
                    replicationService.replicateLog();
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
