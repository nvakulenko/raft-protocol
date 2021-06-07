package ua.edu.ucu.ds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.edu.ucu.RequestVoteRequest;
import ua.edu.ucu.RequestVoteResponse;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.*;

@Component
public class ElectionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionSchedulingConfig.class);
    private Map<Integer, FollowerNode> followers;

    @Autowired
    private ReplicationService replicationService;

    private ExecutorService executor;

    public ElectionService() {
        // TODO init followers
        executor = Executors.newFixedThreadPool(followers.size());
    }

    @Autowired
    private TheNodeStatus theNodeStatus;

    public void initElection() {
        if (theNodeStatus.currentLeader == null) { // or leader is suspects
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
                    .setLastLogIndex(theNodeStatus.log.size() - 1) // ???
                    .build();

            sendVoteRequestToCluster(voteRequest);
        }
//        on node nodeId suspects leader has failed, or on election timeout do
//            currentTerm := currentTerm + 1; currentRole := candidate
//        votedFor := nodeId; votesReceived := {nodeId}; lastTerm := 0
//        if log.length > 0 then lastTerm := log[log.length − 1].term; end if
//        msg := (VoteRequest, nodeId, currentTerm, log.length, lastTerm)
//        for each node ∈ nodes: send msg to node
//        start election timer
//        end on
    }

    private void sendVoteRequestToCluster(RequestVoteRequest voteRequest) {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(followers.size());
            followers.keySet().stream().forEach(
                    followerId ->
                            executor.execute(() -> {
                                try {
                                    RequestVoteResponse voteResponse = sendRequest(voteRequest, followerId);
                                    processResponse(followerId, voteResponse);
                                } catch (Throwable e) {
                                    LOGGER.error(e.getLocalizedMessage(), e);
                                } finally {
                                    countDownLatch.countDown();
                                }
                            }));

            LOGGER.info("Wait for " + (followers.size()) + " followers");
            countDownLatch.await();
            LOGGER.info("Received response from " + (followers.size()) + " followers");
        } catch (InterruptedException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
        }
    }

    private RequestVoteResponse sendRequest(RequestVoteRequest voteRequest, Integer followerId) {
        int i = 0;
        // make 2 attempts ???
        while (i <= 2) {
            try {
                i++;
                LOGGER.info("Replication attempt #{} to: {}, LOG: {}", i + 1, followerId, voteRequest);
                RequestVoteResponse voteResponse =
                        followers.get(followerId).rpcClient.requestVote(voteRequest);
                LOGGER.info("Follower {} has granted vote {} with term {}",
                        followerId,
                        voteResponse.getVoteGranted(),
                        voteResponse.getTerm());
                return voteResponse;
            } catch (Throwable e) {
                LOGGER.error(e.getLocalizedMessage(), e);
            }
        }
        return null;
    }

    private void processResponse(Integer voterId, RequestVoteResponse voteResponse) {
        boolean granted = voteResponse.getVoteGranted();
        Integer term = voteResponse.getTerm();

        if (CANDIDATE.equals(theNodeStatus.currentRole) && term == theNodeStatus.currentTerm && granted) {
            theNodeStatus.votesReceived.add(voterId);
            if (theNodeStatus.votesReceived.size() >= (followers.size() + 1) / 2) {
                theNodeStatus.currentRole = LEADER;
                theNodeStatus.currentLeader = theNodeStatus.nodeId;
                // cancel election timer
                // replicate log after election
                followers.keySet().forEach(nodeId ->
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
