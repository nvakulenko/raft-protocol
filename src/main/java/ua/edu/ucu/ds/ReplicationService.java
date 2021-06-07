package ua.edu.ucu.ds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ua.edu.ucu.AppendEntriesRequest;
import ua.edu.ucu.AppendEntriesResponse;
import ua.edu.ucu.ds.TheNodeStatus.LogEntry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class ReplicationService {

    // taken from replicated-log project - maybe will change according to raft protocol
    public enum ReplicationStatus {
        REPLICATED, FAILED_REPLICATION
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);
    private ExecutorService executor;

    private Integer majority;

    private Map<Integer, FollowerNode> followers;

    public ReplicationService() {
        // TODO init followers
        executor = Executors.newFixedThreadPool(followers.size());
    }

    @Autowired
    private TheNodeStatus theNodeStatus;


    public boolean replicateLog() {
        // 2 - notify Followers in parallel
        // 2.1 - OK -> write to StateMachine and return response
        // 2.2 - NOT OK -> return error
        try {
            CountDownLatch countDownLatch = new CountDownLatch(majority - 1);
            List<Future<ReplicationStatus>> futures = followers.keySet().stream().map(
                    followerId -> {
                        return executor.submit(() -> {
                            try {
                                return replicateLogToFollower(buildRequest(followerId), followerId);
                                // save replication status
                            } catch (Throwable e) {
                                LOGGER.error(e.getLocalizedMessage(), e);
                                return ReplicationStatus.FAILED_REPLICATION;
                            } finally {
                                countDownLatch.countDown();
                            }
                        });
                    }).collect(Collectors.toList());

            LOGGER.info("Wait for " + (majority - 1) + " replicas");
            countDownLatch.await();
            LOGGER.info("Received response from " + (majority - 1) + " replicas");
            long failureResponses = futures.stream()
                    .filter(in -> {
                        try {
                            return in.isDone() && ReplicationStatus.FAILED_REPLICATION.equals(in.get());
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getLocalizedMessage(), e);
                            return false;
                        } catch (ExecutionException e) {
                            LOGGER.error(e.getLocalizedMessage(), e);
                            return false;
                        }
                    }).count();
            return failureResponses == 0;
        } catch (InterruptedException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            // by fact UNKNOWN
            return false;
        }
    }

    private ReplicationStatus replicateLogToFollower(AppendEntriesRequest appendEntriesRequest, Integer followerId) {
        int i = 0;
        while (true) {
            try {
                i++;
                if (i > 2) {
                    Thread.sleep(5000);
                }
                LOGGER.info("Replication attempt #{} to: {}, LOG: {}", i + 1, followerId, appendEntriesRequest);
                AppendEntriesResponse appendEntriesResponse =
                        followers.get(followerId).rpcClient.appendEntries(appendEntriesRequest);

                LOGGER.info("Received from secondary {} response code {} for appendEntriesRequest {}",
                        followerId,
                        appendEntriesResponse.getSuccess(),
                        appendEntriesRequest.getLeaderId());

                if (appendEntriesResponse.getSuccess()) {
                    LOGGER.info("Replicated appendEntriesRequest {} successfully to {}", appendEntriesRequest, followerId);
                    return ReplicationStatus.REPLICATED;
                }
            } catch (Throwable e) {
                LOGGER.error(e.getLocalizedMessage(), e);
            }
        }
    }

    private AppendEntriesRequest buildRequest(Integer followerId) {
//      function ReplicateLog(leaderId, followerId)
//      i := sentLength[followerId]
//      entries := hlog[i], log[i + 1], . . . , log[log.length − 1]i
//      prevLogTerm := 0
//      if i > 0 then
//      prevLogTerm := log[i − 1].term
//      end if
//      send (LogRequest, leaderId, currentTerm, i, prevLogTerm,
//              commitLength, entries) to followerId
//      end function

        // N.B. При реплікації Лідер пробує знайти на якому індексі лога FOLLOWER & LEADER можуть синхронізуватися,
        // Leader keeps nextIndex for each follower

        Integer i = theNodeStatus.sentLength.get(followerId);
        // TODO: copy tail of entries
        List<LogEntry> entries = theNodeStatus.getLogEntries(i);

        Integer prevLogTerm = 0;
        if (i > 0) {
            prevLogTerm = theNodeStatus.log.get(i - 1).term;
        }

        return AppendEntriesRequest.newBuilder()
                .setPrevLogTerm(prevLogTerm)
                .setLeaderId(theNodeStatus.nodeId)
                .setPrevLogIndex(i - 1) // or i???
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
