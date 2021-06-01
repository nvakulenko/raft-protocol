package ua.edu.ucu.ds;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua.edu.ucu.AppendEntriesRequest;
import ua.edu.ucu.AppendEntriesResponse;
import ua.edu.ucu.RaftProtocolGrpc;
import ua.edu.ucu.RaftProtocolGrpc.RaftProtocolBlockingStub;

public class ReplicationService {

  // taken from replicated-log project - maybe will change according to raft protocol
  public enum ReplicationStatus {
    REPLICATED, FAILED_REPLICATION
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);

  private Integer majority;

  private Map<String, RaftProtocolBlockingStub> followers;

  public boolean replicateLog(AppendEntriesRequest appendEntriesRequest) {
    try {

      // При реплікації Лідер пробує знайти на якому індексі лога FOLLOWER & LEADER можуть синхронізуватися,
      // Leader keeps nextIndex for each follower
      CountDownLatch countDownLatch = new CountDownLatch(majority - 1);
      ExecutorService executor = Executors.newFixedThreadPool(followers.size());

      List<Future<ReplicationStatus>> futures = followers.entrySet().stream().map(
          follower -> {
            return executor.submit(() -> {
              try {
                return replicateLog(appendEntriesRequest, follower);
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

  private ReplicationStatus replicateLog(AppendEntriesRequest appendEntriesRequest, Map.Entry<String, RaftProtocolGrpc.RaftProtocolBlockingStub> follower) {
    int i = 0;
    while (true) {
      try {
        i++;

        SecondaryHealthStatus secondaryStatus =
            secondaryHealthChecker.getSecondaryStatus(follower.getKey());

        // retry with delay in 5 seconds after 2 attempts
        if (i > 2) {
          Thread.sleep(5000);
        }

        // do not try to connect to unhealthy follower
        if (SecondaryHealthStatus.UNHEALTHY.equals(secondaryStatus)) {
          continue;
        }

        LOGGER.info("Replication attempt #{} to: {}, LOG: {}", i + 1, follower.getKey(), appendEntriesRequest.getLog());
        AppendEntriesResponse appendEntriesResponse =
            follower.getValue().appendEntries(appendEntriesRequest);

        LOGGER.info("Received from secondary {} response code {} for appendEntriesRequest {}",
            follower.getKey(),
            appendEntriesResponse.getSuccess(),
            appendEntriesRequest.getLeaderId());

        if (appendEntriesResponse.getSuccess()) {
          LOGGER.info("Replicated appendEntriesRequest {} successfully to {}", appendEntriesRequest.getId(), follower.getKey());
          return ReplicationStatus.REPLICATED;
        }
      } catch (Throwable e) {
        LOGGER.error(e.getLocalizedMessage(), e);
      }
    }
  }


}
