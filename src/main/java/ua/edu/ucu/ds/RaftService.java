package ua.edu.ucu.ds;

import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import ua.edu.ucu.*;

@GRpcService
public class RaftService extends RaftProtocolGrpc.RaftProtocolImplBase {

    @Autowired
    private ConsensusService consensusService;

    private final AtomicInteger counter = new AtomicInteger();
    private final Map<Integer, List<String>> replicatedLog = new HashMap<>();

    @Override
    public void appendEntries(AppendEntriesRequest request,
        StreamObserver<AppendEntriesResponse> responseObserver) {
        NodeStatus nodeStatus = consensusService.getNodeStatus();

        if (NodeStatus.LEADER.equals(nodeStatus)) {
            // generate heart beats and replicate log
            // 1 - Write to log
            int leaderIndex = counter.incrementAndGet();
            replicatedLog.put(leaderIndex, request.getEntriesList());
            // 2 - notify Followers in parallel

            // 2.1 - OK -> write to StateMachine and return response
            // 2.2 - NOT OK -> return error
        }

        if (NodeStatus.FOLLOWER.equals(nodeStatus)) {
            // wait for heartbeats
            // if I am not the LEADER -> redirect to LEADER

            // if Appended - return true, if not - false
            boolean success = false;

            // Follower checks consistency of the received log entry
            // check first - prevLogIndex, prevLogTerm = check previous pointer
            // return SUCCESS or MISMATCH
            // Leader tries to send 2 previous log entries
            // if prevLogTerm is older then a current one - trust the message and rewrite log entries
            // while reject -> try to replicate - відмотування назад з кроком -1 поки не знайдеться точка синхронізації
            // т. ч. всі фоловери синхронізуються з новим лідером

            AppendEntriesResponse.newBuilder().setSuccess(success);



        }

        if (NodeStatus.CANDIDATE.equals(nodeStatus)) {
            // ask for votes
        }

    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        super.requestVote(request, responseObserver);
    }
}
