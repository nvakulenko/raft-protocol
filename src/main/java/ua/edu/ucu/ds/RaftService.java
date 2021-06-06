package ua.edu.ucu.ds;

import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import ua.edu.ucu.*;
import ua.edu.ucu.ds.experiments.Log;

import java.util.concurrent.atomic.AtomicInteger;

@GRpcService
public class RaftService extends RaftProtocolGrpc.RaftProtocolImplBase {

    @Autowired
    private TheNodeStatus theNodeStatus;
    @Autowired
    private ReplicationService replicationService;
    @Autowired
    private Log replicatedLog;

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {

        TheNodeStatus.NodeRole currentRole = theNodeStatus.currentRole;

        if (TheNodeStatus.NodeRole.LEADER.equals(currentRole)) {
            // generate heart beats and replicate log
            // 1 - Write to log
            int leaderIndex = counter.incrementAndGet();
            //Log.LogEntry previousLogEntry = replicatedLog.putAndGetPreviousLogEntry(leaderIndex, request);

            TheNodeStatus.LogEntry logEntry = theNodeStatus.appendLog(request.getEntriesList().get(0).getMsg());
            // 2 - notify Followers in parallel
            // 2.1 - OK -> write to StateMachine and return response
            // 2.2 - NOT OK -> return error
            boolean isReplicated = replicationService.replicateLog(logEntry);

            // return response
            responseObserver.onNext(AppendEntriesResponse.newBuilder()
                    .setTerm(theNodeStatus.currentTerm)
                    .setSuccess(isReplicated)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (TheNodeStatus.NodeRole.FOLLOWER.equals(currentRole)) {
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

//            on receiving (LogRequest, leaderId, term, logLength, logTerm,
//                    leaderCommit, entries) at node nodeId do
//                if term > currentTerm then
//            currentTerm := term; votedFor := null
//            currentRole := follower; currentLeader := leaderId
//            end if
//              if term = currentTerm ∧ currentRole = candidate then
//              currentRole := follower; currentLeader := leaderId
//            end if
//            logOk := (log.length ≥ logLength) ∧
//            (logLength = 0 ∨ logTerm = log[logLength − 1].term)
//            if term = currentTerm ∧ logOk then
//            AppendEntries(logLength, leaderCommit, entries)
//            ack := logLength + entries.length
//            send (LogResponse, nodeId, currentTerm, ack,true) to leaderId
//            else
//            send (LogResponse, nodeId, currentTerm, 0, false) to leaderId
//            end if
//            end on


        }

        if (TheNodeStatus.NodeRole.CANDIDATE.equals(currentRole)) {
            // ask for votes
        }

    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        super.requestVote(request, responseObserver);
    }
}
