package ua.edu.ucu.ds;

import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import ua.edu.ucu.*;
import ua.edu.ucu.AppendEntriesRequest.LogEntry;

import java.util.List;

import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.CANDIDATE;
import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.FOLLOWER;

@GRpcService
public class RaftService extends RaftProtocolGrpc.RaftProtocolImplBase {

    @Autowired
    private TheNodeStatus theNodeStatus;
    @Autowired
    private ReplicationService replicationService;

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {

        TheNodeStatus.NodeRole currentRole = theNodeStatus.currentRole;

        if (TheNodeStatus.NodeRole.LEADER.equals(currentRole)) {
            // generate heart beats and replicate log
            theNodeStatus.appendLog(request.getEntriesList().get(0).getMsg());
            boolean isReplicated = replicationService.replicateLog();

            // return response
            responseObserver.onNext(AppendEntriesResponse.newBuilder()
                    .setTerm(theNodeStatus.currentTerm)
                    .setSuccess(isReplicated)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (FOLLOWER.equals(currentRole)) {
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
            int term = request.getTerm();
            int leaderId = request.getLeaderId();
            // accept the new leader
            if (term > theNodeStatus.currentTerm) {
                theNodeStatus.currentTerm = term;
                theNodeStatus.votedFor = null;
                theNodeStatus.currentRole = FOLLOWER;
                theNodeStatus.currentLeader = leaderId;
            }

            if (term == theNodeStatus.currentTerm && CANDIDATE.equals(theNodeStatus.currentRole)) {
                theNodeStatus.currentRole = FOLLOWER;
                theNodeStatus.currentLeader = leaderId;
            }

            int logLength = theNodeStatus.log.size();
            boolean logOk = (request.getEntriesCount() >= logLength) &&
                    (logLength == 0 || request.getPrevLogTerm() == theNodeStatus.log.get(logLength - 1).term);

            AppendEntriesResponse response = null;
            if (term == theNodeStatus.currentTerm && logOk) {
                appendEntries(request.getPrevLogIndex(), request.getLeaderCommit(), request.getEntriesList());
                // AppendEntries(logLength, leaderCommit, entries)
                Integer ack = logLength + request.getEntriesCount();
                // send (LogResponse, nodeId, currentTerm, ack, true) to leaderId
                response = AppendEntriesResponse.newBuilder()
                        .setSuccess(true)
                        .setTerm(theNodeStatus.currentTerm)
                        .build();
            } else {
                // send (LogResponse, nodeId, currentTerm, 0, false) to leaderId
                response = AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .setTerm(theNodeStatus.currentTerm)
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        if (CANDIDATE.equals(currentRole)) {
            // ask for votes
        }
    }

    private void appendEntries(int logLength, int leaderCommit, List<LogEntry> entries) {
        //        function AppendEntries(logLength, leaderCommit, entries)
//        if entries.length > 0 ∧ log.length > logLength then
//        if log[logLength].term 6= entries[0].term then
//        log := hlog[0], log[1], . . . , log[logLength − 1]i
//        end if
//        end if

        if (entries.size() > 0 && theNodeStatus.log.size() > logLength) {
            if (theNodeStatus.log.get(logLength).term != entries.get(0).getTerm()) {
                // log := hlog[0], log[1], . . . , log[logLength − 1]i
                // leave as it is
            }
        }

//        if logLength + entries.length > log.length then
//        for i := log.length − logLength to entries.length − 1 do
//            append entries[i] to log
//        end for
//        end if
        if (logLength + entries.size() > theNodeStatus.log.size()) {
            for (int i = theNodeStatus.log.size() - logLength; i < entries.size() - 1; i++) {
                LogEntry logEntry = entries.get(i);
                theNodeStatus.log.add(new TheNodeStatus.LogEntry(logEntry.getTerm(), logEntry.getMsg()));
            }
        }

        if (leaderCommit > theNodeStatus.commitLength) {
//        ?????
//        for i := commitLength to leaderCommit − 1 do
//            deliver log[i].msg to the application
//        end for
            theNodeStatus.commitLength = leaderCommit;
        }
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {

        // from request
        Integer cId = request.getCandidateId();
        Integer cTerm = request.getTerm();
        Integer cLogLength = request.getLastLogIndex();
        Integer cLogTerm = request.getLastLogTerm();

        // at nodeId
        Integer myLogTerm = theNodeStatus.log.get(theNodeStatus.log.size() - 1).term;
        Boolean logOk =
                (cLogLength > myLogTerm) ||
                        (cLogTerm == myLogTerm && cLogLength >= theNodeStatus.log.size());
        Boolean termOk =
                (cTerm > theNodeStatus.currentTerm) ||
                        (cTerm == theNodeStatus.currentTerm &&
                                (theNodeStatus.votedFor == null || theNodeStatus.votedFor == cId));
        RequestVoteResponse response = null;
        if (logOk && termOk) {
            theNodeStatus.currentTerm = cTerm;
            theNodeStatus.currentRole = FOLLOWER;
            theNodeStatus.votedFor = cId;

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

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
