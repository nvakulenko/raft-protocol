package ua.edu.ucu.ds;

import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import ua.edu.ucu.*;
import ua.edu.ucu.AppendEntriesRequest.LogEntry;

import java.time.Instant;
import java.util.List;

import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.CANDIDATE;
import static ua.edu.ucu.ds.TheNodeStatus.NodeRole.FOLLOWER;

@GRpcService
public class RaftGrpcService extends RaftProtocolGrpc.RaftProtocolImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGrpcService.class);

    @Autowired
    private TheNodeStatus theNodeStatus;
    @Autowired
    private ReplicationService replicationService;

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {
        TheNodeStatus.NodeRole currentRole = theNodeStatus.currentRole;
        Integer nodeId = theNodeStatus.nodeId;
        Integer currentLeader = theNodeStatus.currentLeader;
        Integer currentTerm = theNodeStatus.currentTerm;
        Integer commitLength = theNodeStatus.commitLength;

        LOGGER.info("Node id {} state: Node role is: {}. Current leader: {}. Current term: {}. Commit length {}" +
                " Received AppendEntriesRequest {}", nodeId, currentRole, currentLeader, currentTerm, commitLength, request);

        // if is heartbeat -> update last leader ping time
        // If there are no new messages, entries is the empty list.
        // LogRequest messages with entries = [] serve as heartbeats, letting
        // followers know that the leader is still alive.
        if (currentLeader != null && currentLeader == request.getLeaderId()) {
            // check if entities is empty and return ???
            // is it enouth
            theNodeStatus.lastLeaderAppendTime = Instant.now();
        }

        if (TheNodeStatus.NodeRole.LEADER.equals(currentRole)) {
            // generate heart beats and replicate log
            List<LogEntry> entriesList = request.getEntriesList();
            boolean isReplicated = replicationService.replicateLog(entriesList.get(0).getMsg());

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
                        .setAck(ack)
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

        if (entries.size() > 0 && theNodeStatus.log.size() > logLength) {
            if (theNodeStatus.log.get(logLength).term != entries.get(0).getTerm()) {
                // log := hlog[0], log[1], . . . , log[logLength − 1]i

            }
        }

        if (logLength + entries.size() > theNodeStatus.log.size()) {
            for (int i = theNodeStatus.log.size() - logLength; i < entries.size() - 1; i++) {
                LogEntry logEntry = entries.get(i);
                theNodeStatus.log.add(new TheNodeStatus.LogEntry(logEntry.getTerm(), logEntry.getMsg()));
            }
        }

        if (leaderCommit > theNodeStatus.commitLength) {
            theNodeStatus.commitLength = leaderCommit;
        }
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {

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
        Boolean termOk =
                (cTerm > theNodeStatus.currentTerm) ||
                        (cTerm == theNodeStatus.currentTerm &&
                                (theNodeStatus.votedFor == null || theNodeStatus.votedFor == cId));
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

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
