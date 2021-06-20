package ua.edu.ucu.ds.grpc;

import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import ua.edu.ucu.*;
import ua.edu.ucu.ds.service.ElectionService;
import ua.edu.ucu.ds.service.ReplicationService;
import ua.edu.ucu.ds.service.TheNodeStatus;

import static ua.edu.ucu.ds.service.TheNodeStatus.NodeRole.FOLLOWER;

@GRpcService
public class RaftGrpcService extends RaftProtocolGrpc.RaftProtocolImplBase {

    @Autowired
    private ReplicationService replicationService;

    @Autowired
    private ElectionService electionService;

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {
        AppendEntriesResponse appendEntriesResponse = replicationService.appendEntries(request);

        responseObserver.onNext(appendEntriesResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {

        RequestVoteResponse response = electionService.requestVote(request);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
