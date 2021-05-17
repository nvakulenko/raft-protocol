package ua.edu.ucu.ds;

import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import ua.edu.ucu.*;

@GRpcService
public class RaftService extends RaftProtocolGrpc.RaftProtocolImplBase {

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        super.appendEntries(request, responseObserver);
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        super.requestVote(request, responseObserver);
    }
}
