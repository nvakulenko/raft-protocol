package ua.edu.ucu.ds;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ua.edu.ucu.AppendEntriesRequest;
import ua.edu.ucu.AppendEntriesResponse;

@Service
public class LogReplicationService {

    @Autowired ConsensusService consensusService;

    public AppendEntriesResponse appendLog(AppendEntriesRequest request) {

        return AppendEntriesResponse.newBuilder().build();
    }
}
