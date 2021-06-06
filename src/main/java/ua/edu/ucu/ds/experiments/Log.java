package ua.edu.ucu.ds.experiments;

import lombok.Builder;
import org.springframework.stereotype.Component;
import ua.edu.ucu.AppendEntriesRequest;

import java.util.concurrent.ConcurrentSkipListMap;

@Component
public class Log {

//    @Builder
//    public class LogEntry {
//        AppendEntriesRequest request;
//    }
//    private ConcurrentSkipListMap<Integer, LogEntry> logEntries = new ConcurrentSkipListMap();
//
//    public LogEntry putAndGetPreviousLogEntry(Integer key, AppendEntriesRequest newLogEntry) {
//        var previousEntry = logEntries.get(key - 1);
//        logEntries.put(key, LogEntry.builder().request(newLogEntry).build());
//        return previousEntry;
//    }
}
