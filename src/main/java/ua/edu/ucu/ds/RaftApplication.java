package ua.edu.ucu.ds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RaftApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftApplication.class);

    public static void main(String[] args) {
        LOGGER.info("Starting the Raft protocol app...");
        SpringApplication.run(RaftApplication.class, args);

        // startup: State to FOLLOWER
        // no ping from LEADER -> become CANDIDATE
    }

}
