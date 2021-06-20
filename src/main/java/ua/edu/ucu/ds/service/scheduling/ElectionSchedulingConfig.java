package ua.edu.ucu.ds.service.scheduling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import ua.edu.ucu.ds.service.ElectionService;
import ua.edu.ucu.ds.service.ReplicationService;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;

@Configuration
public class ElectionSchedulingConfig implements SchedulingConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionSchedulingConfig.class);

    private static final Integer MAX_REPLICATION_DELAY_IN_MILISECONDS = 8000;
    private static final Integer MIN_REPLICATION_DELAY_IN_MILISECONDS = 4000;

    private static final Integer MAX_ELECTION_DELAY_IN_MILISECONDS = 8000;
    private static final Integer MIN_ELECTION_DELAY_IN_MILISECONDS = 2000;

    @Autowired
    private ElectionService electionService;

    @Autowired
    private ReplicationService replicationService;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setScheduler(Executors.newSingleThreadScheduledExecutor());
        taskRegistrar.addTriggerTask(
                () -> electionService.initElection(),
                context -> {
                    Optional<Date> lastCompletionTime =
                            Optional.ofNullable(context.lastCompletionTime());
                    Instant nextExecutionTime =
                            lastCompletionTime.orElseGet(Date::new).toInstant()
                                    .plusMillis(getRandomTimeootInMillis(MAX_ELECTION_DELAY_IN_MILISECONDS,
                                            MIN_ELECTION_DELAY_IN_MILISECONDS));
                    Date nextExecutionDate = Date.from(nextExecutionTime);
                    LOGGER.info("Next leader election is scheduled to: " + nextExecutionDate);
                    return nextExecutionDate;
                }
        );

//        taskRegistrar.addTriggerTask(
//                () -> replicationService.replicateLog(),
//                context -> {
//                    Optional<Date> lastCompletionTime =
//                            Optional.ofNullable(context.lastCompletionTime());
//                    Instant nextExecutionTime =
//                            lastCompletionTime.orElseGet(Date::new).toInstant()
//                                    .plusMillis(getRandomTimeootInMillis(MAX_REPLICATION_DELAY_IN_MILISECONDS,
//                                            MIN_REPLICATION_DELAY_IN_MILISECONDS));
//                    Date nextExecutionDate = Date.from(nextExecutionTime);
//                    LOGGER.info("Next replication is scheduled to: " + nextExecutionDate);
//                    return nextExecutionDate;
//                }
//        );
    }

    private long getRandomTimeootInMillis(Integer maxDelay, Integer minDelay) {
        return (new Random().nextInt(maxDelay - minDelay) + maxDelay);
    }
}
