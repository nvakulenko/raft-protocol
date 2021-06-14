package ua.edu.ucu.ds.scheduling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import ua.edu.ucu.ds.ReplicationService;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
public class ReplicationSchedulingConfig /*implements SchedulingConfigurer*/ {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationSchedulingConfig.class);

    private static final Integer MAX_REPLICATION_DELAY_IN_MILISECONDS = 8000;
    private static final Integer MIN_REPLICATION_DELAY_IN_MILISECONDS = 4000;

    @Autowired
    private ReplicationService replicationService;

//    @Override
//    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
//        taskRegistrar.setScheduler(Executors.newSingleThreadScheduledExecutor());
//        taskRegistrar.addTriggerTask(
//                () -> replicationService.replicateLog(),
//                context -> {
//                    Optional<Date> lastCompletionTime =
//                            Optional.ofNullable(context.lastCompletionTime());
//                    Instant nextExecutionTime =
//                            lastCompletionTime.orElseGet(Date::new).toInstant()
//                                    .plusMillis(getRandomTimeoutinMillis());
//                    Date nextExecutionDate = Date.from(nextExecutionTime);
//                    LOGGER.info("Next replication is scheduled to: " + nextExecutionDate);
//                    return nextExecutionDate;
//                }
//        );
//    }

    private long getRandomTimeoutinMillis() {
        return (new Random().nextInt(MAX_REPLICATION_DELAY_IN_MILISECONDS - MIN_REPLICATION_DELAY_IN_MILISECONDS) + MIN_REPLICATION_DELAY_IN_MILISECONDS);
    }
}
