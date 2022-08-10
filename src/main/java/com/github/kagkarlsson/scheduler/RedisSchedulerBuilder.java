package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.task.Task;
import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisSchedulerBuilder extends SchedulerBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSchedulerBuilder.class);

    private final RedisClient redisClient;

    protected boolean enableShutdownHook;

    public RedisSchedulerBuilder(RedisClient redisClient, List<Task<?>> knownTasks) {
        super(null, knownTasks);
        this.redisClient = redisClient;
    }

    public RedisSchedulerBuilder enableShutdownHook() {
        this.enableShutdownHook = true;
        return this;
    }

    @Override
    public Scheduler build() {
        if (this.schedulerName == null) {
            this.schedulerName = new SchedulerName.Hostname();
        }

        TaskResolver taskResolver = new TaskResolver(this.statsRegistry, this.clock, this.knownTasks);
        RedisTaskRepository schedulerTaskRepository = new RedisTaskRepository(redisClient, taskResolver, this.schedulerName, serializer, tableName);
        RedisTaskRepository clientTaskRepository = new RedisTaskRepository(redisClient, taskResolver, this.schedulerName, serializer, tableName);
        ExecutorService candidateExecutorService = this.executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService = Executors.newFixedThreadPool(this.executorThreads, ExecutorUtils.defaultThreadFactoryWithPrefix("db-scheduler-"));
        }

        LOG.info("Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s enable-immediate-execution={}, table-name={}, name={}", this.executorThreads, this.waiter.getWaitDuration().getSeconds(), this.heartbeatInterval.getSeconds(), this.enableImmediateExecution, this.tableName, this.schedulerName.getName());
        Scheduler scheduler = new RedisScheduler(this.clock, schedulerTaskRepository, clientTaskRepository, taskResolver, this.executorThreads, candidateExecutorService, this.schedulerName, this.waiter, this.heartbeatInterval, this.enableImmediateExecution, this.statsRegistry, this.pollingStrategyConfig, this.deleteUnresolvedAfter, this.shutdownMaxWait, this.logLevel, this.logStackTrace, this.startTasks);
        if (this.enableShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Received shutdown signal.");
                RedisConnection.closeConnection();
                scheduler.stop();
            }));
        }

        return scheduler;
    }
}
