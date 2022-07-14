package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import io.lettuce.core.RedisClient;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class RedisScheduler extends Scheduler {
    protected RedisScheduler(Clock clock, TaskRepository schedulerTaskRepository, TaskRepository clientTaskRepository, TaskResolver taskResolver, int threadpoolSize, ExecutorService executorService, SchedulerName schedulerName, Waiter executeDueWaiter, Duration heartbeatInterval, boolean enableImmediateExecution, StatsRegistry statsRegistry, PollingStrategyConfig pollingStrategyConfig, Duration deleteUnresolvedAfter, Duration shutdownMaxWait, LogLevel logLevel, boolean logStackTrace, List<OnStartup> onStartup) {
        super(clock, schedulerTaskRepository, clientTaskRepository, taskResolver, threadpoolSize, executorService, schedulerName, executeDueWaiter, heartbeatInterval, enableImmediateExecution, statsRegistry, pollingStrategyConfig, deleteUnresolvedAfter, shutdownMaxWait, logLevel, logStackTrace, onStartup);
    }

    public static RedisSchedulerBuilder create(RedisClient redisClient, Task<?>... knownTasks) {
        return create(redisClient, Arrays.asList(knownTasks));
    }

    public static RedisSchedulerBuilder create(RedisClient redisClient, List<Task<?>> knownTasks) {
        return new RedisSchedulerBuilder(redisClient, knownTasks);
    }
}
