package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.exceptions.ExecutionException;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RedisTaskRepository implements TaskRepository {
    private static final Logger LOG = LoggerFactory.getLogger(RedisTaskRepository.class);
    private final RedisClient client;

    private final Serializer serializer;

    private final TaskResolver taskResolver;

    private final SchedulerName schedulerName;

    private final String tableName;

    private final static String TIME_INDEX_KEY = "exectime.index";

    // TODO: generalized redis client?
    public RedisTaskRepository(RedisClient client, TaskResolver taskResolver, SchedulerName schedulerName, Serializer serializer, String tableName) {
        this.client = client;
        this.serializer = serializer;
        this.taskResolver = taskResolver;
        this.schedulerName = schedulerName;
        this.tableName = tableName;
    }

    @Override
    public boolean createIfNotExists(SchedulableInstance schedulableInstance) {
        TaskInstance taskInstance = schedulableInstance.getTaskInstance();
        return withConnection((c) -> {
            if (c.hget(taskInstance.getTaskName(), taskInstance.getId()) != null) {
                LOG.debug("Execution already existed. Will not create.");
                return false;
            }
            // TODO: better execution storage pattern?
            Instant now = Instant.now();
            String res = c.hmset(RedisExecutionUtils.buildRedisKey(taskInstance.getTaskName(), taskInstance.getId()), RedisExecutionUtils.newExecution(schedulableInstance.getNextExecutionTime(now), taskInstance.getData()));
            c.zadd(TIME_INDEX_KEY, schedulableInstance.getNextExecutionTime(now).toEpochMilli(), RedisExecutionUtils.buildRedisKey(taskInstance.getTaskName(), taskInstance.getId()));
            return "OK".equals(res);
        });
    }

    @Override
    public List<Execution> getDue(Instant now, int limit) {
        List<TaskResolver.UnresolvedTask> unresolvedTasks = taskResolver.getUnresolved();
        List<Execution> dueExecutions = new ArrayList<>();
        withConnection((c) -> {
            Set<String> taskNamesNotInUse = unresolvedTasks.stream().map(t -> t.getTaskName() + ":").collect(Collectors.toSet());
            // TODO: better way to realize select where taskName not in (....) in Redis?
            List<Object> dueKeys = c.zrangebyscore(TIME_INDEX_KEY, Range.create(0, now.toEpochMilli()), Limit.from(limit));
            dueKeys.forEach(rawK -> {
                String k = (String) rawK;
                String[] nameId = k.split(":");
                if (taskNamesNotInUse.contains(nameId[0] + ":")) {
                    return;
                }
                Optional<Task> task = taskResolver.resolve(nameId[0]);
                task.ifPresentOrElse(t -> {
                    boolean picked = (boolean) c.hget(k, "picked");
                    Instant execTime = (Instant) c.hget(k, "executionTime");
                    if (picked) {
                        return;
                    }

                    Supplier dataSupplier = memoize(() -> task.get().getDataClass().cast(c.get("data")));
                    Execution e = new Execution((Instant) c.hget(k, "executionTime"), new TaskInstance(nameId[0], nameId[1], dataSupplier), picked, (String) c.hget(k, "pickedBy"), (Instant) c.hget(k, "lastSuccess"), (Instant) c.hget(k, "lastFailure"), (int) c.hget(k, "consecutiveFailures"), (Instant) c.hget(k, "lastHeartbeat"), (long) c.hget(k, "version"));

                    dueExecutions.add(e);
                }, () -> {
                    LOG.warn("Failed to find implementation for task with name '{}'. Execution will be excluded from due. Either delete the execution from the database, or add an implementation for it. The scheduler may be configured to automatically delete unresolved tasks after a certain period of time.", nameId[0]);
                });

            });
            return true;
        });
        //dueExecutions.sort(Comparator.comparing(o -> o.executionTime));
        //return dueExecutions.subList(0, Math.min(limit, dueExecutions.size()));
        return dueExecutions;
    }

    @Override
    public void getScheduledExecutions(ScheduledExecutionsFilter scheduledExecutionsFilter, Consumer<Execution> consumer) {
        List<TaskResolver.UnresolvedTask> unresolvedTasks = taskResolver.getUnresolved();
        List<Execution> scheduledExecutions = new ArrayList<>();
        withConnection((c) -> {
            Set<String> taskNamesNotInUse = unresolvedTasks.stream().map(t -> t.getTaskName() + ":").collect(Collectors.toSet());
            // TODO: better way to realize select where taskName not in (....) in Redis?
            c.keys("*").forEach(k -> {
                if (TIME_INDEX_KEY.equals(k)) {
                    return;
                }
                String[] nameId = k.split(":");
                if (taskNamesNotInUse.contains(nameId[0] + ":")) {
                    return;
                }
                Optional<Task> task = taskResolver.resolve(nameId[0]);
                task.ifPresentOrElse(t -> {
                    Optional<Boolean> optionalPicked = scheduledExecutionsFilter.getPickedValue();
                    boolean picked = (boolean) c.hget(k, "picked");
                    if (optionalPicked.isPresent() && optionalPicked.get() != picked) {
                        return;
                    }

                    Supplier dataSupplier = memoize(() -> task.get().getDataClass().cast(c.get("data")));
                    Execution e = new Execution((Instant) c.hget(k, "executionTime"), new TaskInstance(nameId[0], nameId[1], dataSupplier), picked, (String) c.hget(k, "pickedBy"), (Instant) c.hget(k, "lastSuccess"), (Instant) c.hget(k, "lastFailure"), (int) c.hget(k, "consecutiveFailures"), (Instant) c.hget(k, "lastHeartbeat"), (long) c.hget(k, "version"));
                    scheduledExecutions.add(e);
                }, () -> {
                    LOG.warn("Failed to find implementation for task with name '{}'. Execution will be excluded from due. Either delete the execution from the database, or add an implementation for it. The scheduler may be configured to automatically delete unresolved tasks after a certain period of time.", nameId[0]);
                });

            });
            return true;
        });
        scheduledExecutions.sort(Comparator.comparing(o -> o.executionTime));
        scheduledExecutions.forEach(consumer);
    }

    @Override
    public void getScheduledExecutions(ScheduledExecutionsFilter scheduledExecutionsFilter, String taskName, Consumer<Execution> consumer) {
        List<Execution> scheduledExecutions = new ArrayList<>();
        withConnection((c) -> {
            // TODO: better way to realize select where taskName not in (....) in Redis?
            c.keys(taskName + ":*").forEach(k -> {
                String[] nameId = k.split(":");
                Optional<Task> task = taskResolver.resolve(nameId[0]);
                task.ifPresentOrElse(t -> {
                    Optional<Boolean> optionalPicked = scheduledExecutionsFilter.getPickedValue();
                    boolean picked = (boolean) c.hget(k, "picked");
                    if (optionalPicked.isPresent() && optionalPicked.get() != picked) {
                        return;
                    }

                    Supplier dataSupplier = memoize(() -> task.get().getDataClass().cast(c.get("data")));
                    Map<String, Object> eMap = c.hgetall(k);
                    Execution e = new Execution((Instant) eMap.get("executionTime"), new TaskInstance(nameId[0], nameId[1], dataSupplier), picked, (String) eMap.get("pickedBy"), (Instant) eMap.get("lastSuccess"), (Instant) eMap.get("lastFailure"), (int) eMap.get("consecutiveFailures"), (Instant) eMap.get("lastHeartbeat"), (long) eMap.get("version"));
                    scheduledExecutions.add(e);
                }, () -> {
                    LOG.warn("Failed to find implementation for task with name '{}'. Execution will be excluded from due. Either delete the execution from the database, or add an implementation for it. The scheduler may be configured to automatically delete unresolved tasks after a certain period of time.", nameId[0]);
                });

            });
            return true;
        });
        scheduledExecutions.sort(Comparator.comparing(o -> o.executionTime));
        scheduledExecutions.forEach(consumer);
    }

    @Override
    public List<Execution> lockAndGetDue(Instant now, int limit) {
        throw new UnsupportedOperationException("lockAndFetch not supported for Redis");
    }

    @Override
    public void remove(Execution execution) {
        long removed = withConnection((c) -> {
            String key = RedisExecutionUtils.buildRedisKey(execution.taskInstance.getTaskName(), execution.taskInstance.getId());
            c.zrem(TIME_INDEX_KEY, key);
            return c.del(key);
        });

        if (removed <= 0) {
            throw new ExecutionException("Expected one execution to be removed, but removed " + removed + ". Indicates a bug.", execution);
        }
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return reschedule(execution, nextExecutionTime, null, lastSuccess, lastFailure, consecutiveFailures);
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Object newData, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        Map<String, Object> toUpdate = new HashMap<>() {{
            put("executionTime", nextExecutionTime);
            put("picked", false);
            put("pickedBy", null);
            put("consecutiveFailures", consecutiveFailures);
            put("lastHeartbeat", null);
            put("lastFailure", lastFailure);
            put("lastSuccess", lastSuccess);
        }};

        if (newData != null) {
            toUpdate.put("data", newData);
        }

        return withConnection((c) -> {
            String redisKey = RedisExecutionUtils.buildRedisKey(execution.taskInstance.getTaskName(), execution.taskInstance.getId());
            long curVersion = (long) c.hget(redisKey, "version");
            toUpdate.put("version", curVersion + 1);
            c.hset(redisKey, toUpdate);
            c.zadd(TIME_INDEX_KEY, nextExecutionTime.toEpochMilli(), redisKey);
            return true;
        });
        // TODO: decide success?
    }

    @Override
    public Optional<Execution> pick(Execution execution, Instant timePicked) {
        Optional<Execution> executionOptional = getExecution(execution.taskInstance);
        if (executionOptional.isEmpty()) {
            LOG.trace("Failed to get execution {}.", execution);
            return executionOptional;
        }
        Execution e = executionOptional.get();
        if (e.picked || e.version != execution.version) {
            LOG.trace("Failed to pick execution {}. It must have been picked by another scheduler.", execution);
            return Optional.empty();
        }
        Map<String, Object> toUpdate = new HashMap<>() {{
            put("version", e.version + 1);
            put("picked", true);
            put("pickedBy", StringUtils.truncate(schedulerName.getName(), 50));
            put("lastHeartbeat", timePicked);
        }};
        String res = withConnection((c) -> {
            return c.hmset(RedisExecutionUtils.buildRedisKey(execution.taskInstance.getTaskName(), execution.taskInstance.getId()), toUpdate);
        });
        if (!"OK".equals(res)) {
            throw new IllegalStateException("Failed to update execution status: " + e);
        }
        Optional<Execution> updatedExecution = getExecution(execution.taskInstance);
        if (updatedExecution.isEmpty()) {
            throw new IllegalStateException("Unable to find picked execution. Must have been deleted by another thread. Indicates a bug.");
        }

        if (!updatedExecution.get().isPicked()) {
            throw new IllegalStateException("Picked execution does not have expected state in database: " + updatedExecution.get());
        }

        return updatedExecution;
    }

    @Override
    public List<Execution> getDeadExecutions(Instant olderThan) {
        List<TaskResolver.UnresolvedTask> unresolvedTasks = taskResolver.getUnresolved();
        Set<String> taskNamesNotInUse = unresolvedTasks.stream().map(t -> t.getTaskName() + ":").collect(Collectors.toSet());
        List<Execution> executions = new ArrayList<>();
        return withConnection((c) -> {
            c.keys("*").forEach(k -> {
                if (TIME_INDEX_KEY.equals(k)) {
                    return;
                }
                String[] nameId = k.split(":");
                if (taskNamesNotInUse.contains(nameId[0] + ":")) {
                    return;
                }
                Optional<Execution> execution = getExecution(nameId[0], nameId[1]);
                execution.ifPresent(e -> {
                    if (e.isPicked() && !e.lastHeartbeat.isAfter(olderThan)) {
                        executions.add(e);
                    }
                });
            });
            executions.sort(Comparator.comparing(e -> e.lastHeartbeat));
            return executions;
        });
    }

    @Override
    public void updateHeartbeat(Execution execution, Instant newHeartbeat) {
        Optional<Execution> executionOptional = getExecution(execution.taskInstance);
        executionOptional.ifPresentOrElse(e -> {
            withConnection((c) -> {
                c.hset(RedisExecutionUtils.buildRedisKey(e.taskInstance.getTaskName(), e.taskInstance.getId()), "lastHeartbeat", newHeartbeat);
                LOG.debug("Updated heartbeat for execution: " + e);
                return true;
            });
        }, () -> LOG.trace("Did not update heartbeat. Execution {} must have been removed or rescheduled.", execution));

    }

    @Override
    public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
        List<TaskResolver.UnresolvedTask> unresolvedTasks = taskResolver.getUnresolved();
        Set<String> taskNamesNotInUse = unresolvedTasks.stream().map(t -> t.getTaskName() + ":").collect(Collectors.toSet());
        List<Execution> executions = new ArrayList<>();
        return withConnection((c) -> {
            c.keys("*").forEach(k -> {
                if (TIME_INDEX_KEY.equals(k)) {
                    return;
                }
                String[] nameId = k.split(":");
                if (taskNamesNotInUse.contains(nameId[0] + ":")) {
                    return;
                }
                Optional<Execution> execution = getExecution(nameId[0], nameId[1]);
                execution.ifPresent(e -> {
                    if (e.lastSuccess == null && e.lastFailure != null ||
                            e.lastFailure != null && e.lastSuccess.isBefore(Instant.now().minus(interval))) {
                        executions.add(e);
                    }
                });
            });
            return executions;
        });
    }

    public Optional<Execution> getExecution(TaskInstance taskInstance) {
        return getExecution(taskInstance.getTaskName(), taskInstance.getId());
    }
    @Override
    public Optional<Execution> getExecution(String taskName, String id) {
        Map<String, Object> eMap = withConnection((c) -> {
            return c.hgetall(RedisExecutionUtils.buildRedisKey(taskName, id));
        });

        Optional<Task> task = taskResolver.resolve(taskName);
        if (eMap == null || eMap.isEmpty() || !task.isPresent()) {
            LOG.warn("Failed to find implementation for task with name '{}'. Execution will be excluded from due. Either delete the execution from the database, or add an implementation for it. The scheduler may be configured to automatically delete unresolved tasks after a certain period of time.", taskName);
            return Optional.empty();
        }

        Supplier dataSupplier = memoize(() -> task.get().getDataClass().cast(eMap.get("data")));
        Execution e = new Execution((Instant) eMap.get("executionTime"), new TaskInstance(taskName, id, dataSupplier), (boolean) eMap.get("picked"), (String) eMap.get("pickedBy"), (Instant) eMap.get("lastSuccess"), (Instant) eMap.get("lastFailure"), (int) eMap.get("consecutiveFailures"), (Instant) eMap.get("lastHeartbeat"), (long) eMap.get("version"));

        return Optional.of(e);
    }

    @Override
    public int removeExecutions(String taskName) {
        return withConnection((c) -> {
            List<String> keys = c.keys(taskName + ":*");
            c.zrem(TIME_INDEX_KEY, keys.toArray(new String[1]));
            return c.del(keys.toArray(new String[1])).intValue();
        });
    }

    @Override
    public void checkSupportsLockAndFetch() {
        throw new IllegalArgumentException("Database using Redis does not support lock-and-fetch polling (i.e. Select-for-update)");
    }

    private void forEachInstance() {

    }

    private <T> T withConnection(Function<RedisCommands<String, Object>, T> doWithConnection) {
        StatefulRedisConnection<String, Object> connection = RedisConnection.getConnection(client);

        T res;
        RedisCommands<String, Object> syncCommands = connection.sync();
        res = doWithConnection.apply(syncCommands);
        return res;
    }

    private static <T> Supplier<T> memoize(Supplier<T> original) {
        return new Supplier<T>() {
            Supplier<T> delegate = this::firstTime;
            boolean initialized;

            public T get() {
                return delegate.get();
            }

            private synchronized T firstTime() {
                if (!initialized) {
                    T value = original.get();
                    delegate = () -> value;
                    initialized = true;
                }
                return delegate.get();
            }
        };
    }
}
