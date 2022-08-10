package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.example.RedisSchedulerMain;
import com.github.kagkarlsson.scheduler.exceptions.ExecutionException;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.Assert;
import org.junit.jupiter.api.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RedisTaskRepositoryTest {
    private StatefulRedisConnection<String, Object> connection;

    private TaskRepository taskRepository;

    private RedisCommands<String, Object> syncCommands;

    private RedisClient redisClient;

    private SchedulerName schedulerName;

    private OneTimeTask<Void> oneTimeTask;

    private OneTimeTask<Integer> oneTimeTaskWithData;

    private Instant now;

    private Serializer serializer;

    private final static String TIME_INDEX_KEY = "exectime.index";

    @BeforeAll
    void init() {
        schedulerName = new SchedulerName.Hostname();
        redisClient = RedisClient.create("redis://localhost:6379");
        connection = RedisConnection.getConnection(redisClient);
        syncCommands = connection.sync();
        oneTimeTask = new OneTimeTask<>("test-1", Void.class) {
            @Override
            public void executeOnce(TaskInstance<Void> taskInstance, ExecutionContext executionContext) {

            }
        };
        oneTimeTaskWithData = new OneTimeTask<>("test-2", Integer.class) {
            @Override
            public void executeOnce(TaskInstance<Integer> taskInstance, ExecutionContext executionContext) {

            }
        };
        now = Instant.now();
        List<Task<?>> knownTasks = new ArrayList<>(){{
            add(oneTimeTask);
            add(oneTimeTaskWithData);
            add(Tasks.oneTime("test-3", Void.class).execute((inst, ctx) -> {}));
        }};
        serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
        TaskResolver taskResolver = new TaskResolver(new StatsRegistry.DefaultStatsRegistry(), knownTasks);
        taskRepository = new RedisTaskRepository(redisClient, taskResolver, schedulerName, serializer, "scheduled_tasks");
    }

    @Test
    void testSerialization() {
        syncCommands.hset("k", "f", new RedisSchedulerMain.TaskData("test"));
        Object res = syncCommands.hget("k", "f");
        Assertions.assertEquals(new RedisSchedulerMain.TaskData("test"), res);
    }

    @Test
    void shouldCreateNewExecution() {
        Integer data = 10;
        boolean res = taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTaskWithData.instance("1", 10), now));
        Assertions.assertTrue(res);
        Map<String, Object> execution = syncCommands.hgetall(RedisExecutionUtils.buildRedisKey(oneTimeTaskWithData.getName(), "1"));
        Assertions.assertFalse(execution.isEmpty());
        assert data.equals(execution.get("data"));
        Assertions.assertEquals(now.toEpochMilli(), syncCommands.zscore(TIME_INDEX_KEY, RedisExecutionUtils.buildRedisKey(oneTimeTaskWithData.getName(), "1")).longValue());
    }

    @Test
    void shouldGetScheduledExecutions() {
        List<String> tasks = new ArrayList<>();
        taskRepository.createIfNotExists(new SchedulableInstance<String>() {
            @Override
            public TaskInstance<String> getTaskInstance() {
                return new TaskInstance<>("test-1", "1", "test-data");
            }

            @Override
            public Instant getNextExecutionTime(Instant instant) {
                return instant.plusSeconds(1);
            }
        });
        taskRepository.createIfNotExists(new SchedulableInstance<String>() {
            @Override
            public TaskInstance<String> getTaskInstance() {
                return new TaskInstance<>("test-2", "1", "test-data");
            }

            @Override
            public Instant getNextExecutionTime(Instant instant) {
                return instant;
            }
        });
        taskRepository.getScheduledExecutions(ScheduledExecutionsFilter.all(), (e) -> {
            tasks.add(e.taskInstance.getTaskName() + ":" + e.taskInstance.getId());
        });
        assertThat(tasks, equalTo(new ArrayList<>(){{add("test-2:1"); add("test-1:1");}}));
    }

    @Test
    void shouldGetScheduledExecutionsByName() {
        List<String> tasks = new ArrayList<>();
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-1", "1", 5));
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-1", "2", 0));
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-2", "1", 0));
        taskRepository.getScheduledExecutions(ScheduledExecutionsFilter.all(), "test-1", (e) -> {
            tasks.add(e.taskInstance.getTaskName() + ":" + e.taskInstance.getId());
        });
        assertThat(tasks, equalTo(new ArrayList<>(){{add("test-1:2"); add("test-1:1");}}));
    }

    @Test
    void shouldGetDueExecutions() {
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTask.instance("1"), now.plusSeconds(5)));
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTask.instance("2"), now.plusSeconds(1)));
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTaskWithData.instance("1", 10), now));
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-4", "1", 0));
        List<String> executions = taskRepository.getDue(now.plusSeconds(1), 10).stream().map(e -> e.taskInstance.getTaskName() + ":" + e.taskInstance.getId()).collect(Collectors.toList());
        assertThat(executions, equalTo(new ArrayList<>(){{add("test-2:1"); add("test-1:2");}}));
        executions = taskRepository.getDue(Instant.now().plusSeconds(1), 1).stream().map(e -> e.taskInstance.getTaskName() + ":" + e.taskInstance.getId()).collect(Collectors.toList());
        assertThat(executions, equalTo(new ArrayList<>(){{add("test-2:1");}}));
    }

    @Test
    void shouldRescheduleWithoutNewData() {
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTaskWithData.instance("1", 1), now));
        Execution oldE = taskRepository.getExecution(oneTimeTaskWithData.getName(), "1").get();
        Instant nextExecutionTime = now.plusSeconds(10);
        taskRepository.reschedule(oldE, nextExecutionTime, null, null, null, 0);
        Execution newE = taskRepository.getExecution(oneTimeTaskWithData.getName(), "1").get();
        Assertions.assertEquals(nextExecutionTime, newE.executionTime);
        Assertions.assertEquals(1, newE.taskInstance.getData());
        Assertions.assertEquals((double) nextExecutionTime.toEpochMilli(), syncCommands.zscore(TIME_INDEX_KEY, RedisExecutionUtils.buildRedisKey(newE.taskInstance.getTaskName(), newE.taskInstance.getId())));
    }

    @Test
    void shouldRescheduleWithNewData() {
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTaskWithData.instance("1", 1), now));
        Execution oldE = taskRepository.getExecution(oneTimeTaskWithData.getName(), "1").get();
        Instant nextExecutionTime = now.plusSeconds(10);
        taskRepository.reschedule(oldE, nextExecutionTime, 2, null, null, 0);
        Execution newE = taskRepository.getExecution(oneTimeTaskWithData.getName(), "1").get();
        Assertions.assertEquals(2, newE.taskInstance.getData());
    }

    @Test
    void shouldGetExecution() {
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTaskWithData.instance("1", 1), now));
        Optional<Execution> execution = taskRepository.getExecution(oneTimeTaskWithData.getName(), "1");
        Assertions.assertEquals(1, execution.get().taskInstance.getData());
        Assertions.assertEquals(oneTimeTaskWithData.getName(), execution.get().taskInstance.getTaskName());
        Assertions.assertEquals("1", execution.get().taskInstance.getId());
        execution = taskRepository.getExecution(oneTimeTask.getName(), "1");
        assert execution.isEmpty();
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-4", "1", 0));
        execution = taskRepository.getExecution("test-4", "1");
        assert execution.isEmpty();
    }

    @Test
    void shouldRemoveExecution() {
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-1", "1", 0));
        Execution execution = taskRepository.getExecution("test-1", "1").get();
        taskRepository.remove(execution);
        Optional<Execution> executionOptional = taskRepository.getExecution("test-1", "1");
        assert executionOptional.isEmpty();
        Assertions.assertNull(syncCommands.zrank(TIME_INDEX_KEY, RedisExecutionUtils.buildRedisKey(execution.taskInstance.getTaskName(), execution.taskInstance.getId())));
        Assert.assertThrows(ExecutionException.class, () -> {
            taskRepository.remove(execution);
        });
    }

    @Test
    void shouldPickExecution() {
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-1", "1", 0));
        Optional<Execution> execution = taskRepository.getExecution("test-1", "1");
        Instant timePicked = Instant.now();
        Optional<Execution> pickedExecution = taskRepository.pick(execution.get(), timePicked);
        Assertions.assertTrue(pickedExecution.isPresent());
        Assertions.assertEquals(pickedExecution.get().version, 2L);
        Assertions.assertTrue(pickedExecution.get().picked);
        Assertions.assertEquals(pickedExecution.get().pickedBy, StringUtils.truncate(schedulerName.getName(), 50));
        Assertions.assertEquals(pickedExecution.get().lastHeartbeat, timePicked);
        pickedExecution = taskRepository.pick(execution.get(), timePicked);
        Assertions.assertTrue(pickedExecution.isEmpty());
    }

    @Test
    void shouldGetDeadExecutions() {
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-1", "1", 0));
        taskRepository.createIfNotExists(TestUtils.newSchedulableInstance("test-1", "2", 0));
        Instant timePicked = Instant.now();
        Instant older = Instant.now().plusSeconds(10);
        Execution e11 = taskRepository.getExecution("test-1", "1").get();
        taskRepository.pick(e11, timePicked);
        List<Execution> executions = taskRepository.getDeadExecutions(older);
        assertThat(executions, hasSize(1));
        Assertions.assertEquals(executions.get(0).taskInstance.getId(), "1");
        Assertions.assertEquals(executions.get(0).taskInstance.getTaskName(), "test-1");
    }

    @Test
    void shouldUpdateHeartbeat() {
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTask.instance("1"), now));
        Execution execution = taskRepository.getExecution(oneTimeTask.getName(), "1").get();
        taskRepository.updateHeartbeat(execution, now.plusSeconds(10));
        execution = taskRepository.getExecution(oneTimeTask.getName(), "1").get();
        Assertions.assertEquals(execution.lastHeartbeat, now.plusSeconds(10));
    }

    @Test
    void shouldGetExecutionsFailingLongerThan() {
        final TaskInstance<Void> instance = oneTimeTask.instance("1");
        taskRepository.createIfNotExists(new SchedulableTaskInstance<>(instance, now));

        List<Execution> due = taskRepository.getDue(now, 10);
        assertThat(due, hasSize(1));

        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

        taskRepository.reschedule(due.get(0), now, now, null, 0);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(0));

        taskRepository.reschedule(due.get(0), now, null, now, 1);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofMinutes(1)), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofDays(1)), hasSize(1));

        taskRepository.reschedule(due.get(0), now, now.minus(Duration.ofMinutes(1)), now, 1);
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ZERO), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofSeconds(1)), hasSize(1));
        assertThat(taskRepository.getExecutionsFailingLongerThan(Duration.ofHours(1)), hasSize(0));
    }

    @Test
    void shouldRemoveExecutions() {
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTask.instance("1"), now));
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTask.instance("2"), now));
        taskRepository.createIfNotExists(new SchedulableTaskInstance(oneTimeTask.instance("3"), now));
        int res = taskRepository.removeExecutions(oneTimeTask.getName());
        Assertions.assertEquals(3, res);
        List<Execution> restExecutions = taskRepository.getDue(now, 10);
        assertThat(restExecutions, hasSize(0));
    }

    @AfterEach
    void end() {
        syncCommands.flushall();
    }

    @AfterAll
    void endAll() {
        connection.close();
        redisClient.shutdown();
    }
}
