package com.github.kagkarlsson.scheduler.example;

import com.github.kagkarlsson.scheduler.RedisScheduler;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class RedisSchedulerMain {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSchedulerMain.class);

    public static void main(String[] args) {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        OneTimeTask<TaskData> oneTimeTask = Tasks.oneTime("task-1", TaskData.class).execute(((taskInstance, executionContext) -> {
            LOG.info("Executing task={} id={} data={}", taskInstance.getTaskName(), taskInstance.getId(), taskInstance.getData());
        }));

        CustomTask<TaskData> customTask = Tasks.custom("task-2", TaskData.class).execute(((taskInstance, executionContext) -> {

            return new CompletionHandler<TaskData>() {
                @Override
                public void complete(ExecutionComplete executionComplete, ExecutionOperations<TaskData> executionOperations) {
                    TaskData oldTask = (TaskData) executionComplete.getExecution().taskInstance.getData();
                    String newData = oldTask.data.substring(0, oldTask.data.length() - 1);
                    if (newData.length() <= 1) {
                        LOG.info("Stopping task={} id={} data={} executionTime={}", taskInstance.getTaskName(), taskInstance.getId(), taskInstance.getData(), executionContext.getExecution().executionTime);
                        executionOperations.stop();
                    } else {
                        TaskData newTask = new TaskData(newData);
                        Instant nextExecutionTime = Instant.now().plusSeconds(3);
                        executionOperations.reschedule(executionComplete, nextExecutionTime, newTask);
                        LOG.info("Rescheduling task={} id={} data={} executionTime={} nextExecutionTime={}", taskInstance.getTaskName(), taskInstance.getId(), taskInstance.getData(), executionContext.getExecution().executionTime, nextExecutionTime);
                    }

                }
            };
        }));
        Scheduler scheduler = RedisScheduler
                .create(redisClient, oneTimeTask, customTask)
                .pollingInterval(Duration.ofSeconds(5))
                .threads(5)
                .build();

        scheduler.schedule(oneTimeTask.instance("id1", new TaskData("some-data")), Instant.now());
        for (int i = 0; i < 10; i++) {
            scheduler.schedule(customTask.instance("id" + i, new TaskData("data")), Instant.now());
        }

        scheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal.");
            scheduler.stop();
        }));
    }

    public static class TaskData implements Serializable {
        public String data;

        public TaskData(String data) {
            this.data = data == null ? "" : data;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TaskData o = (TaskData) obj;
            return this.data.equals(o.data);

        }

        @Override
        public int hashCode() {
            return Objects.hash(this.data);
        }

        @Override
        public String toString() {
            return "TaskData{" +
                    "data='" + data + '\'' +
                    '}';
        }
    }
}
