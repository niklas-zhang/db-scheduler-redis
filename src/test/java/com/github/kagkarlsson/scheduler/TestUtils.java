package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;

import java.time.Instant;

public class TestUtils {
    public static SchedulableInstance<String> newSchedulableInstance(String taskName, String id, int plusSec) {
        return new SchedulableInstance<String>() {
            @Override
            public TaskInstance<String> getTaskInstance() {
                return new TaskInstance<>(taskName, id, "test-data");
            }

            @Override
            public Instant getNextExecutionTime(Instant instant) {
                return instant.plusSeconds(plusSec);
            }
        };
    }

}
