# db-scheduler-redis

This project adds Redis support for [db-scheduler](https://github.com/kagkarlsson/db-scheduler).

## Components
Basically, db-scheduler-redis provides a Redis-version TaskRepository to enable task scheduling using Redis without making modifications to the original module:

- RedisTaskRepository implements TaskRepository interface.
- RedisScheduler inherits from Scheduler.
- RedisSchedulerBuilder extends SchedulerBuilder and accepts Redis client (now using Lettuce).

## Getting Started
```java
RedisClient redisClient = RedisClient.create("redis://localhost:6379");
OneTimeTask<TaskData> oneTimeTask = Tasks.oneTime("task-1", TaskData.class).execute(((taskInstance, executionContext) -> {
    LOG.info("Executing task={} id={} data={}", taskInstance.getTaskName(), taskInstance.getId(), taskInstance.getData());
}));
Scheduler scheduler = RedisScheduler
    .create(redisClient, oneTimeTask)
    .pollingInterval(Duration.ofSeconds(5))
    .build();

scheduler.start();
```

## TODOS
- Generic Redis client.
- Table equivalence in Redis.
- Better way to go through keys.
- Lock and fetch.
- Make redis client invisible to users.
- Time complexity of fetch and lock.
- Multiple scheduler instances tests.