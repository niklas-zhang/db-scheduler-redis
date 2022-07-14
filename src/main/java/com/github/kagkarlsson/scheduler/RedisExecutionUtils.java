package com.github.kagkarlsson.scheduler;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class RedisExecutionUtils {

    public static Map<String, Object> newExecution(Instant executionTime, Object data) {
        Map<String, Object> hm = new HashMap<>() {{
            put("executionTime", executionTime);
            put("picked", false);
            put("pickedBy", null);
            put("consecutiveFailures", 0);
            put("lastHeartbeat", null);
            put("version", 1L);
            put("lastFailure", null);
            put("lastSuccess", null);
            put("data", data);
        }};
        return hm;
    }

    public static String buildRedisKey(String taskName, String taskId) {
        return taskName + ":" + taskId;
    }

}
