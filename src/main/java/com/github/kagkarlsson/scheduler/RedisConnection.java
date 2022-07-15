package com.github.kagkarlsson.scheduler;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConnection {
    private final static Logger LOG = LoggerFactory.getLogger(RedisConnection.class);

    private static StatefulRedisConnection<String, Object> connection;

    private RedisConnection() {

    }

    public static StatefulRedisConnection<String, Object> getConnection(RedisClient client) {
        if (connection == null) {
            synchronized (RedisConnection.class) {
                if (connection == null) {
                    StatefulRedisConnection<String, Object> connection = null;
                    try {
                        LOG.debug("Trying to connect Redis.");
                        connection = client.connect(new SerializedObjectCodec());
                    } catch (RedisException e) {
                        LOG.error("Error when connecting to Redis.");
                        throw new RedisException("Unable to connect to Redis.");
                    }
                    RedisConnection.connection = connection;
                }
            }
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            connection.close();
        }
    }
}
