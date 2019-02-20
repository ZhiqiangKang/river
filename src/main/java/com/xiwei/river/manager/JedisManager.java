package com.xiwei.river.manager;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class JedisManager {

    private static final String REDIS_HOST_KEY = "redis.host";
    private static final String REDIS_PORT_KEY = "redis.port";
    private static final String REDIS_CONNECTION_TIMEOUT_KEY = "redis.connectionTimeout";
    private static final String REDIS_SO_TIMEOUT_KEY = "redis.soTimeout";

    public static Jedis getJedis(String redisPropertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(redisPropertiesFilePath));

        String host = properties.getProperty(REDIS_HOST_KEY, Protocol.DEFAULT_HOST);
        String portValue = properties.getProperty(REDIS_PORT_KEY);
        String connectionTimeoutValue = properties.getProperty(REDIS_CONNECTION_TIMEOUT_KEY);
        String soTimeoutValue = properties.getProperty(REDIS_SO_TIMEOUT_KEY);

        int port = Protocol.DEFAULT_PORT;
        int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
        int soTimeout = Protocol.DEFAULT_TIMEOUT;
        if (StringUtils.isNotBlank(portValue)){
            port = Integer.valueOf(portValue);
        }
        if (StringUtils.isNotBlank(connectionTimeoutValue)){
            connectionTimeout = Integer.valueOf(connectionTimeoutValue);
        }
        if (StringUtils.isNotBlank(soTimeoutValue)){
            soTimeout = Integer.valueOf(soTimeoutValue);
        }

        return new Jedis(host, port, connectionTimeout, soTimeout);
    }
}
