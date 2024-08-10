package com.gmall.realtime.common.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.time.Duration;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * Jedis工具类
 */
public class JedisUtils {
    public static Jedis getJedis() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        ResourceBundle bundle = ResourceBundle.getBundle("jedis");
        String host = bundle.getString("host");
        int port = Integer.parseInt(bundle.getString("port"));
        int connectTimeout = Integer.parseInt(bundle.getString("connectTimeout"));
        String password = bundle.getString("password");
        int database = Integer.parseInt(bundle.getString("database"));
        int maxTotal = Integer.parseInt(bundle.getString("maxTotal"));
        int maxWaitMillis = Integer.parseInt(bundle.getString("maxWaitMillis"));
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxWait(Duration.ofMillis(maxWaitMillis));
        return new JedisPool(poolConfig, host, port, connectTimeout, password, database).getResource();
    }
    
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }
    
    public static JedisCluster getJedisCluster() {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        ResourceBundle bundle = ResourceBundle.getBundle("jedisCluster");
        String host1 = bundle.getString("host1");
        int port1 = Integer.parseInt(bundle.getString("port1"));
        String host2 = bundle.getString("host2");
        int port2 = Integer.parseInt(bundle.getString("port2"));
        String host3 = bundle.getString("host3");
        int port3 = Integer.parseInt(bundle.getString("port3"));
        String host4 = bundle.getString("host4");
        int port4 = Integer.parseInt(bundle.getString("port4"));
        String host5 = bundle.getString("host5");
        int port5 = Integer.parseInt(bundle.getString("port5"));
        int connectTimeout = Integer.parseInt(bundle.getString("connectTimeout"));
        int soTimeout = Integer.parseInt(bundle.getString("soTimeout"));
        String password = bundle.getString("password");
        int maxTotal = Integer.parseInt(bundle.getString("maxTotal"));
        int maxWaitMillis = Integer.parseInt(bundle.getString("maxWaitMillis"));
        int maxAttempts = Integer.parseInt(bundle.getString("maxAttempts"));
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxWait(Duration.ofMillis(maxWaitMillis));
        Set<HostAndPort> hostAndPortSet = new HashSet<>();
        hostAndPortSet.add(new HostAndPort(host1, port1));
        hostAndPortSet.add(new HostAndPort(host2, port2));
        hostAndPortSet.add(new HostAndPort(host3, port3));
        hostAndPortSet.add(new HostAndPort(host4, port4));
        hostAndPortSet.add(new HostAndPort(host5, port5));
        return new JedisCluster(hostAndPortSet, connectTimeout, soTimeout, maxAttempts, password, poolConfig);
    }
}
