package com.github.dwladdimiroc.normalApp.util;

import redis.clients.jedis.Jedis;

public class Redis {
    // private static String REDIS_HOST = "10.132.0.31";
    private static final String REDIS_HOST = "localhost";

    public int getReplicas(String key) {
        Jedis jedis = new Jedis(REDIS_HOST);
        String cachedResponse = jedis.get(key);
        jedis.close();
        if (cachedResponse == null) {
            return 1;
        } else {
            return Integer.parseInt(cachedResponse);
        }
    }

    public void setLatency(String id, double latency) {
        Jedis jedis = new Jedis(REDIS_HOST);
        String cachedResponse = jedis.set("latency-" + id, String.valueOf(latency));
        jedis.close();
        System.out.println(cachedResponse);
    }
}
