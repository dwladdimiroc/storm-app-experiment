package com.github.dwladdimiroc.normalApp.util;

import redis.clients.jedis.Jedis;

public class Redis {
     private static String REDIS_HOST = "10.132.0.31";

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

    public int getInputIndex() {
        Jedis jedis = new Jedis(REDIS_HOST);
        String cachedResponse = jedis.get("inputIndex");
        jedis.close();
        if (cachedResponse == null) {
            return 0;
        } else {
            return Integer.parseInt(cachedResponse);
        }
    }

    public void setInputIndex(int index) {
        Jedis jedis = new Jedis(REDIS_HOST);
        String cachedResponse = jedis.set("inputIndex", String.valueOf(index));
        jedis.close();
        System.out.println(cachedResponse);
    }
}
