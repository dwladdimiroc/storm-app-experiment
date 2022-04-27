package com.github.dwladdimiroc.normalApp.util;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replica implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Replica.class);

    private final String name;
    private int numReplicas;

    public Replica(String name) {
        logger.info("Replica PoolGrouping {}", name);
        this.name = name;
        this.numReplicas = 0;
    }

    public String getName(){return this.name;}

    public int getNumReplicas() {
        return this.numReplicas;
    }

    @Override
    public void run() {
        Redis redis = new Redis();
        while (true) {
            this.numReplicas = redis.getReplicas(this.name);
            Utils.sleep(1000);
        }
    }
}
