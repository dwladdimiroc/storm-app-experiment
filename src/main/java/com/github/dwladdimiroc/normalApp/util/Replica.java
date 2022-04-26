package com.github.dwladdimiroc.normalApp.util;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.dwladdimiroc.normalApp.topology.Topology.NUM_REPLICAS;

public class Replica implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Replica.class);

    private final String name;
    private final int id;
    private boolean available;

    public Replica(String name, int id) {
        this.name = name;
        this.id = id;
        this.available = false;
    }

    public boolean isAvailable() {
        return available;
    }

    @Override
    public void run() {
        Redis redis = new Redis();
        while (true) {
            int replicas = redis.getReplicas(this.name);
            int numReplicas = id % NUM_REPLICAS;
            available = numReplicas < replicas;
            Utils.sleep(1000);
        }
    }
}
