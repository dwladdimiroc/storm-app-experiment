package com.github.dwladdimiroc.normalApp.util;

import org.apache.storm.utils.Utils;

import java.util.concurrent.atomic.AtomicInteger;

public class Replicas implements Runnable {
    String idReceptor;
    AtomicInteger replicas;

    public Replicas(String idReceptor, AtomicInteger replicas) {
        this.idReceptor = idReceptor;
        this.replicas = replicas;
    }

    @Override
    public void run() {
        Redis redis = new Redis();
        while (true) {
            int replicas = redis.getReplicas(this.idReceptor);
            this.replicas.set(replicas);
            Utils.sleep(5000);
        }
    }
}
