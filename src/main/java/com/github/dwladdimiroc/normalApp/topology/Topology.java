package com.github.dwladdimiroc.normalApp.topology;

import com.github.dwladdimiroc.normalApp.bolt.BoltA;
import com.github.dwladdimiroc.normalApp.bolt.BoltB;
import com.github.dwladdimiroc.normalApp.bolt.BoltC;
import com.github.dwladdimiroc.normalApp.bolt.BoltD;
import com.github.dwladdimiroc.normalApp.spout.Spout;
import com.github.dwladdimiroc.normalApp.util.PoolGrouping;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;
import java.sql.Time;

public class Topology implements Serializable {
    public static final String TOPOLOGY_NAME = "normalApp";
    public static final int NUM_REPLICAS = 23;
    public static final int NUM_WORKERS = 7;
    public static final int QUEUE_SIZE = 1000000;
    public static final int TIMEOUT = 360;
    public static final int TIMEOUT_MS = TIMEOUT*1000;

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(TIMEOUT);
        config.setNumWorkers(NUM_WORKERS);
        config.setNumAckers(NUM_WORKERS);
        config.setMaxSpoutPending(QUEUE_SIZE);

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        builder.setBolt("BoltA", new BoltA("BoltB"), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .customGrouping("Spout", "BoltA", new PoolGrouping());

        builder.setBolt("BoltB", new BoltB("BoltC"), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .customGrouping("BoltA", "BoltB", new PoolGrouping());

        builder.setBolt("BoltC", new BoltC("BoltD"), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .customGrouping("BoltB", "BoltC", new PoolGrouping());

        builder.setBolt("BoltD", new BoltD(), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .customGrouping("BoltC", "BoltD", new PoolGrouping());

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
