package com.github.dwladdimiroc.normalApp.topology;

import com.github.dwladdimiroc.normalApp.bolt.BoltA;
import com.github.dwladdimiroc.normalApp.bolt.BoltB;
import com.github.dwladdimiroc.normalApp.bolt.BoltC;
import com.github.dwladdimiroc.normalApp.bolt.BoltD;
import com.github.dwladdimiroc.normalApp.spout.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;

public class Topology implements Serializable {
    public static final String TOPOLOGY_NAME = "normalApp";
    public static final int NUM_REPLICAS = 5;

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
//        config.setNumWorkers(7);
        config.setNumWorkers(1);

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        builder.setBolt("BoltA", new BoltA("BoltB"), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .shuffleGrouping("Spout", "BoltA");

        builder.setBolt("BoltB", new BoltB("BoltC"), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .shuffleGrouping("BoltA", "BoltB");

        builder.setBolt("BoltC", new BoltC("BoltD"), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .shuffleGrouping("BoltB", "BoltC");

        builder.setBolt("BoltD", new BoltD(), NUM_REPLICAS).setNumTasks(NUM_REPLICAS)
                .shuffleGrouping("BoltC", "BoltD");

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
