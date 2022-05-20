package com.github.dwladdimiroc.normalApp.topology;

import com.github.dwladdimiroc.normalApp.bolt.*;
import com.github.dwladdimiroc.normalApp.spout.Spout;
import com.github.dwladdimiroc.normalApp.util.PoolGrouping;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;

public class Topology implements Serializable {
    private static final String TOPOLOGY_NAME = "normalApp";
    private static final int NUM_PARALLELISM = 23;

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        config.setNumWorkers(7);



        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        // Spout Twitter Streaming -> BoltA ParseData -> BoltB SpamDetector
        builder.setBolt("BoltA", new BoltA(), 1).setNumTasks(NUM_PARALLELISM).
                customGrouping("Spout", "BoltA", new PoolGrouping());
        // BoltA ParseData -> BoltB SpamDetector -> BoltC UserDetect || BoltF NewsDetector
        builder.setBolt("BoltB", new BoltB(), 1).setNumTasks(NUM_PARALLELISM).
                customGrouping("BoltA", "BoltB", new PoolGrouping());
        // BoltB SpamDetector -> BoltC UserDetect -> BoltD SendNotification
        builder.setBolt("BoltC", new BoltC(), 1).setNumTasks(NUM_PARALLELISM).
                customGrouping("BoltB", "BoltC", new PoolGrouping());
        // BoltC UserDetect -> BoltD SendNotification -> BoltE DataSaved
        builder.setBolt("BoltD", new BoltD(), 1).setNumTasks(NUM_PARALLELISM)
                .customGrouping("BoltC", "BoltD", new PoolGrouping());
        // BoltD SendNotification || BoltG SentimentalClassified -> BoltE DataSaved -> ACK
        builder.setBolt("BoltE", new BoltE(), 1).setNumTasks(NUM_PARALLELISM)
                .customGrouping("BoltD", "BoltE", new PoolGrouping())
                .customGrouping("BoltG", "BoltE", new PoolGrouping());
        // BoltB SpamDetector -> BoltF NewsDetector -> BoltG TopicClassified || BoltH SentimentalClassified
        builder.setBolt("BoltF", new BoltF(), 1).setNumTasks(NUM_PARALLELISM)
                .customGrouping("BoltB", "BoltF", new PoolGrouping());
        // BoltF NewsDetector || BoltH Sentimental Classified -> BoltG TopicClassified -> BoltE DataSaved
        builder.setBolt("BoltG", new BoltG(), 1).setNumTasks(NUM_PARALLELISM)
                .customGrouping("BoltF", "BoltG", new PoolGrouping())
                .customGrouping("BoltH", "BoltG", new PoolGrouping());
        // BoltF NewsDetector -> BoltH Sentimental Classified -> BoltG TopicClassified
        builder.setBolt("BoltH", new BoltH(), 1).setNumTasks(NUM_PARALLELISM)
                .customGrouping("BoltF", "BoltH", new PoolGrouping());

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
