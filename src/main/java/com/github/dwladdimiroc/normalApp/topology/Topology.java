package com.github.dwladdimiroc.normalApp.topology;

import com.github.dwladdimiroc.normalApp.bolt.*;
import com.github.dwladdimiroc.normalApp.spout.Spout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;

public class Topology implements Serializable {
    private static final String TOPOLOGY_NAME = "normalApp";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        config.setNumWorkers(7);

        TopologyBuilder builder = new TopologyBuilder();
        int numParallelism = Integer.parseInt(args[1]);

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        // Spout Twitter Streaming -> BoltA ParseData -> BoltB SpamDetector
        builder.setBolt("BoltA", new BoltA(), numParallelism).setNumTasks(numParallelism).
                shuffleGrouping("Spout", "BoltA");
        // BoltA ParseData -> BoltB SpamDetector -> BoltC UserDetect || BoltF NewsDetector
        builder.setBolt("BoltB", new BoltB(), numParallelism).setNumTasks(numParallelism).
                shuffleGrouping("BoltA", "BoltB");
        // BoltB SpamDetector -> BoltC UserDetect -> BoltD SendNotification
        builder.setBolt("BoltC", new BoltC(), numParallelism).setNumTasks(numParallelism).
                shuffleGrouping("BoltB", "BoltC");
        // BoltC UserDetect -> BoltD SendNotification -> BoltE DataSaved
        builder.setBolt("BoltD", new BoltD(), numParallelism).setNumTasks(numParallelism)
                .shuffleGrouping("BoltC", "BoltD");
        // BoltD SendNotification || BoltG SentimentalClassified -> BoltE DataSaved -> ACK
        builder.setBolt("BoltE", new BoltE(), numParallelism).setNumTasks(numParallelism)
                .shuffleGrouping("BoltD", "BoltE")
                .shuffleGrouping("BoltG", "BoltE");
        // BoltB SpamDetector -> BoltF NewsDetector -> BoltG TopicClassified || BoltH SentimentalClassified
        builder.setBolt("BoltF", new BoltF(), numParallelism).setNumTasks(numParallelism)
                .shuffleGrouping("BoltB", "BoltF");
        // BoltF NewsDetector || BoltH Sentimental Classified -> BoltG TopicClassified -> BoltE DataSaved
        builder.setBolt("BoltG", new BoltG(), numParallelism).setNumTasks(numParallelism)
                .shuffleGrouping("BoltF", "BoltG")
                .shuffleGrouping("BoltH", "BoltG");
        // BoltF NewsDetector -> BoltH Sentimental Classified -> BoltG TopicClassified
        builder.setBolt("BoltH", new BoltH(), numParallelism).setNumTasks(numParallelism)
                .shuffleGrouping("BoltF", "BoltH");

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
