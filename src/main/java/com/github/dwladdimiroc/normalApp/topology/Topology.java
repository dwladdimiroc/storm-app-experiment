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
        config.setMessageTimeoutSecs(125);
        config.setNumWorkers(7);

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        // Spout Twitter Streaming -> BoltA ParseData -> BoltB SpamDetector
        builder.setBolt("BoltA", new BoltA(), 5).setNumTasks(5).
                shuffleGrouping("Spout", "BoltA");
        // BoltA ParseData -> BoltB SpamDetector -> BoltC UserDetect || BoltF NewsDetector
        builder.setBolt("BoltB", new BoltB(), 10).setNumTasks(10).
                shuffleGrouping("BoltA", "BoltB");
        // BoltB SpamDetector -> BoltC UserDetect -> BoltD SendNotification
        builder.setBolt("BoltC", new BoltC(), 5).setNumTasks(5).
                shuffleGrouping("BoltB", "BoltC");
        // BoltC UserDetect -> BoltD SendNotification -> BoltE DataSaved
        builder.setBolt("BoltD", new BoltD(), 5).setNumTasks(5)
                .shuffleGrouping("BoltC", "BoltD");
        // BoltD SendNotification || BoltG SentimentalClassified -> BoltE DataSaved -> ACK
        builder.setBolt("BoltE", new BoltE(), 10).setNumTasks(10)
                .shuffleGrouping("BoltD", "BoltE")
                .shuffleGrouping("BoltG", "BoltE");
        // BoltB SpamDetector -> BoltF NewsDetector -> BoltG TopicClassified || BoltH SentimentalClassified
        builder.setBolt("BoltF", new BoltF(), 10).setNumTasks(10)
                .shuffleGrouping("BoltB", "BoltF");
        // BoltF NewsDetector || BoltH Sentimental Classified -> BoltG TopicClassified -> BoltE DataSaved
        builder.setBolt("BoltG", new BoltG(), 10).setNumTasks(10)
                .shuffleGrouping("BoltF", "BoltG")
                .shuffleGrouping("BoltH", "BoltG");
        // BoltF NewsDetector -> BoltH Sentimental Classified -> BoltG TopicClassified
        builder.setBolt("BoltH", new BoltH(), 20).setNumTasks(20)
                .shuffleGrouping("BoltF", "BoltH");

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
