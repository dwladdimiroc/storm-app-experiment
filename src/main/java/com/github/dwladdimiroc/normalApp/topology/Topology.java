package com.github.dwladdimiroc.normalApp.topology;

import com.github.dwladdimiroc.normalApp.bolt.*;
import com.github.dwladdimiroc.normalApp.spout.Spout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;

public class Topology implements Serializable {
    private static final String TOPOLOGY_NAME = "normalApp";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(128);
        config.setNumWorkers(3);

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        // Spout Twitter Streaming -> BoltA ParseData -> BoltB SpamDetector
        builder.setBolt("BoltA", new BoltA("BoltB"), 5).setNumTasks(5).
                fieldsGrouping("Spout", "BoltA", new Fields("id-replica"));
        // BoltA ParseData -> BoltB SpamDetector -> BoltC UserDetect || BoltF NewsDetector
        builder.setBolt("BoltB", new BoltB("BoltC", "BoltF"), 20).setNumTasks(20).
                fieldsGrouping("BoltA", "BoltB", new Fields("id-replica"));
        // BoltB SpamDetector -> BoltC UserDetect -> BoltD SendNotification
        builder.setBolt("BoltC", new BoltC("BoltD"), 5).setNumTasks(5).
                fieldsGrouping("BoltB", "BoltC", new Fields("data-1"));
        // BoltC UserDetect -> BoltD SendNotification -> BoltE DataSaved
        builder.setBolt("BoltD", new BoltD("BoltE"), 5).setNumTasks(5)
                .fieldsGrouping("BoltC", "BoltD", new Fields("id-replica"));
        // BoltD SendNotification || BoltG SentimentalClassified -> BoltE DataSaved -> ACK
        builder.setBolt("BoltE", new BoltE(), 10).setNumTasks(10)
                .fieldsGrouping("BoltD", "BoltE", new Fields("data-1"))
                .fieldsGrouping("BoltG", "BoltE", new Fields("stream-2"));
        // BoltB SpamDetector -> BoltF NewsDetector -> BoltG TopicClassified || BoltH SentimentalClassified
        builder.setBolt("BoltF", new BoltF("BoltG", "BoltH"), 10).setNumTasks(10)
                .fieldsGrouping("BoltB", "BoltF", new Fields("stream-2"));
        // BoltF NewsDetector || BoltH Sentimental Classified -> BoltG TopicClassified -> BoltE DataSaved
        builder.setBolt("BoltG", new BoltG("BoltE"), 10).setNumTasks(10)
                .fieldsGrouping("BoltF", "BoltG", new Fields("data-1"))
                .fieldsGrouping("BoltH", "BoltG", new Fields("id-replica"));
        // BoltF NewsDetector -> BoltH Sentimental Classified -> BoltG TopicClassified
        builder.setBolt("BoltH", new BoltH("BoltG"), 20).setNumTasks(20)
                .fieldsGrouping("BoltF", "BoltH", new Fields("stream-2"));

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
