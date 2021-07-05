package com.github.dwladdimiroc.normalApp.topology;

import com.github.dwladdimiroc.normalApp.bolt.*;
import com.github.dwladdimiroc.normalApp.spout.Spout;
import com.github.dwladdimiroc.normalApp.util.Redis;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;

public class Topology implements Serializable {
    private static final String TOPOLOGY_NAME = "normalApp";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(125);
        config.setNumWorkers(3);

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0], "BoltA"), 1);

        // Set Bolt
        builder.setBolt("BoltA", new BoltA("BoltB"), 10).setNumTasks(10).fieldsGrouping("Spout", "BoltA", new Fields("id-replica"));
        builder.setBolt("BoltB", new BoltB("BoltC", "BoltE"), 10).setNumTasks(10).fieldsGrouping("BoltA", "BoltB",new Fields("id-replica"));

        builder.setBolt("BoltC", new BoltC("BoltD"), 10).setNumTasks(10).fieldsGrouping("BoltB", "BoltC", new Fields("data-1"));
        builder.setBolt("BoltD", new BoltD("BoltH"), 10).setNumTasks(10).fieldsGrouping("BoltC", "BoltD", new Fields("id-replica"));

        builder.setBolt("BoltE", new BoltE("BoltF"), 10).setNumTasks(10).fieldsGrouping("BoltB", "BoltE", new Fields("stream-2"));
        builder.setBolt("BoltF", new BoltF("BoltG"), 10).setNumTasks(10).fieldsGrouping("BoltE", "BoltF", new Fields("id-replica"));
        builder.setBolt("BoltG", new BoltG("BoltH"), 10).setNumTasks(10).fieldsGrouping("BoltF", "BoltG", new Fields("id-replica"));

        builder.setBolt("BoltH", new BoltH(), 1).setNumTasks(1).fieldsGrouping("BoltD", "BoltH", new Fields("id-replica")).fieldsGrouping("BoltG", "BoltH", new Fields("id-replica"));

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
