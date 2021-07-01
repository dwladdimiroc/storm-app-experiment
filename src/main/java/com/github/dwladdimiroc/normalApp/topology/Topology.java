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
        builder.setBolt("BoltA", new BoltA("BoltB"), 10).setNumTasks(10).fieldsGrouping("Spout", new Fields("id-replica"));
        builder.setBolt("BoltB", new BoltB("BoltC", "BoltE"), 25).setNumTasks(25).fieldsGrouping("BoltA", new Fields("id-replica"));
        builder.setBolt("BoltC", new BoltC("BoltD"), 20).setNumTasks(20).fieldsGrouping("BoltB", new Fields("id-replica"));
        builder.setBolt("BoltE", new BoltE("BoltD"), 20).setNumTasks(20).fieldsGrouping("BoltB", new Fields("id-replica"));
        builder.setBolt("BoltD", new BoltD(), 10).setNumTasks(10).fieldsGrouping("BoltC", new Fields("id-replica")).fieldsGrouping("BoltE", new Fields("id-replica"));

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
