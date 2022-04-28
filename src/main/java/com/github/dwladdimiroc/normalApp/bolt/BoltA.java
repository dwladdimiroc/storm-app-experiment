package com.github.dwladdimiroc.normalApp.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BoltA implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltA.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    public BoltA() {
        logger.info("Constructor BoltA");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[10000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }

        logger.info("Prepare BoltA");
    }

    @Override
    public void execute(Tuple input) {
        int x = (int) (Math.random() * 1000);
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < 100; j++) {
                if (x == array[i]) {
                    x = x + j;
                }
            }
        }

        Values v = new Values(input.getValue(0));
        this.outputCollector.emit("BoltB", v);
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltB", new Fields("time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
