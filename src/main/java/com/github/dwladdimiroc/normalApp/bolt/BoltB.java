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

public class BoltB implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltB.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    private long events;

    public BoltB() {
        logger.info("Constructor BoltB");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[20000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }
        this.events = 0;
        logger.info("Prepare BoltB");
    }

    @Override
    public void execute(Tuple input) {
        this.events++;
        int x = (int) (Math.random() * 1000);
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < 100; j++) {
                if (x == array[i]) {
                    x = x + j;
                }
            }
        }

        Values v = new Values(input.getValue(0));
        if ((this.events % 10 == 0) || (this.events % 10 == 1)) {
            this.outputCollector.emit("BoltC", v);
        } else {
            this.outputCollector.emit("BoltF", v);
        }
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltC", new Fields("time"));
        declarer.declareStream("BoltF", new Fields("time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
