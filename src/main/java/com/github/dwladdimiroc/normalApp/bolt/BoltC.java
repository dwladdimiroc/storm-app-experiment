package com.github.dwladdimiroc.normalApp.bolt;

import com.github.dwladdimiroc.normalApp.util.Replica;
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

public class BoltC implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltC.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;
    private String stream;

    public BoltC(String stream) {
        logger.info("Constructor BoltC");
        this.stream = stream;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[75000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }

        logger.info("Prepare BoltC");
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
        this.outputCollector.emit(stream, v);
        this.outputCollector.ack(input);
    }


    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltD", new Fields("time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
