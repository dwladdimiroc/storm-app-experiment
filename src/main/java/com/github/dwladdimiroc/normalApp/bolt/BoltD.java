package com.github.dwladdimiroc.normalApp.bolt;

import com.github.dwladdimiroc.normalApp.topology.Topology;
import com.github.dwladdimiroc.normalApp.util.Redis;
import com.github.dwladdimiroc.normalApp.util.Replica;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class BoltD implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltD.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int taskId;
    private int[] array;

    public BoltD() {
        logger.info("Constructor BoltD");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[50000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }

        logger.info("Prepare BoltD");
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLong(0);
        long currentTime = Time.currentTimeMillis();
        long timeout = currentTime - timestamp;
        if (timeout < Topology.TIMEOUT_MS) {
            int x = (int) (Math.random() * 1000);
            for (int i = 0; i < array.length; i++) {
                for (int j = 0; j < 100; j++) {
                    if (x == array[i]) {
                        x = x + j;
                    }
                }
            }

            this.outputCollector.ack(input);
        } else {
            this.outputCollector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        logger.info("Cleanup");
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
