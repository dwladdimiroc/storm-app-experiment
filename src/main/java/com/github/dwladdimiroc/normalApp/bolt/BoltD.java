package com.github.dwladdimiroc.normalApp.bolt;

import com.github.dwladdimiroc.normalApp.util.Redis;
import com.github.dwladdimiroc.normalApp.util.Replica;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
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

    private Replica replica;
    private DescriptiveStatistics stats = new DescriptiveStatistics();

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

        this.taskId = context.getThisTaskId();
        logger.info("taskId: {}", taskId);
        this.replica = new Replica(id, taskId);
        Thread tReplica = new Thread(replica);
        tReplica.start();

        logger.info("Prepare BoltD");
    }

    @Override
    public void execute(Tuple input) {
        if (this.replica.isAvailable()) {
            int x = (int) (Math.random() * 1000);
            for (int i = 0; i < array.length; i++) {
                for (int j = 0; j < 100; j++) {
                    if (x == array[i]) {
                        x = x + j;
                    }
                }
            }
            addLatency(input);
            this.outputCollector.ack(input);
        } else {
            while (!this.replica.isAvailable()) {
                Utils.sleep(1000);
            }
            this.outputCollector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        logger.info("Cleanup");
        Redis redis = new Redis();
        String idBolt = this.id + "-" + this.taskId;
        redis.setLatency(idBolt, this.stats.getMean());
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

    public void addLatency(Tuple input){
        long timeI = input.getLong(0);
        long timeF = Time.currentTimeMillis();
        long latency = timeF - timeI;
        this.stats.addValue((double) latency);
    }
}
