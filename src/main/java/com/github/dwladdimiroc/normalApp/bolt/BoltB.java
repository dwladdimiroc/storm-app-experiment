package com.github.dwladdimiroc.normalApp.bolt;


import com.github.dwladdimiroc.normalApp.util.Replicas;
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

public class BoltB implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltB.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    private AtomicInteger numReplicas;
    private long events;
    private String stream;

    public BoltB(String stream) {
        logger.info("Constructor BoltB");
        this.stream = stream;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[100000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }

        this.numReplicas = new AtomicInteger(1);
        this.events = 0;
        Thread adaptiveBolt = new Thread(new Replicas(this.stream, this.numReplicas));
        adaptiveBolt.start();
        logger.info("Prepare BoltB");
    }

    @Override
    public void execute(Tuple input) {
//        logger.info("Process event");
        this.events++;
//        Utils.sleep(1);
        int x = (int) (Math.random() * 1000);
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < 100; j++) {
                if (x == array[i]) {
                    x = x + j;
                }
            }
        }

        long idReplica = events % this.numReplicas.get();
        Values v = new Values(input.getValue(0), idReplica);
        this.outputCollector.emit("BoltC", v);
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltC", new Fields("number", "id-replica"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
