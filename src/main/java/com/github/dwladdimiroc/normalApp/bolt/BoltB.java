package com.github.dwladdimiroc.normalApp.bolt;

import com.github.dwladdimiroc.normalApp.spout.Spout;
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

    private AtomicInteger numReplicas1;
    private AtomicInteger numReplicas2;
    private long events;
    private String stream1;
    private String stream2;

    public BoltB(String stream1, String stream2) {
        logger.info("Constructor BoltB");
        this.stream1 = stream1;
        this.stream2 = stream2;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[40000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }

        this.numReplicas1 = new AtomicInteger(1);
        this.numReplicas2 = new AtomicInteger(2);
        this.events = 0;
        Thread adaptiveBolt1 = new Thread(new Replicas(this.stream1, this.numReplicas1));
        adaptiveBolt1.start();
        Thread adaptiveBolt2 = new Thread(new Replicas(this.stream2, this.numReplicas2));
        adaptiveBolt2.start();
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

        long idReplica = 0;
        long idReplica1 = events % this.numReplicas1.get();
        long idReplica2 = events % this.numReplicas2.get();

        Values v = new Values(input.getValue(0), idReplica, idReplica1, idReplica2);
        if (events % 2 == 0) {
            this.outputCollector.emit("BoltC", v);
        } else{
            this.outputCollector.emit("BoltE", v);
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
        declarer.declareStream("stream1", new Fields("number", "id-replica", "data-1", "stream-2"));
        declarer.declareStream("stream2", new Fields("number", "id-replica", "data-1", "stream-2"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
