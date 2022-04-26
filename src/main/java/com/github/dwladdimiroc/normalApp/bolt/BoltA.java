package com.github.dwladdimiroc.normalApp.bolt;

import com.github.dwladdimiroc.normalApp.util.Replica;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class BoltA implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltA.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;
    private String stream;

    private Replica replica;


    public BoltA(String stream) {
        logger.info("Constructor BoltA");
        this.stream = stream;
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

        int taskId = context.getThisTaskId();
        logger.info("taskId: {}", taskId);
        this.replica = new Replica(id, taskId);
        Thread tReplica = new Thread(replica);
        tReplica.start();

        logger.info("Prepare BoltA");
    }

    @Override
    public void execute(Tuple input) {
        if (this.replica.isAvailable()) {
            logger.info("Execute {}", input);
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
        } else {
            while (!this.replica.isAvailable()) {
                Utils.sleep(1000);
            }
            this.outputCollector.fail(input);
        }
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
