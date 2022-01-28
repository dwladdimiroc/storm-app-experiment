package com.github.dwladdimiroc.normalApp.bolt;

import com.github.dwladdimiroc.normalApp.util.Replicas;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BoltD implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltD.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    private AtomicInteger numReplicas;
    private long events;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();

        this.array = new int[50000];
        for (int i = 0; i < this.array.length; i++) {
            this.array[i] = i;
        }

//        this.numReplicas = new AtomicInteger(1);
//        this.events = 0;
        logger.info("Prepare BoltD");
    }

    @Override
    public void execute(Tuple input) {
//        logger.info("Process event");
        this.events++;
//        Utils.sleep(3);
        int x = (int) (Math.random() * 1000);
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < 100; j++) {
                if (x == array[i]) {
                    x = x + j;
                }
            }
        }
        long timeInit = input.getLong(0);
        long timeFinal = Time.currentTimeMillis();
        long latency = timeFinal - timeInit;
        //String message = "Latency={"+latency+"}";
        //logger.info(message);
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number", "id-replica"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
