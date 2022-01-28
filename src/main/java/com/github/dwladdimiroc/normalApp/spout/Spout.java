package com.github.dwladdimiroc.normalApp.spout;

import com.github.dwladdimiroc.normalApp.util.Distribution;
import com.github.dwladdimiroc.normalApp.util.Redis;
import com.github.dwladdimiroc.normalApp.util.Replicas;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Spout implements IRichSpout, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Spout.class);
    private Map conf;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    private LinkedBlockingQueue<Integer> queue;
    private String distribution;
    private float[] samples;
    private int indexSamples;

    private AtomicInteger numReplicas;
    private long events;
    private String stream;

    private String id;

    public Spout(String distribution, String stream) {
        this.distribution = distribution;
        this.stream = stream;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        this.id = context.getThisComponentId();
        this.queue = new LinkedBlockingQueue<Integer>(100000);

        this.numReplicas = new AtomicInteger(1);
        this.events = 0;
        Thread adaptiveBolt = new Thread(new Replicas(this.stream, this.numReplicas));
        adaptiveBolt.start();

        Distribution file = new Distribution(this.distribution);
        this.samples = file.Input();

        Redis redis = new Redis();
        this.indexSamples = redis.getInputIndex();
        logger.info("Open Redis IndexSamples: " + this.indexSamples);

        Thread createTuples = new Thread(new TuplesCreator());
        createTuples.start();
    }

    class TuplesCreator implements Runnable {
        @Override
        public void run() {
            createTuples();
        }

        public void createTuples() {
            while (true) {
                for (int i = 0; i < samples[indexSamples]; i++) {
                    queue.add(i);
                }
                indexSamples++;
                Utils.sleep(1000);
                Redis redis = new Redis();
                redis.setInputIndex(indexSamples);
            }
        }
    }

    class SaveIndex implements Runnable {
        @Override
        public void run() {
            saveIndex();
        }

        public void saveIndex() {
            while (true) {
                Utils.sleep(10000);
                Redis redis = new Redis();
                redis.setInputIndex(indexSamples);
            }
        }
    }

    @Override
    public void close() {
        logger.info("Close");
        Redis redis = new Redis();
        redis.setInputIndex(this.indexSamples);
        logger.info("Close Redis IndexSamples: " + this.indexSamples);
    }

    @Override
    public void activate() {
        logger.info("Activate");
        Redis redis = new Redis();
        this.indexSamples = redis.getInputIndex();
        logger.info("Activate Redis IndexSamples: " + this.indexSamples);
    }

    @Override
    public void deactivate() {
        logger.info("Deactivate");
        Redis redis = new Redis();
        redis.setInputIndex(this.indexSamples);
        logger.info("Deactivate Redis IndexSamples: " + this.indexSamples);
    }


    @Override
    public void nextTuple() {
        Integer nums = queue.poll();
        if (nums == null) {
            Utils.sleep(10);
        } else {
            long idReplica;
            if (this.numReplicas.get() > 0) {
                idReplica = this.events % this.numReplicas.get();
            } else{
                idReplica = this.events % 1;
            }
            Values values = new Values(Time.currentTimeMillis(), idReplica);
            this.collector.emit("BoltA", values, values.get(0));
            this.events++;
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltA", new Fields("number", "id-replica"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return conf;
    }
}


