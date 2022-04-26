package com.github.dwladdimiroc.normalApp.util;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class PoolGrouping implements LoadAwareCustomStreamGrouping {
    private static final Logger logger = LoggerFactory.getLogger(PoolGrouping.class);

    private List<Integer>[] rets;
    private int[] targets;
    private int[] loads;
    private int total;
    private long lastUpdate = 0;

    private Random random;
    private Replica replica;


    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.replica = new Replica(stream.get_streamId());
        Thread tReplica = new Thread(replica);
        tReplica.start();

        this.random = new Random();

        rets = (List<Integer>[]) new List<?>[targetTasks.size()];
        targets = new int[targetTasks.size()];
        for (int i = 0; i < targets.length; i++) {
            rets[i] = Arrays.asList(targetTasks.get(i));
            targets[i] = targetTasks.get(i);
        }
        loads = new int[targets.length];
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        throw new RuntimeException("NOT IMPLEMENTED");
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
        if ((lastUpdate + 1000) < System.currentTimeMillis()) {
            int local_total = 0;
            for (int i = 0; i < this.replica.getNumReplicas(); i++) {
                int val = (int) (101 - (load.get(targets[i]) * 100));
                loads[i] = val;
                local_total += val;
            }
            total = local_total;
            lastUpdate = System.currentTimeMillis();
        }
        int selected = random.nextInt(total);
        int sum = 0;
        for (int i = 0; i < this.replica.getNumReplicas(); i++) {
            sum += loads[i];
            if (selected < sum) {
                return rets[i];
            }
        }
        return rets[0];
    }
}

