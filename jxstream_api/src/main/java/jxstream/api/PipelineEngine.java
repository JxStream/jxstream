/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author brian mwambazi and andy philogene
 */
public class PipelineEngine {

    private final List<Runnable> switches;
    private final ExecutorService threadpool;
    private long startTime;
    private long upstreamTuples;
    private long downStreamTuples;
    Map<String, long[]> stats;
    private boolean started;
    private long limit = 0L;
    private String statsAtLimit;
    private Boolean reachedLimit = false;

    PipelineEngine() {
        switches = new ArrayList<>();
        stats = new HashMap<>();
        threadpool = Executors.newCachedThreadPool();
    }

    boolean isStarted() {
        return started;
    }

    void setLimit(long count) {
        limit = count;
    }

    long getLimit() {
        return this.limit;
    }

    Boolean reachedLimit() {
        return reachedLimit;
    }

    String getStatsAtLimit() {
        return statsAtLimit;
    }

    public void start() {

        started = true;
        switches.stream().forEach((s) -> {
            threadpool.execute(s);
        });
        startTime = System.nanoTime();

    }

    public void stop() {
        threadpool.shutdown();
    }

    void addSwitch(Runnable sw) {
        switches.add(sw);
    }

    int getUpstreamThrput() {
        long dur = (System.nanoTime() - startTime);
        if (dur == 0) {
            return 0;
        }
        return (int) ((upstreamTuples * 1000000000.0) / dur);

    }

    int getdownstreamThrput() {
        long dur = (System.nanoTime() - startTime);
        if (dur == 0) {
            return 0;
        }
        return (int) ((downStreamTuples * 1000000000.0) / dur);

    }

    void incUpstreamCount() {
        upstreamTuples++;
        if (!reachedLimit && upstreamTuples > limit) {
            statsAtLimit = getStats(); // Done only once for that name
            reachedLimit = true;
        }
    }

    long upStreamCount() {
        return upstreamTuples;
    }

    long downStreamCount() {
        return downStreamTuples;
    }

    void incDownStreamCount() {
        downStreamTuples++;
    }

    void addNewCountStat(String name, long[] counts) {
        if (stats.containsKey(name)) {
            throw new IllegalArgumentException("Stat name supplied already exists");
        }

        stats.put(name, counts);
    }

    String getStats() {
        StringBuilder str = new StringBuilder();
        str.append(String.format("  Upstream  -> count= %d throuput= %d tuples/sec\n", upStreamCount(), getUpstreamThrput()));
        str.append(String.format("  Downstream-> count= %d throuput= %d tuples/sec\n", downStreamCount(), getdownstreamThrput()));
        for (Map.Entry<String, long[]> entrySet : stats.entrySet()) {
            String name = entrySet.getKey();
            long[] counts = entrySet.getValue();
            long start = counts[0];//pos is for start
            long total = getTotalSkipFirst(counts);
            long dur = System.nanoTime() - start;
            double thrput = 0;
            if (dur > 0) {
                thrput = total * 1000000.0 / ((System.nanoTime() - start) / 1000.0);
            }

            str.append(String.format("  %s-> count= %d thoughput= %.2f tuples/sec \n", name, total, thrput));

        }
        return str.toString();
    }

    private long getTotalSkipFirst(long[] counts) {
        long rtVal = 0;
        for (int i = 1; i < counts.length; i++) {
            rtVal += counts[i];
        }

        return rtVal;
    }

}
