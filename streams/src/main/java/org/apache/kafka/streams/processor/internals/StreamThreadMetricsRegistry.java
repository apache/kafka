package org.apache.kafka.streams.processor.internals;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.MetricNameTemplate;

public class StreamThreadMetricsRegistry {
    
    public MetricNameTemplate commitTimeAvg;
    public MetricNameTemplate commitTimeMax;
    public MetricNameTemplate commitCallsRate;
    public MetricNameTemplate pollTimeAvg;
    public MetricNameTemplate pollTimeMax;
    public MetricNameTemplate pollCallsRate;
    public MetricNameTemplate processTimeAvgMs;
    public MetricNameTemplate processTimeMaxMs;
    public MetricNameTemplate processCallsRate;
    public MetricNameTemplate punctuateTimeAvg;
    public MetricNameTemplate punctuateTimeMax;
    public MetricNameTemplate punctuateCallsRate;
    public MetricNameTemplate taskCreationRate;
    public MetricNameTemplate taskDestructionRate;

    public StreamThreadMetricsRegistry() {
        String groupName = "stream-metrics";

        this.commitTimeAvg = new MetricNameTemplate("commit-time-avg", groupName, "The average commit time in ms", "client-id");
        this.commitTimeMax = new MetricNameTemplate("commit-time-max", groupName, "The maximum commit time in ms", "client-id");
        this.commitCallsRate = new MetricNameTemplate("commit-calls-rate", groupName, "The average per-second number of commit calls", "client-id");

        this.pollTimeAvg = new MetricNameTemplate("poll-time-avg", groupName, "The average poll time in ms", "client-id");
        this.pollTimeMax = new MetricNameTemplate("poll-time-max", groupName, "The maximum poll time in ms", "client-id");
        this.pollCallsRate = new MetricNameTemplate("poll-calls-rate", groupName, "The average per-second number of record-poll calls", "client-id");

        this.processTimeAvgMs = new MetricNameTemplate("process-time-avg-ms", groupName, "The average process time in ms", "client-id");
        this.processTimeMaxMs = new MetricNameTemplate("process-time-max-ms", groupName, "The maximum process time in ms", "client-id");
        this.processCallsRate = new MetricNameTemplate("process-calls-rate", groupName, "The average per-second number of process calls", "client-id");

        this.punctuateTimeAvg = new MetricNameTemplate("punctuate-time-avg", groupName, "The average punctuate time in ms", "client-id");
        this.punctuateTimeMax = new MetricNameTemplate("punctuate-time-max", groupName, "The maximum punctuate time in ms", "client-id");
        this.punctuateCallsRate = new MetricNameTemplate("punctuate-calls-rate", groupName, "The average per-second number of punctuate calls", "client-id");

        this.taskCreationRate = new MetricNameTemplate("task-creation-rate", groupName, "The average per-second number of= newly created tasks", "client-id");

        this.taskDestructionRate = new MetricNameTemplate("task-destruction-rate", groupName, "The average per-second number of destructed tasks", "client-id");

    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(
                this.commitTimeAvg,
                this.commitTimeMax,
                this.commitCallsRate,
                this.pollTimeAvg,
                this.pollTimeMax,
                this.pollCallsRate,
                this.processTimeAvgMs,
                this.processTimeMaxMs,
                this.processCallsRate,
                this.punctuateTimeAvg,
                this.punctuateTimeMax,
                this.punctuateCallsRate,
                this.taskCreationRate,
                this.taskDestructionRate
                );
    }

}
