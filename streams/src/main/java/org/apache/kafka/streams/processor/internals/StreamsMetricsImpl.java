/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.state.internals.ThreadCacheMetrics;

import java.util.HashMap;
import java.util.Map;

public class StreamsMetricsImpl implements StreamsMetrics, ThreadCacheMetrics {
    final Metrics metrics;
    final String metricGrpName;
    final String sensorNamePrefix;
    final Map<String, String> metricTags;

    final Sensor commitTimeSensor;
    final Sensor pollTimeSensor;
    final Sensor processTimeSensor;
    final Sensor punctuateTimeSensor;
    final Sensor taskCreationSensor;
    final Sensor taskDestructionSensor;

    public StreamsMetricsImpl(Metrics metrics, String metricGrpName, String sensorNamePrefix,
                              Map<String, String> tags) {
        this.metrics = metrics;
        this.metricGrpName = metricGrpName;
        this.sensorNamePrefix = sensorNamePrefix;
        this.metricTags = tags;


        this.commitTimeSensor = metrics.sensor(sensorNamePrefix + ".commit-time");
        this.commitTimeSensor.add(metrics.metricName("commit-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
        this.commitTimeSensor.add(metrics.metricName("commit-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
        this.commitTimeSensor.add(metrics.metricName("commit-calls-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

        this.pollTimeSensor = metrics.sensor(sensorNamePrefix + ".poll-time");
        this.pollTimeSensor.add(metrics.metricName("poll-time-avg", metricGrpName, "The average poll time in ms", metricTags), new Avg());
        this.pollTimeSensor.add(metrics.metricName("poll-time-max", metricGrpName, "The maximum poll time in ms", metricTags), new Max());
        this.pollTimeSensor.add(metrics.metricName("poll-calls-rate", metricGrpName, "The average per-second number of record-poll calls", metricTags), new Rate(new Count()));

        this.processTimeSensor = metrics.sensor(sensorNamePrefix + ".process-time");
        this.processTimeSensor.add(metrics.metricName("process-time-avg-ms", metricGrpName, "The average process time in ms", metricTags), new Avg());
        this.processTimeSensor.add(metrics.metricName("process-time-max-ms", metricGrpName, "The maximum process time in ms", metricTags), new Max());
        this.processTimeSensor.add(metrics.metricName("process-calls-rate", metricGrpName, "The average per-second number of process calls", metricTags), new Rate(new Count()));

        this.punctuateTimeSensor = metrics.sensor(sensorNamePrefix + ".punctuate-time");
        this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-avg", metricGrpName, "The average punctuate time in ms", metricTags), new Avg());
        this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-max", metricGrpName, "The maximum punctuate time in ms", metricTags), new Max());
        this.punctuateTimeSensor.add(metrics.metricName("punctuate-calls-rate", metricGrpName, "The average per-second number of punctuate calls", metricTags), new Rate(new Count()));

        this.taskCreationSensor = metrics.sensor(sensorNamePrefix + ".task-creation");
        this.taskCreationSensor.add(metrics.metricName("task-creation-rate", metricGrpName, "The average per-second number of newly created tasks", metricTags), new Rate(new Count()));

        this.taskDestructionSensor = metrics.sensor(sensorNamePrefix + ".task-destruction");
        this.taskDestructionSensor.add(metrics.metricName("task-destruction-rate", metricGrpName, "The average per-second number of destructed tasks", metricTags), new Rate(new Count()));
    }

    public Metrics metrics() {
        return metrics;
    }

    @Override
    public void recordLatency(Sensor sensor, long startNs, long endNs) {
        // TODO: sensor.record(time, timeMS) is faster in certain scenarios
        sensor.record(endNs - startNs);
    }

    @Override
    public void recordCacheSensor(Sensor sensor, double count) {
        sensor.record(count);
    }

    @Override
    public Sensor sensor(String name) {
        return metrics.sensor(name);
    }

    @Override
    public Sensor addSensor(String name, Sensor... parents) {
        return metrics.sensor(name, parents);
    }

    @Override
    public void removeSensor(String name) {
        metrics.removeSensor(name);
    }

    @Override
    public Sensor sensor(String name, MetricConfig config, Sensor... parents) {
        return metrics.sensor(name, config, parents);
    }

    @Override
    public Sensor getSensor(String name) {
        return metrics.getSensor(name);
    }


    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags) {
        // extract the additional tags if there are any
        Map<String, String> tagMap = new HashMap<>(this.metricTags);
        if ((tags.length % 2) != 0)
            throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");

        for (int i = 0; i < tags.length; i += 2)
            tagMap.put(tags[i], tags[i + 1]);

        String metricGroupName = "stream-" + scopeName + "-metrics";

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor(sensorNamePrefix + "." + scopeName + "-" + operationName);
        addLatencyMetrics(metricGroupName, parent, "all", operationName, this.metricTags);

        // add the store operation metrics with additional tags
        Sensor sensor = metrics.sensor(sensorNamePrefix + "." + scopeName + "-" + entityName + "-" + operationName, parent);
        addLatencyMetrics(metricGroupName, sensor, entityName, operationName, tagMap);

        return sensor;
    }

    @Override
    public Sensor addCacheSensor(String entityName, String operationName, String... tags) {
        // extract the additional tags if there are any
        Map<String, String> tagMap = new HashMap<>(this.metricTags);
        if ((tags.length % 2) != 0)
            throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");

        for (int i = 0; i < tags.length; i += 2)
            tagMap.put(tags[i], tags[i + 1]);

        String metricGroupName = "stream-thread-cache-metrics";

        Sensor sensor = metrics.sensor(sensorNamePrefix + "-" + entityName + "-" + operationName);
        addCacheMetrics(metricGroupName, sensor, entityName, operationName, tagMap);
        return sensor;

    }

    private void addCacheMetrics(String metricGrpName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg", metricGrpName,
            "The current count of " + entityName + " " + opName + " operation.", tags), new Avg());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-min", metricGrpName,
            "The current count of " + entityName + " " + opName + " operation.", tags), new Min());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max", metricGrpName,
            "The current count of " + entityName + " " + opName + " operation.", tags), new Max());
    }

    private void addLatencyMetrics(String metricGrpName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg-latency-ms", metricGrpName,
            "The average latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Avg());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max-latency-ms", metricGrpName,
            "The max latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Max());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-qps", metricGrpName,
            "The average number of occurrence of " + entityName + " " + opName + " operation per second.", tags), new Rate(new Count()));
    }

    private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
        if (!metrics.metrics().containsKey(name))
            sensor.add(name, stat);
    }
}