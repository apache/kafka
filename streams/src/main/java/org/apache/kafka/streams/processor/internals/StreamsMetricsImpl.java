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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.state.internals.ThreadCacheMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class StreamsMetricsImpl implements StreamsMetrics, ThreadCacheMetrics {
    private static final Logger log = LoggerFactory.getLogger(StreamsMetricsImpl.class);

    final Metrics metrics;
    final String groupName;
    final String prefix;
    final Map<String, String> tags;

    final Sensor commitTimeSensor;
    final Sensor pollTimeSensor;
    final Sensor processTimeSensor;
    final Sensor punctuateTimeSensor;
    final Sensor taskCreationSensor;
    final Sensor taskDestructionSensor;
    final Sensor skippedRecordsSensor;


    public StreamsMetricsImpl(Metrics metrics, String groupName, String prefix, Map<String, String> tags) {
        this.metrics = metrics;
        this.groupName = groupName;
        this.prefix = prefix;
        this.tags = tags;


        this.commitTimeSensor = metrics.sensor(this.prefix + ".commit-time", Sensor.RecordLevel.SENSOR_INFO);
        this.commitTimeSensor.add(metrics.metricName("commit-time-avg", this.groupName, "The average commit time in ms", this.tags), new Avg());
        this.commitTimeSensor.add(metrics.metricName("commit-time-max", this.groupName, "The maximum commit time in ms", this.tags), new Max());
        this.commitTimeSensor.add(metrics.metricName("commit-calls-rate", this.groupName, "The average per-second number of commit calls", this.tags), new Rate(new Count()));

        this.pollTimeSensor = metrics.sensor(this.prefix + ".poll-time", Sensor.RecordLevel.SENSOR_INFO);
        this.pollTimeSensor.add(metrics.metricName("poll-time-avg", this.groupName, "The average poll time in ms", this.tags), new Avg());
        this.pollTimeSensor.add(metrics.metricName("poll-time-max", this.groupName, "The maximum poll time in ms", this.tags), new Max());
        this.pollTimeSensor.add(metrics.metricName("poll-calls-rate", this.groupName, "The average per-second number of record-poll calls", this.tags), new Rate(new Count()));

        this.processTimeSensor = metrics.sensor(this.prefix + ".process-time", Sensor.RecordLevel.SENSOR_INFO);
        this.processTimeSensor.add(metrics.metricName("process-time-avg", this.groupName, "The average process time in ms", this.tags), new Avg());
        this.processTimeSensor.add(metrics.metricName("process-time-max", this.groupName, "The maximum process time in ms", this.tags), new Max());
        this.processTimeSensor.add(metrics.metricName("process-calls-rate", this.groupName, "The average per-second number of process calls", this.tags), new Rate(new Count()));

        this.punctuateTimeSensor = metrics.sensor(this.prefix + ".punctuate-time", Sensor.RecordLevel.SENSOR_INFO);
        this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-avg", this.groupName, "The average punctuate time in ms", this.tags), new Avg());
        this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-max", this.groupName, "The maximum punctuate time in ms", this.tags), new Max());
        this.punctuateTimeSensor.add(metrics.metricName("punctuate-calls-rate", this.groupName, "The average per-second number of punctuate calls", this.tags), new Rate(new Count()));

        this.taskCreationSensor = metrics.sensor(this.prefix + ".task-creation", Sensor.RecordLevel.SENSOR_INFO);
        this.taskCreationSensor.add(metrics.metricName("task-creation-rate", this.groupName, "The average per-second number of newly created tasks", this.tags), new Rate(new Count()));

        this.taskDestructionSensor = metrics.sensor(this.prefix + ".task-destruction", Sensor.RecordLevel.SENSOR_INFO);
        this.taskDestructionSensor.add(metrics.metricName("task-destruction-rate", this.groupName, "The average per-second number of destructed tasks", this.tags), new Rate(new Count()));

        this.skippedRecordsSensor = metrics.sensor(this.prefix + ".skipped-records");
        this.skippedRecordsSensor.add(metrics.metricName("skipped-records-count", this.groupName, "The average per-second number of skipped records.", this.tags), new Rate(new Count()));
    }

    public Metrics metrics() {
        return metrics;
    }

    @Override
    public void recordLatency(Sensor sensor, long startNs, long endNs) {
        sensor.record(endNs - startNs);
    }

    @Override
    public void recordCacheSensor(Sensor sensor, double count) {
        sensor.record(count);
    }

    @Override
    public Sensor sensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel) {
        return metrics.sensor(sensorName(operationName, entityName), recordLevel);
    }

    @Override
    public Sensor sensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, Sensor... parents) {
        return metrics.sensor(sensorName(operationName, entityName), recordLevel, parents);
    }

    @Override
    public void removeSensor(String name) {
        metrics.removeSensor(name);
    }

    private String groupNameFromScope(String scopeName) {
        return "stream-" + scopeName + "-metrics";
    }

    private String sensorName(String operationName, String entityName) {
        if (entityName == null) {
            return prefix + "." + operationName;
        } else {
            return prefix + "." + entityName + "-" + operationName;
        }
    }

    private Map<String, String> tagMap(String... tags) {
        // extract the additional tags if there are any
        Map<String, String> tagMap = new HashMap<>(this.tags);
        if ((tags.length % 2) != 0)
            throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");

        for (int i = 0; i < tags.length; i += 2)
            tagMap.put(tags[i], tags[i + 1]);

        return tagMap;
    }



    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addLatencySensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, String... tags) {
        Map<String, String> tagMap = tagMap(tags);

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor(sensorName(operationName, null), recordLevel);
        addLatencyMetrics(scopeName, parent, "all", operationName, tagMap);

        // add the operation metrics with additional tags
        Sensor sensor = metrics.sensor(sensorName(operationName, entityName), recordLevel, parent);
        addLatencyMetrics(scopeName, sensor, entityName, operationName, tagMap);

        return sensor;
    }

    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addThroughputSensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, String... tags) {
        Map<String, String> tagMap = tagMap(tags);

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor(sensorName(operationName, null), recordLevel);
        addThroughputMetrics(scopeName, parent, "all", operationName, tagMap);

        // add the operation metrics with additional tags
        Sensor sensor = metrics.sensor(sensorName(operationName, entityName), recordLevel, parent);
        addThroughputMetrics(scopeName, sensor, entityName, operationName, tagMap);

        return sensor;
    }

    @Override
    public Sensor addCacheSensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel,  String... tags) {
        Map<String, String> tagMap = tagMap(tags);

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor(sensorName(operationName, null), recordLevel);
        addCacheMetrics(scopeName, parent, "all", operationName, tagMap);

        // add additional parents
        Sensor sensor = metrics.sensor(sensorName(operationName, entityName), recordLevel, parent);
        addCacheMetrics(scopeName, sensor, entityName, operationName, tagMap);
        return sensor;

    }

    private void addCacheMetrics(String scopeName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg", groupNameFromScope(scopeName),
            "The current count of " + entityName + " " + opName + " operation.", tags), new Avg());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-min", groupNameFromScope(scopeName),
            "The current count of " + entityName + " " + opName + " operation.", tags), new Min());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max", groupNameFromScope(scopeName),
            "The current count of " + entityName + " " + opName + " operation.", tags), new Max());
    }

    private void addLatencyMetrics(String scopeName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg-latency", groupNameFromScope(scopeName),
            "The average latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Avg());
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max-latency", groupNameFromScope(scopeName),
            "The max latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Max());
        addThroughputMetrics(scopeName, sensor, entityName, opName, tags);
    }

    private void addThroughputMetrics(String scopeName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
        maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-qps", groupNameFromScope(scopeName),
            "The average number of occurrence of " + entityName + " " + opName + " operation per second.", tags), new Rate(new Count()));
    }

    private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
        if (name.toString().contains("all-process-avg-latency")) {
            log.warn("Trying to add metric twice " + name);
        }
        if (!metrics.metrics().containsKey(name)) {
            sensor.add(name, stat);
        } else {
            log.debug("Trying to add metric twice " + name);
        }
    }
}
