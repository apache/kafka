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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.streams.StreamsMetrics;

public class MockStreamsMetrics implements StreamsMetrics {

    final Metrics metrics;
    final String metricGrpName;
    Map<String, String> metricTags;

    Sensor commitTimeSensor;
    Sensor pollTimeSensor;
    Sensor processTimeSensor;
    Sensor punctuateTimeSensor;
    Sensor taskCreationSensor;
    Sensor taskDestructionSensor;

    public MockStreamsMetrics(Metrics metrics) {
        this.metrics = metrics;
        this.metricGrpName = "stream-metrics";
        this.metricTags = new LinkedHashMap<>();

        this.commitTimeSensor = metrics.sensor("commit-time");
        this.pollTimeSensor = metrics.sensor("poll-time");
        this.processTimeSensor = metrics.sensor("process-time");
        this.punctuateTimeSensor = metrics.sensor("punctuate-time");
        this.taskCreationSensor = metrics.sensor("task-creation");
        this.taskDestructionSensor = metrics.sensor("task-destruction");

    }

    @Override
    public Sensor addLatencySensor(String scopeName, String entityName, String operationName, Sensor.LogLevel logLevel, String... tags) {
        Map<String, String> tagMap = new HashMap<>();
        String metricGroupName = "stream-" + scopeName + "-metrics";

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor("sensorNamePrefix" + "." + scopeName + "-" + operationName, logLevel);
        addLatencyMetrics(metricGroupName, parent, "all", operationName, tagMap);

        // add the store operation metrics with additional tags
        Sensor sensor = metrics.sensor("sensorNamePrefix" + "." + scopeName + "-" + entityName + "-" + operationName, logLevel, parent);
        addLatencyMetrics(metricGroupName, sensor, entityName, operationName, tagMap);

        return sensor;
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


    @Override
    public void recordLatency(Sensor sensor, long startNs, long endNs) {
    }

    @Override
    public Sensor sensor(String name, Sensor.LogLevel logLevel) {
        return metrics.sensor(name, logLevel);
    }


    @Override
    public void removeSensor(String name) {
        metrics.removeSensor(name);
    }

    @Override
    public Sensor sensor(String name, MetricConfig config, Sensor.LogLevel logLevel, Sensor... parents) {
        return metrics.sensor(name, config, logLevel, parents);
    }
}
