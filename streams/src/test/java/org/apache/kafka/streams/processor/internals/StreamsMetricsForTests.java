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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsMetrics;

public class StreamsMetricsForTests implements StreamsMetrics {

    final Metrics metrics;
    final String metricGrpName;
    Map<String, String> metricTags;

    Sensor commitTimeSensor;
    Sensor pollTimeSensor;
    Sensor processTimeSensor;
    Sensor punctuateTimeSensor;
    Sensor taskCreationSensor;
    Sensor taskDestructionSensor;

    public StreamsMetricsForTests(Metrics metrics) {
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
    public Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags) {
        Sensor sensor = metrics.sensor(scopeName + "-" + entityName + "-" + operationName);
        return sensor;
    }


    @Override
    public void recordLatency(Sensor sensor, long startNs, long endNs) {
        sensor.record((endNs - startNs) / 1000000, endNs);
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

}
