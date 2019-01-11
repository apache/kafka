/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgMaxLatency;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public final class Sensors {
    private Sensors() {}

    public static Sensor createTaskAndStoreLatencyAndThroughputSensors(final Sensor.RecordingLevel level,
                                                                       final String operation,
                                                                       final StreamsMetricsImpl metrics,
                                                                       final String metricsGroup,
                                                                       final String taskName,
                                                                       final String storeName,
                                                                       final Map<String, String> taskTags,
                                                                       final Map<String, String> storeTags) {
        final Sensor taskSensor = metrics.taskLevelSensor(taskName, operation, level);
        addAvgMaxLatency(taskSensor, metricsGroup, taskTags, operation);
        addInvocationRateAndCount(taskSensor, metricsGroup, taskTags, operation);
        final Sensor sensor = metrics.storeLevelSensor(taskName, storeName, operation, level, taskSensor);
        addAvgMaxLatency(sensor, metricsGroup, storeTags, operation);
        addInvocationRateAndCount(sensor, metricsGroup, storeTags, operation);
        return sensor;
    }

    public static Sensor createBufferSizeSensor(final StateStore store,
                                                final InternalProcessorContext context) {
        return getBufferSizeOrCountSensor(store, context, "size");
    }

    public static Sensor createBufferCountSensor(final StateStore store,
                                                 final InternalProcessorContext context) {
        return getBufferSizeOrCountSensor(store, context, "count");
    }

    private static Sensor getBufferSizeOrCountSensor(final StateStore store,
                                                     final InternalProcessorContext context,
                                                     final String property) {
        final StreamsMetricsImpl metrics = context.metrics();

        final String sensorName = "suppression-buffer-" + property;

        final Sensor sensor = metrics.storeLevelSensor(
            context.taskId().toString(),
            store.name(),
            sensorName,
            Sensor.RecordingLevel.DEBUG
        );

        final String metricsGroup = "stream-buffer-metrics";

        final Map<String, String> tags = metrics.tagMap(
            "task-id", context.taskId().toString(),
            "buffer-id", store.name()
        );

        sensor.add(
            new MetricName(
                sensorName + "-current",
                metricsGroup,
                "The current " + property + " of buffered records.",
                tags),
            new Value()
        );


        sensor.add(
            new MetricName(
                sensorName + "-avg",
                metricsGroup,
                "The average " + property + " of buffered records.",
                tags),
            new Avg()
        );

        sensor.add(
            new MetricName(
                sensorName + "-max",
                metricsGroup,
                "The max " + property + " of buffered records.",
                tags),
            new Max()
        );

        return sensor;
    }
}

