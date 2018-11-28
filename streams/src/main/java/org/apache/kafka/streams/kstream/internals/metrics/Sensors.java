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
package org.apache.kafka.streams.kstream.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Sum;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP;

public class Sensors {
    private Sensors() {}

    public static Sensor lateRecordDropSensor(final InternalProcessorContext context) {
        final StreamsMetricsImpl metrics = context.metrics();
        final Sensor sensor = metrics.nodeLevelSensor(
            context.taskId().toString(),
            context.currentNode().name(),
            "late-record-drop",
            Sensor.RecordingLevel.INFO
        );
        StreamsMetricsImpl.addInvocationRateAndCount(
            sensor,
            PROCESSOR_NODE_METRICS_GROUP,
            metrics.tagMap("task-id", context.taskId().toString(), PROCESSOR_NODE_ID_TAG, context.currentNode().name()),
            "late-record-drop"
        );
        return sensor;
    }

    public static Sensor recordLatenessSensor(final InternalProcessorContext context) {
        final StreamsMetricsImpl metrics = context.metrics();

        final Sensor sensor = metrics.taskLevelSensor(
            context.taskId().toString(),
            "record-lateness",
            Sensor.RecordingLevel.DEBUG
        );

        final Map<String, String> tags = metrics.tagMap(
            "task-id", context.taskId().toString()
        );
        sensor.add(
            new MetricName(
                "record-lateness-avg",
                "stream-task-metrics",
                "The average observed lateness of records.",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                "record-lateness-max",
                "stream-task-metrics",
                "The max observed lateness of records.",
                tags),
            new Max()
        );
        return sensor;
    }

    public static Sensor suppressionEmitSensor(final InternalProcessorContext context) {
        final StreamsMetricsImpl metrics = context.metrics();

        final Sensor sensor = metrics.nodeLevelSensor(
            context.taskId().toString(),
            context.currentNode().name(),
            "suppression-emit",
            Sensor.RecordingLevel.DEBUG
        );

        final Map<String, String> tags = metrics.tagMap(
            "task-id", context.taskId().toString(),
            PROCESSOR_NODE_ID_TAG, context.currentNode().name()
        );

        sensor.add(
            new MetricName(
                "suppression-emit-rate",
                PROCESSOR_NODE_METRICS_GROUP,
                "The average number of occurrence of suppression-emit operation per second.",
                tags
            ),
            new Rate(TimeUnit.SECONDS, new Sum())
        );
        sensor.add(
            new MetricName(
                "suppression-emit-total",
                PROCESSOR_NODE_METRICS_GROUP,
                "The total number of occurrence of suppression-emit operations.",
                tags
            ),
            new Total()
        );
        return sensor;
    }
}
