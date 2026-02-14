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
package org.apache.kafka.streams.processor.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;

public class RebalanceListenerMetrics {
    private RebalanceListenerMetrics() {}

    private static final String TASKS_REVOKED = "tasks-revoked";
    private static final String TASKS_ASSIGNED = "tasks-assigned";
    private static final String TASKS_LOST = "tasks-lost";

    private static final String TASKS_REVOKED_AVG_LATENCY_DESCRIPTION = "The average time taken for tasks-revoked rebalance listener callback";
    private static final String TASKS_REVOKED_MAX_LATENCY_DESCRIPTION = "The max time taken for tasks-revoked rebalance listener callback";
    private static final String TASKS_ASSIGNED_AVG_LATENCY_DESCRIPTION = "The average time taken for tasks-assigned rebalance listener callback";
    private static final String TASKS_ASSIGNED_MAX_LATENCY_DESCRIPTION = "The max time taken for tasks-assigned rebalance listener callback";
    private static final String TASKS_LOST_AVG_LATENCY_DESCRIPTION = "The average time taken for tasks-lost rebalance listener callback";
    private static final String TASKS_LOST_MAX_LATENCY_DESCRIPTION = "The max time taken for tasks-lost rebalance listener callback";

    public static Sensor tasksRevokedSensor(final String threadId,
                                            final StreamsMetricsImpl streamsMetrics) {
        return rebalanceLatencySensor(
            threadId,
            TASKS_REVOKED,
            TASKS_REVOKED_AVG_LATENCY_DESCRIPTION,
            TASKS_REVOKED_MAX_LATENCY_DESCRIPTION,
            streamsMetrics
        );
    }

    public static Sensor tasksAssignedSensor(final String threadId,
                                             final StreamsMetricsImpl streamsMetrics) {
        return rebalanceLatencySensor(
            threadId,
            TASKS_ASSIGNED,
            TASKS_ASSIGNED_AVG_LATENCY_DESCRIPTION,
            TASKS_ASSIGNED_MAX_LATENCY_DESCRIPTION,
            streamsMetrics
        );
    }

    public static Sensor tasksLostSensor(final String threadId,
                                         final StreamsMetricsImpl streamsMetrics) {
        return rebalanceLatencySensor(
            threadId,
            TASKS_LOST,
            TASKS_LOST_AVG_LATENCY_DESCRIPTION,
            TASKS_LOST_MAX_LATENCY_DESCRIPTION,
            streamsMetrics
        );
    }

    private static Sensor rebalanceLatencySensor(final String threadId,
                                                 final String operation,
                                                 final String avgDescription,
                                                 final String maxDescription,
                                                 final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, operation + LATENCY_SUFFIX, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addAvgAndMaxToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            operation + LATENCY_SUFFIX,
            avgDescription,
            maxDescription
        );
        return sensor;
    }
}