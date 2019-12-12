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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP_0100_TO_24;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;

public class ThreadMetrics {
    private ThreadMetrics() {}

    private static final String COMMIT = "commit";
    private static final String COMMIT_LATENCY = COMMIT + LATENCY_SUFFIX;
    private static final String POLL = "poll";
    private static final String PROCESS = "process";
    private static final String PUNCTUATE = "punctuate";
    private static final String CREATE_TASK = "task-created";
    private static final String CLOSE_TASK = "task-closed";
    private static final String SKIP_RECORD = "skipped-records";

    private static final String COMMIT_DESCRIPTION = "calls to commit";
    private static final String COMMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String COMMIT_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String COMMIT_AVG_LATENCY_DESCRIPTION = "The average commit latency";
    private static final String COMMIT_MAX_LATENCY_DESCRIPTION = "The maximum commit latency";
    private static final String CREATE_TASK_DESCRIPTION = "newly created tasks";
    private static final String CREATE_TASK_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CREATE_TASK_DESCRIPTION;
    private static final String CREATE_TASK_RATE_DESCRIPTION = RATE_DESCRIPTION + CREATE_TASK_DESCRIPTION;
    private static final String CLOSE_TASK_DESCRIPTION = "closed tasks";
    private static final String CLOSE_TASK_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
    private static final String CLOSE_TASK_RATE_DESCRIPTION = RATE_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
    private static final String POLL_DESCRIPTION = "calls to poll";
    private static final String POLL_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + POLL_DESCRIPTION;
    private static final String POLL_RATE_DESCRIPTION = RATE_DESCRIPTION + POLL_DESCRIPTION;
    private static final String POLL_AVG_LATENCY_DESCRIPTION = "The average poll latency";
    private static final String POLL_MAX_LATENCY_DESCRIPTION = "The maximum poll latency";
    private static final String PROCESS_DESCRIPTION = "calls to process";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION = RATE_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_AVG_LATENCY_DESCRIPTION = "The average process latency";
    private static final String PROCESS_MAX_LATENCY_DESCRIPTION = "The maximum process latency";
    private static final String PUNCTUATE_DESCRIPTION = "calls to punctuate";
    private static final String PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_RATE_DESCRIPTION = RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_AVG_LATENCY_DESCRIPTION = "The average punctuate latency";
    private static final String PUNCTUATE_MAX_LATENCY_DESCRIPTION = "The maximum punctuate latency";
    private static final String SKIP_RECORDS_DESCRIPTION = "skipped records";
    private static final String SKIP_RECORD_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SKIP_RECORDS_DESCRIPTION;
    private static final String SKIP_RECORD_RATE_DESCRIPTION = RATE_DESCRIPTION + SKIP_RECORDS_DESCRIPTION;
    private static final String COMMIT_OVER_TASKS_DESCRIPTION =
        "calls to commit over all tasks assigned to one stream thread";
    private static final String COMMIT_OVER_TASKS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
    private static final String COMMIT_OVER_TASKS_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
    private static final String COMMIT_OVER_TASKS_AVG_LATENCY_DESCRIPTION =
        "The average commit latency over all tasks assigned to one stream thread";
    private static final String COMMIT_OVER_TASKS_MAX_LATENCY_DESCRIPTION =
        "The maximum commit latency over all tasks assigned to one stream thread";

    public static Sensor createTaskSensor(final String threadId,
                                          final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountSensor(
            threadId,
            CREATE_TASK,
            CREATE_TASK_RATE_DESCRIPTION,
            CREATE_TASK_TOTAL_DESCRIPTION,
            RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor closeTaskSensor(final String threadId,
                                         final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountSensor(
            threadId,
            CLOSE_TASK,
            CLOSE_TASK_RATE_DESCRIPTION,
            CLOSE_TASK_TOTAL_DESCRIPTION,
            RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor skipRecordSensor(final String threadId,
                                          final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountSensor(
            threadId,
            SKIP_RECORD,
            SKIP_RECORD_RATE_DESCRIPTION,
            SKIP_RECORD_TOTAL_DESCRIPTION,
            RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor commitSensor(final String threadId,
                                      final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountAndAvgAndMaxLatencySensor(
            threadId,
            COMMIT,
            COMMIT_RATE_DESCRIPTION,
            COMMIT_TOTAL_DESCRIPTION,
            COMMIT_AVG_LATENCY_DESCRIPTION,
            COMMIT_MAX_LATENCY_DESCRIPTION,
            Sensor.RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor pollSensor(final String threadId,
                                    final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountAndAvgAndMaxLatencySensor(
            threadId,
            POLL,
            POLL_RATE_DESCRIPTION,
            POLL_TOTAL_DESCRIPTION,
            POLL_AVG_LATENCY_DESCRIPTION,
            POLL_MAX_LATENCY_DESCRIPTION,
            Sensor.RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor processSensor(final String threadId,
                                       final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountAndAvgAndMaxLatencySensor(
            threadId,
            PROCESS,
            PROCESS_RATE_DESCRIPTION,
            PROCESS_TOTAL_DESCRIPTION,
            PROCESS_AVG_LATENCY_DESCRIPTION,
            PROCESS_MAX_LATENCY_DESCRIPTION,
            Sensor.RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor punctuateSensor(final String threadId,
                                         final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountAndAvgAndMaxLatencySensor(
            threadId,
            PUNCTUATE,
            PUNCTUATE_RATE_DESCRIPTION,
            PUNCTUATE_TOTAL_DESCRIPTION,
            PUNCTUATE_AVG_LATENCY_DESCRIPTION,
            PUNCTUATE_MAX_LATENCY_DESCRIPTION,
            Sensor.RecordingLevel.INFO,
            streamsMetrics
        );
    }

    public static Sensor commitOverTasksSensor(final String threadId,
                                               final StreamsMetricsImpl streamsMetrics) {
        final Sensor commitOverTasksSensor =
            streamsMetrics.threadLevelSensor(threadId, COMMIT, Sensor.RecordingLevel.DEBUG);
        final Map<String, String> tagMap = streamsMetrics.taskLevelTagMap(threadId, ROLLUP_VALUE);
        addAvgAndMaxToSensor(
            commitOverTasksSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            COMMIT_LATENCY,
            COMMIT_OVER_TASKS_AVG_LATENCY_DESCRIPTION,
            COMMIT_OVER_TASKS_MAX_LATENCY_DESCRIPTION
        );
        addInvocationRateAndCountToSensor(
            commitOverTasksSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            COMMIT,
            COMMIT_OVER_TASKS_RATE_DESCRIPTION,
            COMMIT_OVER_TASKS_TOTAL_DESCRIPTION
        );
        return commitOverTasksSensor;
    }

    private static Sensor invocationRateAndCountSensor(final String threadId,
                                                       final String metricName,
                                                       final String descriptionOfRate,
                                                       final String descriptionOfCount,
                                                       final RecordingLevel recordingLevel,
                                                       final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, metricName, recordingLevel);
        addInvocationRateAndCountToSensor(
            sensor,
            threadLevelGroup(streamsMetrics),
            streamsMetrics.threadLevelTagMap(threadId),
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }

    private static Sensor invocationRateAndCountAndAvgAndMaxLatencySensor(final String threadId,
                                                                          final String metricName,
                                                                          final String descriptionOfRate,
                                                                          final String descriptionOfCount,
                                                                          final String descriptionOfAvg,
                                                                          final String descriptionOfMax,
                                                                          final RecordingLevel recordingLevel,
                                                                          final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, metricName, recordingLevel);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        final String threadLevelGroup = threadLevelGroup(streamsMetrics);
        addAvgAndMaxToSensor(
            sensor,
            threadLevelGroup,
            tagMap,
            metricName + LATENCY_SUFFIX,
            descriptionOfAvg,
            descriptionOfMax
        );
        addInvocationRateAndCountToSensor(
            sensor,
            threadLevelGroup,
            tagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }

    private static String threadLevelGroup(final StreamsMetricsImpl streamsMetrics) {
        return streamsMetrics.version() == Version.LATEST ? THREAD_LEVEL_GROUP : THREAD_LEVEL_GROUP_0100_TO_24;
    }
}
