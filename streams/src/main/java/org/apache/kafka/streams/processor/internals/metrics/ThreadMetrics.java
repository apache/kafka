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

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamThreadTotalBlockedTime;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATIO_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORDS_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addRateOfSumAndSumMetricsToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addValueMetricToSensor;

public class ThreadMetrics {
    private ThreadMetrics() {}

    private static final String COMMIT = "commit";
    private static final String POLL = "poll";
    private static final String PROCESS = "process";
    private static final String PUNCTUATE = "punctuate";
    private static final String CREATE_TASK = "task-created";
    private static final String CLOSE_TASK = "task-closed";
    private static final String BLOCKED_TIME = "blocked-time-ns-total";
    private static final String STATE  = "state";
    private static final String THREAD_START_TIME = "thread-start-time";
    private static final String THREAD_STATE = "thread-state";

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
    private static final String POLL_AVG_RECORDS_DESCRIPTION = "The average number of records polled from consumer within an iteration";
    private static final String POLL_MAX_RECORDS_DESCRIPTION = "The maximum number of records polled from consumer within an iteration";
    private static final String PROCESS_DESCRIPTION = "calls to process";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION = RATE_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_AVG_LATENCY_DESCRIPTION = "The average process latency";
    private static final String PROCESS_MAX_LATENCY_DESCRIPTION = "The maximum process latency";
    private static final String PROCESS_AVG_RECORDS_DESCRIPTION = "The average number of records processed within an iteration";
    private static final String PROCESS_MAX_RECORDS_DESCRIPTION = "The maximum number of records processed within an iteration";
    private static final String PUNCTUATE_DESCRIPTION = "calls to punctuate";
    private static final String PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_RATE_DESCRIPTION = RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_AVG_LATENCY_DESCRIPTION = "The average punctuate latency";
    private static final String PUNCTUATE_MAX_LATENCY_DESCRIPTION = "The maximum punctuate latency";
    private static final String PROCESS_RATIO_DESCRIPTION =
        "The fraction of time the thread spent on processing active tasks";
    private static final String PUNCTUATE_RATIO_DESCRIPTION =
        "The fraction of time the thread spent on punctuating active tasks";
    private static final String POLL_RATIO_DESCRIPTION =
        "The fraction of time the thread spent on polling records from consumer";
    private static final String COMMIT_RATIO_DESCRIPTION =
        "The fraction of time the thread spent on committing all tasks";
    private static final String BLOCKED_TIME_DESCRIPTION =
        "The total time the thread spent blocked on kafka in nanoseconds";
    private static final String THREAD_START_TIME_DESCRIPTION =
        "The time that the thread was started";
    private static final String THREAD_STATE_DESCRIPTION =
        "The current state of the thread";

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

    public static Sensor processLatencySensor(final String threadId,
                                              final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, PROCESS + LATENCY_SUFFIX, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addAvgAndMaxToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            PROCESS + LATENCY_SUFFIX,
            PROCESS_AVG_LATENCY_DESCRIPTION,
            PROCESS_MAX_LATENCY_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor pollRecordsSensor(final String threadId,
                                           final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, POLL + RECORDS_SUFFIX, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addAvgAndMaxToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            POLL + RECORDS_SUFFIX,
            POLL_AVG_RECORDS_DESCRIPTION,
            POLL_MAX_RECORDS_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor processRecordsSensor(final String threadId,
                                              final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, PROCESS + RECORDS_SUFFIX, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addAvgAndMaxToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            PROCESS + RECORDS_SUFFIX,
            PROCESS_AVG_RECORDS_DESCRIPTION,
            PROCESS_MAX_RECORDS_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor processRateSensor(final String threadId,
                                           final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, PROCESS + RATE_SUFFIX, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addRateOfSumAndSumMetricsToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            PROCESS,
            PROCESS_RATE_DESCRIPTION,
            PROCESS_TOTAL_DESCRIPTION
        );
        return sensor;
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

    public static Sensor processRatioSensor(final String threadId,
                                            final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, PROCESS + RATIO_SUFFIX, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addValueMetricToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            PROCESS + RATIO_SUFFIX,
            PROCESS_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor punctuateRatioSensor(final String threadId,
                                              final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, PUNCTUATE + RATIO_SUFFIX, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addValueMetricToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            PUNCTUATE + RATIO_SUFFIX,
            PUNCTUATE_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor pollRatioSensor(final String threadId,
                                         final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, POLL + RATIO_SUFFIX, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addValueMetricToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            POLL + RATIO_SUFFIX,
            POLL_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor commitRatioSensor(final String threadId,
                                           final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.threadLevelSensor(threadId, COMMIT + RATIO_SUFFIX, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addValueMetricToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            COMMIT + RATIO_SUFFIX,
            COMMIT_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static void addThreadStartTimeMetric(final String threadId,
                                                final StreamsMetricsImpl streamsMetrics,
                                                final long startTime) {
        streamsMetrics.addThreadLevelImmutableMetric(
            THREAD_START_TIME,
            THREAD_START_TIME_DESCRIPTION,
            threadId,
            startTime
        );
    }

    public static void addThreadStateTelemetryMetric(final String threadId,
                                                     final StreamsMetricsImpl streamsMetrics,
                                                     final Gauge<Integer> threadStateProvider) {
        streamsMetrics.addThreadLevelMutableMetric(
            THREAD_STATE,
            THREAD_STATE_DESCRIPTION,
            threadId,
            threadStateProvider
        );
    }

    public static void addThreadStateMetric(final String threadId,
                                            final StreamsMetricsImpl streamsMetrics,
                                            final Gauge<StreamThread.State> threadStateProvider) {
        streamsMetrics.addThreadLevelMutableMetric(
            STATE,
            THREAD_STATE_DESCRIPTION,
            threadId,
            threadStateProvider
        );
    }



    public static void addThreadBlockedTimeMetric(final String threadId,
                                                  final StreamThreadTotalBlockedTime blockedTime,
                                                  final StreamsMetricsImpl streamsMetrics) {
        streamsMetrics.addThreadLevelMutableMetric(
            BLOCKED_TIME,
            BLOCKED_TIME_DESCRIPTION,
            threadId,
            (config, now) -> blockedTime.compute()
        );
    }

    private static Sensor invocationRateAndCountSensor(final String threadId,
                                                       final String operation,
                                                       final String descriptionOfRate,
                                                       final String descriptionOfCount,
                                                       final RecordingLevel recordingLevel,
                                                       final StreamsMetricsImpl streamsMetrics) {
        // use operation name as the sensor suffix, and metric names
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, operation, recordingLevel);
        addInvocationRateAndCountToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            streamsMetrics.threadLevelTagMap(threadId),
            operation,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }

    private static Sensor invocationRateAndCountAndAvgAndMaxLatencySensor(final String threadId,
                                                                          final String operation,
                                                                          final String descriptionOfRate,
                                                                          final String descriptionOfCount,
                                                                          final String descriptionOfAvg,
                                                                          final String descriptionOfMax,
                                                                          final RecordingLevel recordingLevel,
                                                                          final StreamsMetricsImpl streamsMetrics) {
        // use operation name as the sensor suffix, and metric names
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, operation, recordingLevel);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(threadId);
        addAvgAndMaxToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            operation + LATENCY_SUFFIX,
            descriptionOfAvg,
            descriptionOfMax
        );
        addInvocationRateAndCountToSensor(
            sensor,
            THREAD_LEVEL_GROUP,
            tagMap,
            operation,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }
}
