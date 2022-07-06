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
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATIO_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addValueMetricToSensor;

public class TaskMetrics {
    private TaskMetrics() {}

    private static final String AVG_LATENCY_DESCRIPTION = "The average latency of ";
    private static final String MAX_LATENCY_DESCRIPTION = "The maximum latency of ";
    private static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    private static final String RATE_DESCRIPTION_SUFFIX = " per second";
    private static final String ACTIVE_TASK_PREFIX = "active-";

    private static final String COMMIT = "commit";
    private static final String COMMIT_DESCRIPTION = "calls to commit";
    private static final String COMMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String COMMIT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + COMMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String PUNCTUATE = "punctuate";
    private static final String PUNCTUATE_DESCRIPTION = "calls to punctuate";
    private static final String PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PUNCTUATE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PUNCTUATE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION + PUNCTUATE_DESCRIPTION;

    private static final String ENFORCED_PROCESSING = "enforced-processing";
    private static final String ENFORCED_PROCESSING_TOTAL_DESCRIPTION =
        "The total number of occurrences of enforced-processing operations";
    private static final String ENFORCED_PROCESSING_RATE_DESCRIPTION =
        "The average number of occurrences of enforced-processing operations per second";

    private static final String RECORD_LATENESS = "record-lateness";
    private static final String RECORD_LATENESS_MAX_DESCRIPTION =
        "The observed maximum lateness of records in milliseconds, measured by comparing the record timestamp with the "
            + "current stream time";
    private static final String RECORD_LATENESS_AVG_DESCRIPTION =
        "The observed average lateness of records in milliseconds, measured by comparing the record timestamp with the "
            + "current stream time";

    private static final String DROPPED_RECORDS = "dropped-records";
    private static final String DROPPED_RECORDS_DESCRIPTION = "dropped records";
    private static final String DROPPED_RECORDS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + DROPPED_RECORDS_DESCRIPTION;
    private static final String DROPPED_RECORDS_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + DROPPED_RECORDS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String PROCESS = "process";
    private static final String PROCESS_LATENCY = PROCESS + LATENCY_SUFFIX;
    private static final String PROCESS_DESCRIPTION = "calls to process";
    private static final String PROCESS_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATIO_DESCRIPTION = "The fraction of time the thread spent " +
        "on processing this task among all assigned active tasks";

    private static final String BUFFER_COUNT = "buffer-count";
    private static final String NUM_BUFFERED_RECORDS_DESCRIPTION = "The count of buffered records that are polled " +
        "from consumer and not yet processed for this active task";

    public static Sensor processLatencySensor(final String threadId,
                                              final String taskId,
                                              final StreamsMetricsImpl streamsMetrics) {
        return avgAndMaxSensor(
            threadId,
            taskId,
            PROCESS_LATENCY,
            PROCESS_AVG_LATENCY_DESCRIPTION,
            PROCESS_MAX_LATENCY_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor activeProcessRatioSensor(final String threadId,
                                                  final String taskId,
                                                  final StreamsMetricsImpl streamsMetrics) {
        final String name = ACTIVE_TASK_PREFIX + PROCESS + RATIO_SUFFIX;
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, name, Sensor.RecordingLevel.INFO);
        addValueMetricToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(threadId, taskId),
            name,
            PROCESS_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor activeBufferedRecordsSensor(final String threadId,
                                                     final String taskId,
                                                     final StreamsMetricsImpl streamsMetrics) {
        final String name = ACTIVE_TASK_PREFIX + BUFFER_COUNT;
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, name, Sensor.RecordingLevel.DEBUG);
        addValueMetricToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(threadId, taskId),
            name,
            NUM_BUFFERED_RECORDS_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor punctuateSensor(final String threadId,
                                         final String taskId,
                                         final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountAndAvgAndMaxLatencySensor(
            threadId,
            taskId,
            PUNCTUATE,
            PUNCTUATE_RATE_DESCRIPTION,
            PUNCTUATE_TOTAL_DESCRIPTION,
            PUNCTUATE_AVG_LATENCY_DESCRIPTION,
            PUNCTUATE_MAX_LATENCY_DESCRIPTION,
            Sensor.RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor commitSensor(final String threadId,
                                      final String taskId,
                                      final StreamsMetricsImpl streamsMetrics,
                                      final Sensor... parentSensor) {
        return invocationRateAndCountSensor(
            threadId,
            taskId,
            COMMIT,
            COMMIT_RATE_DESCRIPTION,
            COMMIT_TOTAL_DESCRIPTION,
            Sensor.RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensor
        );
    }

    public static Sensor enforcedProcessingSensor(final String threadId,
                                                  final String taskId,
                                                  final StreamsMetricsImpl streamsMetrics,
                                                  final Sensor... parentSensors) {
        return invocationRateAndCountSensor(
            threadId,
            taskId,
            ENFORCED_PROCESSING,
            ENFORCED_PROCESSING_RATE_DESCRIPTION,
            ENFORCED_PROCESSING_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensors
        );
    }

    public static Sensor recordLatenessSensor(final String threadId,
                                              final String taskId,
                                              final StreamsMetricsImpl streamsMetrics) {
        return avgAndMaxSensor(
            threadId,
            taskId,
            RECORD_LATENESS,
            RECORD_LATENESS_AVG_DESCRIPTION,
            RECORD_LATENESS_MAX_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor droppedRecordsSensor(final String threadId,
                                              final String taskId,
                                              final StreamsMetricsImpl streamsMetrics) {
        return invocationRateAndCountSensor(
            threadId,
            taskId,
            DROPPED_RECORDS,
            DROPPED_RECORDS_RATE_DESCRIPTION,
            DROPPED_RECORDS_TOTAL_DESCRIPTION,
            RecordingLevel.INFO,
            streamsMetrics
        );
    }

    private static Sensor invocationRateAndCountSensor(final String threadId,
                                                       final String taskId,
                                                       final String metricName,
                                                       final String descriptionOfRate,
                                                       final String descriptionOfCount,
                                                       final RecordingLevel recordingLevel,
                                                       final StreamsMetricsImpl streamsMetrics,
                                                       final Sensor... parentSensors) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, metricName, recordingLevel, parentSensors);
        addInvocationRateAndCountToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(threadId, taskId),
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }

    private static Sensor avgAndMaxSensor(final String threadId,
                                          final String taskId,
                                          final String metricName,
                                          final String descriptionOfAvg,
                                          final String descriptionOfMax,
                                          final RecordingLevel recordingLevel,
                                          final StreamsMetricsImpl streamsMetrics,
                                          final Sensor... parentSensors) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, metricName, recordingLevel, parentSensors);
        final Map<String, String> tagMap = streamsMetrics.taskLevelTagMap(threadId, taskId);
        addAvgAndMaxToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            tagMap,
            metricName,
            descriptionOfAvg,
            descriptionOfMax
        );
        return sensor;
    }

    private static Sensor invocationRateAndCountAndAvgAndMaxLatencySensor(final String threadId,
                                                                          final String taskId,
                                                                          final String metricName,
                                                                          final String descriptionOfRate,
                                                                          final String descriptionOfCount,
                                                                          final String descriptionOfAvg,
                                                                          final String descriptionOfMax,
                                                                          final RecordingLevel recordingLevel,
                                                                          final StreamsMetricsImpl streamsMetrics,
                                                                          final Sensor... parentSensors) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, metricName, recordingLevel, parentSensors);
        final Map<String, String> tagMap = streamsMetrics.taskLevelTagMap(threadId, taskId);
        addAvgAndMaxToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            tagMap,
            metricName + LATENCY_SUFFIX,
            descriptionOfAvg,
            descriptionOfMax
        );
        addInvocationRateAndCountToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            tagMap,
            metricName,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }
}
