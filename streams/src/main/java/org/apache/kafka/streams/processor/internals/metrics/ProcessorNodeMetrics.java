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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.AVG_LATENCY_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MAX_LATENCY_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_AVG_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_MAX_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_MIN_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMinAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addRateOfSumAndSumMetricsToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;

public class ProcessorNodeMetrics {
    private ProcessorNodeMetrics() {}

    private static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    private static final String RATE_DESCRIPTION_SUFFIX = " per second";

    private static final String SUPPRESSION_EMIT = "suppression-emit";
    private static final String SUPPRESSION_EMIT_DESCRIPTION = "emitted records from the suppression buffer";
    private static final String SUPPRESSION_EMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SUPPRESSION_EMIT_DESCRIPTION;
    private static final String SUPPRESSION_EMIT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + SUPPRESSION_EMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String IDEMPOTENT_UPDATE_SKIP = "idempotent-update-skip";
    private static final String IDEMPOTENT_UPDATE_SKIP_DESCRIPTION = "skipped idempotent updates";
    private static final String IDEMPOTENT_UPDATE_SKIP_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + IDEMPOTENT_UPDATE_SKIP_DESCRIPTION;
    private static final String IDEMPOTENT_UPDATE_SKIP_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + IDEMPOTENT_UPDATE_SKIP_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String PROCESS = "process";
    private static final String PROCESS_DESCRIPTION = "calls to process";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String FORWARD = "forward";
    private static final String FORWARD_DESCRIPTION = "calls to forward";
    private static final String FORWARD_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + FORWARD_DESCRIPTION;
    private static final String FORWARD_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + FORWARD_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String EMITTED_RECORDS = "window-aggregate-final-emit";
    private static final String EMITTED_RECORDS_DESCRIPTION = "emit final records";
    private static final String EMITTED_RECORDS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + EMITTED_RECORDS_DESCRIPTION;
    private static final String EMITTED_RECORDS_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + EMITTED_RECORDS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String EMIT_FINAL_LATENCY = EMITTED_RECORDS + LATENCY_SUFFIX;
    private static final String EMIT_FINAL_DESCRIPTION = "calls to emit final";
    private static final String EMIT_FINAL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION + EMIT_FINAL_DESCRIPTION;
    private static final String EMIT_FINAL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION + EMIT_FINAL_DESCRIPTION;

    public static Sensor suppressionEmitSensor(final String threadId,
                                               final String taskId,
                                               final String processorNodeId,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            SUPPRESSION_EMIT,
            SUPPRESSION_EMIT_RATE_DESCRIPTION,
            SUPPRESSION_EMIT_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor skippedIdempotentUpdatesSensor(final String threadId,
            final String taskId,
            final String processorNodeId,
            final StreamsMetricsImpl streamsMetrics) {
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            IDEMPOTENT_UPDATE_SKIP,
            IDEMPOTENT_UPDATE_SKIP_RATE_DESCRIPTION,
            IDEMPOTENT_UPDATE_SKIP_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor processAtSourceSensor(final String threadId,
                                               final String taskId,
                                               final String processorNodeId,
                                               final StreamsMetricsImpl streamsMetrics) {
        final Sensor parentSensor = streamsMetrics.taskLevelSensor(threadId, taskId, PROCESS, RecordingLevel.DEBUG);
        addInvocationRateAndCountToSensor(
            parentSensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(threadId, taskId),
            PROCESS,
            PROCESS_RATE_DESCRIPTION,
            PROCESS_TOTAL_DESCRIPTION
        );
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            PROCESS,
            PROCESS_RATE_DESCRIPTION,
            PROCESS_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensor
        );
    }

    public static Sensor forwardSensor(final String threadId,
                                       final String taskId,
                                       final String processorNodeId,
                                       final StreamsMetricsImpl streamsMetrics) {
        final Sensor parentSensor = throughputParentSensor(
            threadId,
            taskId,
            FORWARD,
            FORWARD_RATE_DESCRIPTION,
            FORWARD_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            FORWARD,
            FORWARD_RATE_DESCRIPTION,
            FORWARD_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensor
        );
    }

    public static Sensor e2ELatencySensor(final String threadId,
                                          final String taskId,
                                          final String processorNodeId,
                                          final StreamsMetricsImpl streamsMetrics) {
        final String sensorSuffix = processorNodeId + "-" + RECORD_E2E_LATENCY;
        final Sensor sensor = streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, sensorSuffix, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addAvgAndMinAndMaxToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            RECORD_E2E_LATENCY,
            RECORD_E2E_LATENCY_AVG_DESCRIPTION,
            RECORD_E2E_LATENCY_MIN_DESCRIPTION,
            RECORD_E2E_LATENCY_MAX_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor emitFinalLatencySensor(final String threadId,
                                                final String taskId,
                                                final String processorNodeId,
                                                final StreamsMetricsImpl streamsMetrics) {
        final String sensorSuffix = processorNodeId + "-" + EMIT_FINAL_LATENCY;
        final Sensor sensor = streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, sensorSuffix, RecordingLevel.DEBUG);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addAvgAndMaxToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            EMIT_FINAL_LATENCY,
            EMIT_FINAL_AVG_LATENCY_DESCRIPTION,
            EMIT_FINAL_MAX_LATENCY_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor emittedRecordsSensor(final String threadId,
                                              final String taskId,
                                              final String processorNodeId,
                                              final StreamsMetricsImpl streamsMetrics) {
        final String sensorSuffix = processorNodeId + "-" + EMITTED_RECORDS;
        final Sensor sensor = streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, sensorSuffix, RecordingLevel.DEBUG);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addRateOfSumAndSumMetricsToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            EMITTED_RECORDS,
            EMITTED_RECORDS_RATE_DESCRIPTION,
            EMITTED_RECORDS_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    private static Sensor throughputParentSensor(final String threadId,
                                                 final String taskId,
                                                 final String operation,
                                                 final String descriptionOfRate,
                                                 final String descriptionOfCount,
                                                 final RecordingLevel recordingLevel,
                                                 final StreamsMetricsImpl streamsMetrics) {
        // use operation name as sensor suffix and metric prefix
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, operation, recordingLevel);
        final Map<String, String> parentTagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, ROLLUP_VALUE);
        addInvocationRateAndCountToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            parentTagMap,
            operation,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }

    private static Sensor throughputSensor(final String threadId,
                                           final String taskId,
                                           final String processorNodeId,
                                           final String operationName,
                                           final String descriptionOfRate,
                                           final String descriptionOfCount,
                                           final RecordingLevel recordingLevel,
                                           final StreamsMetricsImpl streamsMetrics,
                                           final Sensor... parentSensors) {
        // use operation name as sensor suffix and metric name prefix
        final Sensor sensor =
            streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, operationName, recordingLevel, parentSensors);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addInvocationRateAndCountToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            operationName,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }


}
