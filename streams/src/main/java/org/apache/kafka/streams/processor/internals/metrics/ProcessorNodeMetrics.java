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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;

public class ProcessorNodeMetrics {
    private ProcessorNodeMetrics() {}

    private static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    private static final String RATE_DESCRIPTION_SUFFIX = " per second";

    private static final String SUPPRESSION_EMIT = "suppression-emit";
    private static final String SUPPRESSION_EMIT_DESCRIPTION = "emitted records from the suppression buffer";
    private static final String SUPPRESSION_EMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SUPPRESSION_EMIT_DESCRIPTION;
    private static final String SUPPRESSION_EMIT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + SUPPRESSION_EMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    public static Sensor suppressionEmitSensor(final String threadId,
                                               final String taskId,
                                               final String processorNodeId,
                                               final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor =
            streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, SUPPRESSION_EMIT, RecordingLevel.DEBUG);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addInvocationRateAndCountToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            SUPPRESSION_EMIT,
            SUPPRESSION_EMIT_RATE_DESCRIPTION,
            SUPPRESSION_EMIT_TOTAL_DESCRIPTION
        );
        return sensor;
    }
}
