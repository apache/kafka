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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOPIC_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addTotalCountAndSumMetricsToSensor;

public class TopicMetrics {

    private static final String CONSUMED = "consumed";
    private static final String BYTES_CONSUMED = "bytes-consumed";
    private static final String BYTES_CONSUMED_DESCRIPTION = "bytes consumed from this topic";
    private static final String BYTES_CONSUMED_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + BYTES_CONSUMED_DESCRIPTION;
    private static final String RECORDS_CONSUMED = "records-consumed";
    private static final String RECORDS_CONSUMED_DESCRIPTION = "records consumed from this topic";
    private static final String RECORDS_CONSUMED_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + RECORDS_CONSUMED_DESCRIPTION;

    private static final String PRODUCED = "produced";
    private static final String BYTES_PRODUCED = "bytes-produced";
    private static final String BYTES_PRODUCED_DESCRIPTION = "bytes produced to this topic";
    private static final String BYTES_PRODUCED_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + BYTES_PRODUCED_DESCRIPTION;
    private static final String RECORDS_PRODUCED = "records-produced";
    private static final String RECORDS_PRODUCED_DESCRIPTION = "records produced to this topic";
    private static final String RECORDS_PRODUCED_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + RECORDS_PRODUCED_DESCRIPTION;

    public static Sensor consumedSensor(final String threadId,
                                        final String taskId,
                                        final String processorNodeId,
                                        final String topic,
                                        final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.topicLevelSensor(
            threadId,
            taskId,
            processorNodeId,
            topic,
            CONSUMED,
            RecordingLevel.INFO);
        addTotalCountAndSumMetricsToSensor(
            sensor,
            TOPIC_LEVEL_GROUP,
            streamsMetrics.topicLevelTagMap(threadId, taskId, processorNodeId, topic),
            RECORDS_CONSUMED,
            BYTES_CONSUMED,
            RECORDS_CONSUMED_TOTAL_DESCRIPTION,
            BYTES_CONSUMED_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor producedSensor(final String threadId,
                                        final String taskId,
                                        final String processorNodeId,
                                        final String topic,
                                        final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.topicLevelSensor(
            threadId,
            taskId,
            processorNodeId,
            topic,
            PRODUCED,
            RecordingLevel.INFO);
        addTotalCountAndSumMetricsToSensor(
            sensor,
            TOPIC_LEVEL_GROUP,
            streamsMetrics.topicLevelTagMap(threadId, taskId, processorNodeId, topic),
            RECORDS_PRODUCED,
            BYTES_PRODUCED,
            RECORDS_PRODUCED_TOTAL_DESCRIPTION,
            BYTES_PRODUCED_TOTAL_DESCRIPTION
        );
        return sensor;
    }

}
