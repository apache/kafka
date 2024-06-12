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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SinkUtils {

    public static final String KAFKA_TOPIC_KEY = "kafka_topic";
    public static final String KAFKA_PARTITION_KEY = "kafka_partition";
    public static final String KAFKA_OFFSET_KEY = "kafka_offset";

    private SinkUtils() {}

    public static String consumerGroupId(String connector) {
        return "connect-" + connector;
    }

    public static ConnectorOffsets consumerGroupOffsetsToConnectorOffsets(Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets) {
        List<ConnectorOffset> connectorOffsets = new ArrayList<>();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> topicPartitionOffset : consumerGroupOffsets.entrySet()) {
            Map<String, Object> partition = new HashMap<>();
            partition.put(KAFKA_TOPIC_KEY, topicPartitionOffset.getKey().topic());
            partition.put(KAFKA_PARTITION_KEY, topicPartitionOffset.getKey().partition());
            connectorOffsets.add(new ConnectorOffset(partition,
                    Collections.singletonMap(KAFKA_OFFSET_KEY, topicPartitionOffset.getValue().offset())));
        }

        return new ConnectorOffsets(connectorOffsets);
    }

    /**
     * Ensure that the provided partitions (keys in the {@code partitionOffsets} map) look like:
     * <pre>
     *     {
     *       "kafka_topic": "topic"
     *       "kafka_partition": 3
     *     }
     * </pre>
     *
     * and that the provided offsets (values in the {@code partitionOffsets} map) look like:
     * <pre>
     *     {
     *       "kafka_offset": 1000
     *     }
     * </pre>
     *
     * and then parse them into a mapping from {@link TopicPartition}s to their corresponding {@link Long}
     * valued offsets.
     *
     * @param partitionOffsets the partitions to offset map that needs to be validated and parsed; may not be null or empty
     * @return the parsed mapping from {@link TopicPartition}s to their corresponding {@link Long} valued offsets; may not be null or empty
     *
     * @throws BadRequestException if the provided offsets aren't in the expected format
     */
    public static Map<TopicPartition, Long> parseSinkConnectorOffsets(Map<Map<String, ?>, Map<String, ?>> partitionOffsets) {
        Map<TopicPartition, Long> parsedOffsetMap = new HashMap<>();

        for (Map.Entry<Map<String, ?>, Map<String, ?>> partitionOffset : partitionOffsets.entrySet()) {
            Map<String, ?> partitionMap = partitionOffset.getKey();
            if (partitionMap == null) {
                throw new BadRequestException("The partition for a sink connector offset cannot be null or missing");
            }
            if (!partitionMap.containsKey(KAFKA_TOPIC_KEY) || !partitionMap.containsKey(KAFKA_PARTITION_KEY)) {
                throw new BadRequestException(String.format("The partition for a sink connector offset must contain the keys '%s' and '%s'",
                        KAFKA_TOPIC_KEY, KAFKA_PARTITION_KEY));
            }
            if (partitionMap.get(KAFKA_TOPIC_KEY) == null) {
                throw new BadRequestException("Kafka topic names must be valid strings and may not be null");
            }
            if (partitionMap.get(KAFKA_PARTITION_KEY) == null) {
                throw new BadRequestException("Kafka partitions must be valid numbers and may not be null");
            }
            String topic = String.valueOf(partitionMap.get(KAFKA_TOPIC_KEY));
            int partition;
            try {
                // We parse it this way because both "10" and 10 should be accepted as valid partition values in the REST API's
                // JSON request payload. If it throws an exception, we should propagate it since it's indicative of a badly formatted value.
                partition = Integer.parseInt(String.valueOf(partitionMap.get(KAFKA_PARTITION_KEY)));
            } catch (Exception e) {
                throw new BadRequestException("Failed to parse the following Kafka partition value in the provided offsets: '" +
                        partitionMap.get(KAFKA_PARTITION_KEY) + "'. Partition values for sink connectors need " +
                        "to be integers.", e);
            }
            TopicPartition tp = new TopicPartition(topic, partition);

            Map<String, ?> offsetMap = partitionOffset.getValue();

            if (offsetMap == null) {
                // represents an offset reset
                parsedOffsetMap.put(tp, null);
            } else {
                if (!offsetMap.containsKey(KAFKA_OFFSET_KEY)) {
                    throw new BadRequestException(String.format("The offset for a sink connector should either be null or contain " +
                            "the key '%s'", KAFKA_OFFSET_KEY));
                }
                long offset;
                try {
                    // We parse it this way because both "1000" and 1000 should be accepted as valid offset values in the REST API's
                    // JSON request payload. If it throws an exception, we should propagate it since it's indicative of a badly formatted value.
                    offset = Long.parseLong(String.valueOf(offsetMap.get(KAFKA_OFFSET_KEY)));
                } catch (Exception e) {
                    throw new BadRequestException("Failed to parse the following Kafka offset value in the provided offsets: '" +
                            offsetMap.get(KAFKA_OFFSET_KEY) + "'. Offset values for sink connectors need " +
                            "to be integers.", e);
                }
                parsedOffsetMap.put(tp, offset);
            }
        }

        return parsedOffsetMap;
    }
}
