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
}
