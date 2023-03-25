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
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SinkUtilsTest {

    @Test
    public void testConsumerGroupOffsetsToConnectorOffsets() {
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = new HashMap<>();
        ConnectorOffsets connectorOffsets = SinkUtils.consumerGroupOffsetsToConnectorOffsets(consumerGroupOffsets);
        assertEquals(0, connectorOffsets.offsets().size());

        consumerGroupOffsets.put(new TopicPartition("test-topic", 0), new OffsetAndMetadata(100));

        connectorOffsets = SinkUtils.consumerGroupOffsetsToConnectorOffsets(consumerGroupOffsets);
        assertEquals(1, connectorOffsets.offsets().size());
        assertEquals(Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 100L), connectorOffsets.offsets().get(0).offset());

        Map<String, Object> expectedPartition = new HashMap<>();
        expectedPartition.put(SinkUtils.KAFKA_TOPIC_KEY, "test-topic");
        expectedPartition.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        assertEquals(expectedPartition, connectorOffsets.offsets().get(0).partition());
    }
}
