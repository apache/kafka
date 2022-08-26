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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetSyncTest {

    @Test
    public void testSerde() {
        OffsetSync offsetSync = new OffsetSync(new TopicPartition("topic-1", 2), 3, 4);
        byte[] key = offsetSync.recordKey();
        byte[] value = offsetSync.recordValue();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("any-topic", 6, 7, key, value);
        OffsetSync deserialized = OffsetSync.deserializeRecord(record);
        assertEquals(offsetSync.topicPartition(), deserialized.topicPartition(),
                "Failure on offset sync topic partition serde");
        assertEquals(offsetSync.upstreamOffset(), deserialized.upstreamOffset(),
                "Failure on upstream offset serde");
        assertEquals(offsetSync.downstreamOffset(), deserialized.downstreamOffset(),
                "Failure on downstream offset serde");
    }
}
