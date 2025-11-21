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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CheckpointTest {

    @Test
    public void testConstructorAndGetters() {
        TopicPartition tp = new TopicPartition("test-topic", 2);
        Checkpoint cp = new Checkpoint("group1", tp, 101L, 202L, "metaX");
        assertEquals("group1", cp.consumerGroupId());
        assertEquals(tp, cp.topicPartition());
        assertEquals(101L, cp.upstreamOffset());
        assertEquals(202L, cp.downstreamOffset());
        assertEquals("metaX", cp.metadata());
    }

    @Test
    public void testOffsetAndMetadata() {
        Checkpoint cp = new Checkpoint("group1", new TopicPartition("topic", 0), 1L, 999L, "info");
        OffsetAndMetadata om = cp.offsetAndMetadata();
        assertEquals(999L, om.offset());
        assertEquals("info", om.metadata());
    }

    @Test
    public void testConnectPartitionAndUnwrapGroup() {
        Checkpoint cp = new Checkpoint("group2", new TopicPartition("abc", 3), 1, 2, "zzz");
        Map<String, ?> partition = cp.connectPartition();
        assertEquals("group2", partition.get(Checkpoint.CONSUMER_GROUP_ID_KEY));
        assertEquals("abc", partition.get(Checkpoint.TOPIC_KEY));
        assertEquals(3, partition.get(Checkpoint.PARTITION_KEY));
        assertEquals("group2", Checkpoint.unwrapGroup(partition));
    }

    @Test
    public void testToString() {
        Checkpoint cp = new Checkpoint("g", new TopicPartition("t", 1), 11, 22, "m");
        String result = cp.toString();
        assertTrue(result.contains("g"));
        assertTrue(result.contains("t"));
        assertTrue(result.contains("11"));
        assertTrue(result.contains("22"));
        assertTrue(result.contains("m"));
    }

    @Test
    public void testSerializationRoundtrip() {
        TopicPartition tp = new TopicPartition("some-topic", 4);
        Checkpoint cp = new Checkpoint("cg1", tp, 11L, 22L, "meta");
        byte[] key = cp.recordKey();
        byte[] value = cp.recordValue();

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("checkpoints", 0, 0L, key, value);

        Checkpoint restored = Checkpoint.deserializeRecord(record);
        assertEquals(cp, restored);
    }

    @Test
    public void testEqualsAndHashCode() {
        Checkpoint a = new Checkpoint("g", new TopicPartition("t", 0), 3, 4, "d");
        Checkpoint b = new Checkpoint("g", new TopicPartition("t", 0), 3, 4, "d");
        Checkpoint c = new Checkpoint("g2", new TopicPartition("t", 0), 3, 4, "d");
        assertEquals(a, b);
        assertNotEquals(a, c);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
