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
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.connect.util.Callback;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointStoreTest {

    @Test
    public void testReadCheckpointsTopic() {
        Set<String> consumerGroups = Set.of("group1");

        MirrorCheckpointTaskConfig config = mock(MirrorCheckpointTaskConfig.class);
        when(config.checkpointsTopic()).thenReturn("checkpoint.topic");

        try (CheckpointStore store = new CheckpointStore(config, consumerGroups) {
            @Override
            void readCheckpointsImpl(MirrorCheckpointTaskConfig config, Callback<ConsumerRecord<byte[], byte[]>> consumedCallback) {
                consumedCallback.onCompletion(null, newCheckpointRecord("group1", "t1", 0, 0, 0));
                // this record must be ignored as not part of consumerGroups for task
                consumedCallback.onCompletion(null, newCheckpointRecord("group2", "t1", 0, 0, 0));
                // this record must be ignored as malformed
                consumedCallback.onCompletion(null,
                        new ConsumerRecord<>("checkpoint.topic", 0, 0L, new byte[0], new byte[0]));
                consumedCallback.onCompletion(null, newCheckpointRecord("group1", "t1", 0, 1, 1));
            }
        }) {
            assertFalse(store.isInitialized());

            assertTrue(store.start(), "expected start to return success");
            assertTrue(store.isInitialized());

            Map<String, Map<TopicPartition, Checkpoint>> expected = new HashMap<>();
            expected.put("group1", Map.of(new TopicPartition("t1", 0),
                    new Checkpoint("group1", new TopicPartition("t1", 0), 1, 1, "")));
            assertEquals(expected, store.checkpointsPerConsumerGroup);
        }
    }

    @Test
    public void testReadCheckpointsTopicError() {
        Set<String> consumerGroups = Set.of("group1");

        MirrorCheckpointTaskConfig config = mock(MirrorCheckpointTaskConfig.class);
        when(config.checkpointsTopic()).thenReturn("checkpoint.topic");

        try (CheckpointStore store = new CheckpointStore(config, consumerGroups) {
            @Override
            void readCheckpointsImpl(MirrorCheckpointTaskConfig config, Callback<ConsumerRecord<byte[], byte[]>> consumedCallback) {
                consumedCallback.onCompletion(null, newCheckpointRecord("group1", "topic", 1, 0, 0));
                consumedCallback.onCompletion(new TopicAuthorizationException("test"), null);
            }
        }) {

            assertFalse(store.start(), "expected start to return failure");
            assertTrue(store.isInitialized());
            assertTrue(store.checkpointsPerConsumerGroup.isEmpty());
        }
    }

    ConsumerRecord<byte[], byte[]> newCheckpointRecord(String gid, String topic, int partition, long upo, long dwo) {
        Checkpoint cp = new Checkpoint(gid, new TopicPartition(topic, partition), upo, dwo, "");
        return new ConsumerRecord<>("checkpoint.topic", 0, 0L, cp.recordKey(), cp.recordValue());
    }
}
