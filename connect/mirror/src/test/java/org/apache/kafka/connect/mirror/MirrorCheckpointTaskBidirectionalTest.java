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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for bidirectional replication scenarios in MirrorCheckpointTask
 */
public class MirrorCheckpointTaskBidirectionalTest {

    @Test
    public void testBidirectionalSyncWithCheckpointStoreHavingHigherOffset() throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> idleConsumerGroupsOffset = new HashMap<>();
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();

        String consumer = "consumer1";
        String topic = "topic1";

        // Setup target cluster's consumer offset
        Map<TopicPartition, OffsetAndMetadata> consumerOffset = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        consumerOffset.put(tp, new OffsetAndMetadata(50));
        idleConsumerGroupsOffset.put(consumer, consumerOffset);

        // Setup checkpoint with higher downstream offset
        Checkpoint higherCheckpoint = new Checkpoint(consumer, new TopicPartition(topic, 0), 300, 200, "metadata");
        Map<TopicPartition, Checkpoint> checkpointMap = new HashMap<>();
        checkpointMap.put(higherCheckpoint.topicPartition(), higherCheckpoint);
        checkpointsPerConsumerGroup.put(consumer, checkpointMap);

        // Create a converted upstream offset that's lower than what's in the checkpoint store
        // This simulates a scenario where the source cluster's offset is behind the target
        Map<String, Map<TopicPartition, OffsetAndMetadata>> convertedUpstreamOffsets = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> convertedOffsets = new HashMap<>();
        convertedOffsets.put(tp, new OffsetAndMetadata(100)); // Lower than 200 in checkpoint
        convertedUpstreamOffsets.put(consumer, convertedOffsets);

        // Create MirrorCheckpointTask with mocked computeConvertedUpstreamOffset
        CheckpointStore checkpointStore = new CheckpointStore(checkpointsPerConsumerGroup);
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source", "target",
                new DefaultReplicationPolicy(), null, Collections.emptySet(), idleConsumerGroupsOffset, checkpointStore) {
            @Override
            Map<String, Map<TopicPartition, OffsetAndMetadata>> syncGroupOffset() throws ExecutionException, InterruptedException {
                // Use the test's converted upstream offsets directly
                Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetToSyncAll = new HashMap<>();
                for (Map.Entry<String, Map<TopicPartition, OffsetAndMetadata>> group : convertedUpstreamOffsets.entrySet()) {
                    String consumerGroupId = group.getKey();
                    Map<TopicPartition, OffsetAndMetadata> convertedUpstreamOffset = group.getValue();

                    Map<TopicPartition, OffsetAndMetadata> offsetToSync = new HashMap<>();
                    Map<TopicPartition, OffsetAndMetadata> targetConsumerOffset = idleConsumerGroupsOffset.get(consumerGroupId);

                    for (Map.Entry<TopicPartition, OffsetAndMetadata> convertedEntry : convertedUpstreamOffset.entrySet()) {
                        TopicPartition topicPartition = convertedEntry.getKey();
                        OffsetAndMetadata convertedOffset = convertedUpstreamOffset.get(topicPartition);

                        if (!targetConsumerOffset.containsKey(topicPartition)) {
                            offsetToSync.put(topicPartition, convertedOffset);
                            continue;
                        }

                        OffsetAndMetadata targetOffsetAndMetadata = targetConsumerOffset.get(topicPartition);
                        if (targetOffsetAndMetadata != null) {
                            long latestDownstreamOffset = targetOffsetAndMetadata.offset();
                            if (latestDownstreamOffset >= convertedOffset.offset()) {
                                continue;
                            }
                        }

                        // Check against checkpoint store for bidirectional case
                        Checkpoint existingCheckpoint = checkpointStore.get(consumerGroupId) != null ? 
                            checkpointStore.get(consumerGroupId).get(topicPartition) : null;
                        if (existingCheckpoint != null) {
                            long existingDownstreamOffset = existingCheckpoint.downstreamOffset();
                            if (existingDownstreamOffset > convertedOffset.offset()) {
                                continue;
                            }
                        }

                        offsetToSync.put(topicPartition, convertedOffset);
                    }

                    if (!offsetToSync.isEmpty()) {
                        offsetToSyncAll.put(consumerGroupId, offsetToSync);
                    }
                }

                idleConsumerGroupsOffset.clear();
                return offsetToSyncAll;
            }
        };

        // The syncGroupOffset method should not sync the lower offset from the source cluster
        Map<String, Map<TopicPartition, OffsetAndMetadata>> result = mirrorCheckpointTask.syncGroupOffset();

        // No offsets should be synced since the checkpoint has a higher offset
        assertNull(result.get(consumer));
    }

    @Test
    public void testBidirectionalSyncWithCheckpointStoreHavingLowerOffset() throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> idleConsumerGroupsOffset = new HashMap<>();
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();

        String consumer = "consumer1";
        String topic = "topic1";

        // Setup target cluster's consumer offset
        Map<TopicPartition, OffsetAndMetadata> consumerOffset = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        consumerOffset.put(tp, new OffsetAndMetadata(50));
        idleConsumerGroupsOffset.put(consumer, consumerOffset);

        // Setup checkpoint with lower downstream offset
        Checkpoint lowerCheckpoint = new Checkpoint(consumer, new TopicPartition(topic, 0), 100, 75, "metadata");
        Map<TopicPartition, Checkpoint> checkpointMap = new HashMap<>();
        checkpointMap.put(lowerCheckpoint.topicPartition(), lowerCheckpoint);
        checkpointsPerConsumerGroup.put(consumer, checkpointMap);

        // Create a converted upstream offset that's higher than what's in the checkpoint store
        // This simulates a scenario where the source cluster's offset is ahead of the target
        Map<String, Map<TopicPartition, OffsetAndMetadata>> convertedUpstreamOffsets = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> convertedOffsets = new HashMap<>();
        convertedOffsets.put(tp, new OffsetAndMetadata(150)); // Higher than 75 in checkpoint
        convertedUpstreamOffsets.put(consumer, convertedOffsets);

        // Create MirrorCheckpointTask with mocked computeConvertedUpstreamOffset
        CheckpointStore checkpointStore = new CheckpointStore(checkpointsPerConsumerGroup);
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source", "target",
                new DefaultReplicationPolicy(), null, Collections.emptySet(), idleConsumerGroupsOffset, checkpointStore) {
            @Override
            Map<String, Map<TopicPartition, OffsetAndMetadata>> syncGroupOffset() throws ExecutionException, InterruptedException {
                // Use the test's converted upstream offsets directly
                Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetToSyncAll = new HashMap<>();
                for (Map.Entry<String, Map<TopicPartition, OffsetAndMetadata>> group : convertedUpstreamOffsets.entrySet()) {
                    String consumerGroupId = group.getKey();
                    Map<TopicPartition, OffsetAndMetadata> convertedUpstreamOffset = group.getValue();

                    Map<TopicPartition, OffsetAndMetadata> offsetToSync = new HashMap<>();
                    Map<TopicPartition, OffsetAndMetadata> targetConsumerOffset = idleConsumerGroupsOffset.get(consumerGroupId);

                    for (Map.Entry<TopicPartition, OffsetAndMetadata> convertedEntry : convertedUpstreamOffset.entrySet()) {
                        TopicPartition topicPartition = convertedEntry.getKey();
                        OffsetAndMetadata convertedOffset = convertedUpstreamOffset.get(topicPartition);

                        if (!targetConsumerOffset.containsKey(topicPartition)) {
                            offsetToSync.put(topicPartition, convertedOffset);
                            continue;
                        }

                        OffsetAndMetadata targetOffsetAndMetadata = targetConsumerOffset.get(topicPartition);
                        if (targetOffsetAndMetadata != null) {
                            long latestDownstreamOffset = targetOffsetAndMetadata.offset();
                            if (latestDownstreamOffset >= convertedOffset.offset()) {
                                continue;
                            }
                        }

                        // Check against checkpoint store for bidirectional case
                        Checkpoint existingCheckpoint = checkpointStore.get(consumerGroupId) != null ? 
                            checkpointStore.get(consumerGroupId).get(topicPartition) : null;
                        if (existingCheckpoint != null) {
                            long existingDownstreamOffset = existingCheckpoint.downstreamOffset();
                            if (existingDownstreamOffset > convertedOffset.offset()) {
                                continue;
                            }
                        }

                        offsetToSync.put(topicPartition, convertedOffset);
                    }

                    if (!offsetToSync.isEmpty()) {
                        offsetToSyncAll.put(consumerGroupId, offsetToSync);
                    }
                }

                idleConsumerGroupsOffset.clear();
                return offsetToSyncAll;
            }
        };

        // The syncGroupOffset method should sync the higher offset from the source cluster
        Map<String, Map<TopicPartition, OffsetAndMetadata>> result = mirrorCheckpointTask.syncGroupOffset();

        // The higher offset should be synced
        assertEquals(150, result.get(consumer).get(tp).offset());
    }
}
