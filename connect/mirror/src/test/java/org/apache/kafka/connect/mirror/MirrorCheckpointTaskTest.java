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
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MirrorCheckpointTaskTest {

    @Test
    public void testDownstreamTopicRenaming() {
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), null, Collections.emptySet(), Collections.emptyMap(),
            new CheckpointStore(Collections.emptyMap()));
        assertEquals(new TopicPartition("source1.topic3", 4),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("topic3", 4)),
                "Renaming source1.topic3 failed");
        assertEquals(new TopicPartition("source1.source6.topic7", 8),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("source6.topic7", 8)),
                "Renaming source1.source6.topic7 failed");
    }

    @Test
    public void testCheckpoint() {
        long t1UpstreamOffset = 3L;
        long t1DownstreamOffset = 4L;
        long t2UpstreamOffset = 7L;
        long t2DownstreamOffset = 8L;
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start(true);
        OffsetSyncStoreTest.FakeOffsetSyncStore reverseOffsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore(true);
        reverseOffsetSyncStore.start(true);
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), offsetSyncStore, reverseOffsetSyncStore, Collections.emptySet(),
            Collections.emptyMap(), new CheckpointStore(Collections.emptyMap()));
        offsetSyncStore.sync(new TopicPartition("topic1", 2), t1UpstreamOffset, t1DownstreamOffset);
        reverseOffsetSyncStore.sync(new TopicPartition("topic1", 2), t2UpstreamOffset, t2DownstreamOffset);
        Optional<Checkpoint> optionalCheckpoint1 = mirrorCheckpointTask.checkpoint("group9", new TopicPartition("topic1", 2),
            new OffsetAndMetadata(10, null));
        assertTrue(optionalCheckpoint1.isPresent());
        Checkpoint checkpoint1 = optionalCheckpoint1.get();
        SourceRecord sourceRecord1 = mirrorCheckpointTask.checkpointRecord(checkpoint1, 123L);
        assertEquals(new TopicPartition("source1.topic1", 2), checkpoint1.topicPartition(),
                "checkpoint group9 source1.topic1 failed");
        assertEquals("group9", checkpoint1.consumerGroupId(),
                "checkpoint group9 consumerGroupId failed");
        assertEquals("group9", Checkpoint.unwrapGroup(sourceRecord1.sourcePartition()),
                "checkpoint group9 sourcePartition failed");
        assertEquals(10, checkpoint1.upstreamOffset(),
                "checkpoint group9 upstreamOffset failed");
        assertEquals(t1DownstreamOffset + 1, checkpoint1.downstreamOffset(),
                "checkpoint group9 downstreamOffset failed");
        assertEquals(123L, sourceRecord1.timestamp().longValue(),
                "checkpoint group9 timestamp failed");

        Optional<Checkpoint> optionalCheckpoint2 = mirrorCheckpointTask.reverseCheckpoint("group9",
                new TopicPartition("target2.topic1", 2), new OffsetAndMetadata(10, null));
        assertTrue(optionalCheckpoint2.isPresent());
        Checkpoint checkpoint2 = optionalCheckpoint2.get();
        SourceRecord sourceRecord2 = mirrorCheckpointTask.checkpointRecord(checkpoint2, 123L);
        assertEquals(new TopicPartition("topic1", 2), checkpoint2.topicPartition(),
                "checkpoint group9 source1.topic1 failed");
        assertEquals("group9", checkpoint2.consumerGroupId(),
                "checkpoint group9 consumerGroupId failed");
        assertEquals("group9", Checkpoint.unwrapGroup(sourceRecord2.sourcePartition()),
                "checkpoint group9 sourcePartition failed");
        assertEquals(10, checkpoint2.upstreamOffset(),
                "checkpoint group9 upstreamOffset failed");
        assertEquals(t2UpstreamOffset + 1, checkpoint2.downstreamOffset(),
                "checkpoint group9 downstreamOffset failed");
        assertEquals(123L, sourceRecord2.timestamp().longValue(),
                "checkpoint group9 timestamp failed");
    }

    @Test
    public void testSyncOffset() throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> idleConsumerGroupsOffset = new HashMap<>();
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();

        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        String topic1 = "topic1";
        String topic2 = "topic2";

        // 'c1t1' denotes consumer offsets of all partitions of topic1 for consumer1
        Map<TopicPartition, OffsetAndMetadata> c1t1 = new HashMap<>();
        // 't1p0' denotes topic1, partition 0
        TopicPartition t1p0 = new TopicPartition(topic1, 0);

        c1t1.put(t1p0, new OffsetAndMetadata(100));

        Map<TopicPartition, OffsetAndMetadata> c2t2 = new HashMap<>();
        TopicPartition t2p0 = new TopicPartition(topic2, 0);

        c2t2.put(t2p0, new OffsetAndMetadata(50));

        idleConsumerGroupsOffset.put(consumer1, c1t1);
        idleConsumerGroupsOffset.put(consumer2, c2t2);

        // 'cpC1T1P0' denotes 'checkpoint' of topic1, partition 0 for consumer1
        Checkpoint cpC1T1P0 = new Checkpoint(consumer1, new TopicPartition(topic1, 0), 200, 101, "metadata");

        // 'cpC2T2p0' denotes 'checkpoint' of topic2, partition 0 for consumer2
        Checkpoint cpC2T2P0 = new Checkpoint(consumer2, new TopicPartition(topic2, 0), 100, 51, "metadata");

        // 'checkpointMapC1' denotes 'checkpoint' map for consumer1
        Map<TopicPartition, Checkpoint> checkpointMapC1 = new HashMap<>();
        checkpointMapC1.put(cpC1T1P0.topicPartition(), cpC1T1P0);

        // 'checkpointMapC2' denotes 'checkpoint' map for consumer2
        Map<TopicPartition, Checkpoint> checkpointMapC2 = new HashMap<>();
        checkpointMapC2.put(cpC2T2P0.topicPartition(), cpC2T2P0);

        checkpointsPerConsumerGroup.put(consumer1, checkpointMapC1);
        checkpointsPerConsumerGroup.put(consumer2, checkpointMapC2);

        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), null, Collections.emptySet(), idleConsumerGroupsOffset,
            new CheckpointStore(checkpointsPerConsumerGroup));

        Map<String, Map<TopicPartition, OffsetAndMetadata>> output = mirrorCheckpointTask.syncGroupOffset();

        assertEquals(101, output.get(consumer1).get(t1p0).offset(),
                "Consumer 1 " + topic1 + " failed");
        assertEquals(51, output.get(consumer2).get(t2p0).offset(),
                "Consumer 2 " + topic2 + " failed");
    }

    @Test
    public void testSyncOffsetForTargetGroupWithNullOffsetAndMetadata() throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> idleConsumerGroupsOffset = new HashMap<>();
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();

        String consumer = "consumer";
        String topic = "topic";
        Map<TopicPartition, OffsetAndMetadata> ct = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        // Simulate other clients such as Sarama, which may reset group offsets to -1. This can cause
        // the obtained `OffsetAndMetadata` of the target cluster to be null.
        ct.put(tp, null);
        idleConsumerGroupsOffset.put(consumer, ct);

        Checkpoint cp = new Checkpoint(consumer, new TopicPartition(topic, 0), 200, 101, "metadata");
        Map<TopicPartition, Checkpoint> checkpointMap = new HashMap<>();
        checkpointMap.put(cp.topicPartition(), cp);
        checkpointsPerConsumerGroup.put(consumer, checkpointMap);

        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source", "target",
                new DefaultReplicationPolicy(), null, Collections.emptySet(), idleConsumerGroupsOffset,
                new CheckpointStore(checkpointsPerConsumerGroup));

        Map<String, Map<TopicPartition, OffsetAndMetadata>> output = mirrorCheckpointTask.syncGroupOffset();

        assertEquals(101, output.get(consumer).get(tp).offset(), "Consumer " + topic + " failed");
    }

    @Test
    public void testNoCheckpointForTopicWithoutOffsetSyncs() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start(true);
        OffsetSyncStoreTest.FakeOffsetSyncStore reverseOffsetSyncStore =
                new OffsetSyncStoreTest.FakeOffsetSyncStore(true);
        reverseOffsetSyncStore.start(true);
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(), offsetSyncStore, reverseOffsetSyncStore, Collections.emptySet(),
                Collections.emptyMap(), new CheckpointStore(Collections.emptyMap()));
        offsetSyncStore.sync(new TopicPartition("topic1", 0), 3L, 4L);
        reverseOffsetSyncStore.sync(new TopicPartition("topic1", 0), 3L, 4L);

        Optional<Checkpoint> checkpoint1 = mirrorCheckpointTask.checkpoint("group9", new TopicPartition("topic1", 1),
                new OffsetAndMetadata(10, null));
        Optional<Checkpoint> checkpoint2 = mirrorCheckpointTask.checkpoint("group9", new TopicPartition("topic1", 0),
                new OffsetAndMetadata(10, null));
        assertFalse(checkpoint1.isPresent());
        assertTrue(checkpoint2.isPresent());

        Optional<Checkpoint> checkpoint3 = mirrorCheckpointTask.reverseCheckpoint("group9",
                new TopicPartition("source1.topic1", 1), new OffsetAndMetadata(10, null));
        Optional<Checkpoint> checkpoint4 = mirrorCheckpointTask.reverseCheckpoint("group9",
                new TopicPartition("source1.topic1", 0), new OffsetAndMetadata(10, null));
        assertFalse(checkpoint3.isPresent());
        assertTrue(checkpoint4.isPresent());
    }

    @Test
    public void testNoCheckpointForTopicWithNullOffsetAndMetadata() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start(true);
        OffsetSyncStoreTest.FakeOffsetSyncStore reverseOffsetSyncStore =
                new OffsetSyncStoreTest.FakeOffsetSyncStore(true);
        reverseOffsetSyncStore.start(true);
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), offsetSyncStore, Collections.emptySet(), Collections.emptyMap(),
            new CheckpointStore(Collections.emptyMap()));
        offsetSyncStore.sync(new TopicPartition("topic1", 0), 1L, 3L);
        reverseOffsetSyncStore.sync(new TopicPartition("topic1", 0), 1L, 3L);
        Optional<Checkpoint> checkpoint = mirrorCheckpointTask.checkpoint("g1", new TopicPartition("topic1", 0), null);
        assertFalse(checkpoint.isPresent());
        Optional<Checkpoint> checkpoint2 = mirrorCheckpointTask.reverseCheckpoint("g1", new TopicPartition("source1.topic1", 0), null);
        assertFalse(checkpoint2.isPresent());
    }

    @Test
    public void testCheckpointRecordsMonotonicIfStoreRewinds() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start(true);
        OffsetSyncStoreTest.FakeOffsetSyncStore reverseOffsetSyncStore =
                new OffsetSyncStoreTest.FakeOffsetSyncStore(true);
        reverseOffsetSyncStore.start(true);
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(), offsetSyncStore, reverseOffsetSyncStore, Collections.emptySet(),
                Collections.emptyMap(), new CheckpointStore(checkpointsPerConsumerGroup));
        TopicPartition tp = new TopicPartition("topic1", 0);
        TopicPartition targetTP = new TopicPartition("source1.topic1", 0);
        TopicPartition replicaTP = new TopicPartition("target2.topic1", 0);

        long upstream = 11L;
        long downstream = 4L;
        // Emit syncs 0 and 1, and use the sync 1 to translate offsets and commit checkpoints
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        long consumerGroupOffset = upstream;
        long expectedDownstreamOffset = downstream;
        assertEquals(OptionalLong.of(expectedDownstreamOffset), offsetSyncStore.translate("g1", tp, consumerGroupOffset));

        long upstream2 = 4L;
        long downstream2 = 11L;
        // Emit syncs 0 and 1, and use the sync 1 to translate offsets and commit checkpoints
        reverseOffsetSyncStore.sync(tp, upstream2++, downstream2++);
        reverseOffsetSyncStore.sync(tp, upstream2++, downstream2++);
        long consumerGroupOffset2 = downstream2;
        long expectedDownstreamOffset2 = upstream2;
        assertEquals(OptionalLong.of(upstream2), reverseOffsetSyncStore.translate("g1", tp, consumerGroupOffset2));

        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = new HashMap<>();
        consumerGroupOffsets.put(tp, new OffsetAndMetadata(consumerGroupOffset));
        consumerGroupOffsets.put(replicaTP, new OffsetAndMetadata(consumerGroupOffset2));
        List<TopicPartition> topicPartitions = Arrays.asList(tp, targetTP);

        Map<TopicPartition, Checkpoint> checkpoints = assertCheckpointForTopic(mirrorCheckpointTask,
                consumerGroupOffsets, topicPartitions, true);

        // the task normally does this, but simulate it here
        checkpointsPerConsumerGroup.put("g1", checkpoints);

        // Emit syncs 2-6 which will cause the store to drop sync 1, forcing translation to fall back to 0.
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream, downstream);
        reverseOffsetSyncStore.sync(tp, upstream2++, downstream2++);
        reverseOffsetSyncStore.sync(tp, upstream2++, downstream2++);
        reverseOffsetSyncStore.sync(tp, upstream2++, downstream2++);
        reverseOffsetSyncStore.sync(tp, upstream2++, downstream2++);
        reverseOffsetSyncStore.sync(tp, upstream2, downstream2);
        // The OffsetSyncStore will change its translation of the same offset
        assertNotEquals(OptionalLong.of(expectedDownstreamOffset), offsetSyncStore.translate("g1", tp, consumerGroupOffset));
        assertNotEquals(OptionalLong.of(expectedDownstreamOffset2), reverseOffsetSyncStore.translate("g1", tp, consumerGroupOffset2));
        // But the task will filter this out and not emit a checkpoint
        assertCheckpointForTopic(mirrorCheckpointTask, consumerGroupOffsets, topicPartitions, false);

        // If then the upstream offset rewinds in the topic and is still translatable, a checkpoint will be emitted
        // also rewinding the downstream offsets to match. This will not affect auto-synced groups, only checkpoints.
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets2 = new HashMap<>();
        consumerGroupOffsets2.put(tp, new OffsetAndMetadata(consumerGroupOffset - 1));
        consumerGroupOffsets2.put(replicaTP, new OffsetAndMetadata(consumerGroupOffset2 - 1));
        assertCheckpointForTopic(mirrorCheckpointTask, consumerGroupOffsets2, topicPartitions, true);
    }

    private Map<TopicPartition, Checkpoint> assertCheckpointForTopic(
            MirrorCheckpointTask task, TopicPartition tp, TopicPartition remoteTp, long consumerGroupOffset, boolean truth
    ) {
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = Collections.singletonMap(tp, new OffsetAndMetadata(consumerGroupOffset));
        return assertCheckpointForTopic(task, consumerGroupOffsets, Collections.singleton(remoteTp), truth);
    }

    private Map<TopicPartition, Checkpoint> assertCheckpointForTopic(
            MirrorCheckpointTask task, Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets,
            Collection<TopicPartition> tps, boolean truth
    ) {
        Map<TopicPartition, Checkpoint> checkpoints = task.checkpointsForGroup(consumerGroupOffsets, "g1");
        if (truth) {
            assertEquals(new HashSet<>(tps), checkpoints.keySet(), "should emit offset sync");
        } else {
            assertEquals(Collections.emptySet(), checkpoints.keySet(), "should not emit offset sync");
        }
        return checkpoints;
    }

    @Test
    public void testCheckpointsTaskRestartUsesExistingCheckpoints() {
        TopicPartition t1p0 = new TopicPartition("t1", 0);
        TopicPartition sourceT1p0 = new TopicPartition("source1.t1", 0);
        TopicPartition targetT1p0 = new TopicPartition("target2.t1", 0);
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore() {
            @Override
            void backingStoreStart() {
                // OffsetSyncStore contains entries for: 100->100, 200->200, 300->300
                for (int i = 100; i <= 300; i += 100) {
                    sync(t1p0, i, i);
                }
            }
        };
        offsetSyncStore.start(false);
        OffsetSyncStoreTest.FakeOffsetSyncStore reverseOffsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore(true) {
            @Override
            void backingStoreStart() {
                // OffsetSyncStore contains entries for: 100->100, 200->200, 300->300
                for (int i = 100; i <= 300; i += 100) {
                    sync(t1p0, i, i);
                }
            }
        };
        reverseOffsetSyncStore.start(false);

        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(), offsetSyncStore, reverseOffsetSyncStore, Collections.emptySet(),
                Collections.emptyMap(), new CheckpointStore(Collections.emptyMap()));

        // Generate a checkpoint for upstream offset 250, and assert it maps to downstream 201
        // (as nearest mapping in OffsetSyncStore is 200->200)
        Map<TopicPartition, OffsetAndMetadata> upstreamGroupOffsets = new HashMap<>();
        upstreamGroupOffsets.put(t1p0, new OffsetAndMetadata(250));
        upstreamGroupOffsets.put(targetT1p0, new OffsetAndMetadata(250));
        Map<TopicPartition, Checkpoint> checkpoints = mirrorCheckpointTask.checkpointsForGroup(upstreamGroupOffsets, "group1");
        assertEquals(2, checkpoints.size());
        assertEquals(new Checkpoint("group1", sourceT1p0, 250, 201, ""), checkpoints.get(sourceT1p0));
        assertEquals(new Checkpoint("group1", t1p0, 250, 201, ""), checkpoints.get(t1p0));

        // Simulate task restart, during which more offsets are added to the sync topic, and thus the
        // corresponding OffsetSyncStore no longer has a mapping for 100->100
        // Now OffsetSyncStore contains entries for: 175->175, 375->375, 475->475
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore2 = new OffsetSyncStoreTest.FakeOffsetSyncStore() {
            @Override
            void backingStoreStart() {
                for (int i = 175; i <= 475; i += 100) {
                    sync(t1p0, i, i);
                }
            }
        };
        offsetSyncStore2.start(false);
        OffsetSyncStoreTest.FakeOffsetSyncStore reverseOffsetSyncStore2 = new OffsetSyncStoreTest.FakeOffsetSyncStore(true) {
            @Override
            void backingStoreStart() {
                for (int i = 175; i <= 475; i += 100) {
                    sync(t1p0, i, i);
                }
            }
        };
        reverseOffsetSyncStore2.start(false);

        // Simulate loading existing checkpoints into checkpointsPerConsumerGroup (250->201)
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();
        checkpointsPerConsumerGroup.put("group1", checkpoints);
        MirrorCheckpointTask mirrorCheckpointTask2 = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(), offsetSyncStore2, reverseOffsetSyncStore2, Collections.emptySet(),
                Collections.emptyMap(), new CheckpointStore(checkpointsPerConsumerGroup));

        // Upstream offsets 250 and 370 now have the closest downstream value of 176, but this is
        // earlier than the downstream value of the last checkpoint (201) - so they are not emitted.
        assertEquals(OptionalLong.of(176), offsetSyncStore2.translate(null, t1p0, 250));
        assertEquals(OptionalLong.of(176), offsetSyncStore2.translate(null, t1p0, 370));
        assertEquals(OptionalLong.of(176), reverseOffsetSyncStore2.translate(null, t1p0, 250));
        assertEquals(OptionalLong.of(176), reverseOffsetSyncStore2.translate(null, t1p0, 370));
        upstreamGroupOffsets.put(t1p0, new OffsetAndMetadata(250));
        upstreamGroupOffsets.put(targetT1p0, new OffsetAndMetadata(250));
        assertTrue(mirrorCheckpointTask2.checkpointsForGroup(upstreamGroupOffsets, "group1").isEmpty());
        upstreamGroupOffsets.put(t1p0, new OffsetAndMetadata(370));
        upstreamGroupOffsets.put(targetT1p0, new OffsetAndMetadata(370));
        assertTrue(mirrorCheckpointTask2.checkpointsForGroup(upstreamGroupOffsets, "group1").isEmpty());

        // Upstream offset 400 has a closest downstream value of 376, and is emitted because it has
        // a later downstream offset than the last checkpoint's downstream (201)
        upstreamGroupOffsets.put(t1p0, new OffsetAndMetadata(400));
        upstreamGroupOffsets.put(targetT1p0, new OffsetAndMetadata(400));
        Map<TopicPartition, Checkpoint> checkpoints2 = mirrorCheckpointTask2.checkpointsForGroup(upstreamGroupOffsets, "group1");
        assertEquals(2, checkpoints2.size());
        assertEquals(new Checkpoint("group1", sourceT1p0, 400, 376, ""), checkpoints2.get(sourceT1p0));
        assertEquals(new Checkpoint("group1", t1p0, 400, 376, ""), checkpoints2.get(t1p0));
    }
    
    @Test
    public void testCheckpointStoreInitialized() throws InterruptedException {
        CheckpointStore checkpointStore = mock(CheckpointStore.class);

        MirrorCheckpointTask task = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(),
                new OffsetSyncStoreTest.FakeOffsetSyncStore(),
                Collections.singleton("group"),
                Collections.emptyMap(),
                checkpointStore) {

            @Override
            List<SourceRecord> sourceRecordsForGroup(String group) {
                SourceRecord sr = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "", 0, null, null);
                return Collections.singletonList(sr);
            }
        };

        assertNull(task.poll());

        when(checkpointStore.isInitialized()).thenReturn(true);
        List<SourceRecord> polled = task.poll();
        assertEquals(1, polled.size());
    }
}
