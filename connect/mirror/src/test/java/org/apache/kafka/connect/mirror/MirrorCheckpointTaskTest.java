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

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorCheckpointTaskTest {

    @Test
    public void testDownstreamTopicRenaming() {
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), null, Collections.emptyMap(), Collections.emptyMap());
        assertEquals(new TopicPartition("source1.topic3", 4),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("topic3", 4)),
                "Renaming source1.topic3 failed");
        assertEquals(new TopicPartition("topic3", 5),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("target2.topic3", 5)),
                "Renaming target2.topic3 failed");
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
        offsetSyncStore.start();
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), offsetSyncStore, Collections.emptyMap(), Collections.emptyMap());
        offsetSyncStore.sync(new TopicPartition("topic1", 2), t1UpstreamOffset, t1DownstreamOffset);
        offsetSyncStore.sync(new TopicPartition("target2.topic5", 6), t2UpstreamOffset, t2DownstreamOffset);
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
        Optional<Checkpoint> optionalCheckpoint2 = mirrorCheckpointTask.checkpoint("group11", new TopicPartition("target2.topic5", 6),
            new OffsetAndMetadata(12, null));
        assertTrue(optionalCheckpoint2.isPresent());
        Checkpoint checkpoint2 = optionalCheckpoint2.get();
        SourceRecord sourceRecord2 = mirrorCheckpointTask.checkpointRecord(checkpoint2, 234L);
        assertEquals(new TopicPartition("topic5", 6), checkpoint2.topicPartition(),
                "checkpoint group11 topic5 failed");
        assertEquals("group11", checkpoint2.consumerGroupId(),
                "checkpoint group11 consumerGroupId failed");
        assertEquals("group11", Checkpoint.unwrapGroup(sourceRecord2.sourcePartition()),
                "checkpoint group11 sourcePartition failed");
        assertEquals(12, checkpoint2.upstreamOffset(),
                "checkpoint group11 upstreamOffset failed");
        assertEquals(t2DownstreamOffset + 1, checkpoint2.downstreamOffset(),
                "checkpoint group11 downstreamOffset failed");
        assertEquals(234L, sourceRecord2.timestamp().longValue(),
                    "checkpoint group11 timestamp failed");
        Optional<Checkpoint> optionalCheckpoint3 = mirrorCheckpointTask.checkpoint("group13", new TopicPartition("target2.topic5", 6),
                new OffsetAndMetadata(7, null));
        assertTrue(optionalCheckpoint3.isPresent());
        Checkpoint checkpoint3 = optionalCheckpoint3.get();
        SourceRecord sourceRecord3 = mirrorCheckpointTask.checkpointRecord(checkpoint3, 234L);
        assertEquals(new TopicPartition("topic5", 6), checkpoint3.topicPartition(),
                "checkpoint group13 topic5 failed");
        assertEquals("group13", checkpoint3.consumerGroupId(),
                "checkpoint group13 consumerGroupId failed");
        assertEquals("group13", Checkpoint.unwrapGroup(sourceRecord3.sourcePartition()),
                "checkpoint group13 sourcePartition failed");
        assertEquals(t2UpstreamOffset, checkpoint3.upstreamOffset(),
                "checkpoint group13 upstreamOffset failed");
        assertEquals(t2DownstreamOffset, checkpoint3.downstreamOffset(),
                "checkpoint group13 downstreamOffset failed");
        assertEquals(234L, sourceRecord3.timestamp().longValue(),
                "checkpoint group13 timestamp failed");
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
            new DefaultReplicationPolicy(), null, idleConsumerGroupsOffset, checkpointsPerConsumerGroup);

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
                new DefaultReplicationPolicy(), null, idleConsumerGroupsOffset, checkpointsPerConsumerGroup);

        Map<String, Map<TopicPartition, OffsetAndMetadata>> output = mirrorCheckpointTask.syncGroupOffset();

        assertEquals(101, output.get(consumer).get(tp).offset(), "Consumer " + topic + " failed");
    }

    @Test
    public void testNoCheckpointForTopicWithoutOffsetSyncs() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start();
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(), offsetSyncStore, Collections.emptyMap(), Collections.emptyMap());
        offsetSyncStore.sync(new TopicPartition("topic1", 0), 3L, 4L);

        Optional<Checkpoint> checkpoint1 = mirrorCheckpointTask.checkpoint("group9", new TopicPartition("topic1", 1),
                new OffsetAndMetadata(10, null));
        Optional<Checkpoint> checkpoint2 = mirrorCheckpointTask.checkpoint("group9", new TopicPartition("topic1", 0),
                new OffsetAndMetadata(10, null));
        assertFalse(checkpoint1.isPresent());
        assertTrue(checkpoint2.isPresent());
    }

    @Test
    public void testNoCheckpointForTopicWithNullOffsetAndMetadata() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start();
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), offsetSyncStore, Collections.emptyMap(), Collections.emptyMap());
        offsetSyncStore.sync(new TopicPartition("topic1", 0), 1L, 3L);
        Optional<Checkpoint> checkpoint = mirrorCheckpointTask.checkpoint("g1", new TopicPartition("topic1", 0), null);
        assertFalse(checkpoint.isPresent());
    }

    @Test
    public void testCheckpointRecordsMonotonicIfStoreRewinds() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        offsetSyncStore.start();
        Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup = new HashMap<>();
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
                new DefaultReplicationPolicy(), offsetSyncStore, Collections.emptyMap(), checkpointsPerConsumerGroup);
        TopicPartition tp = new TopicPartition("topic1", 0);
        TopicPartition targetTP = new TopicPartition("source1.topic1", 0);

        long upstream = 11L;
        long downstream = 4L;
        // Emit syncs 0 and 1, and use the sync 1 to translate offsets and commit checkpoints
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        long consumerGroupOffset = upstream;
        long expectedDownstreamOffset = downstream;
        assertEquals(OptionalLong.of(expectedDownstreamOffset), offsetSyncStore.translateDownstream("g1", tp, consumerGroupOffset));
        Map<TopicPartition, Checkpoint> checkpoints = assertCheckpointForTopic(mirrorCheckpointTask, tp, targetTP, consumerGroupOffset, true);

        // the task normally does this, but simulate it here
        checkpointsPerConsumerGroup.put("g1", checkpoints);

        // Emit syncs 2-6 which will cause the store to drop sync 1, forcing translation to fall back to 0.
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        offsetSyncStore.sync(tp, upstream++, downstream++);
        // The OffsetSyncStore will change its translation of the same offset
        assertNotEquals(OptionalLong.of(expectedDownstreamOffset), offsetSyncStore.translateDownstream("g1", tp, consumerGroupOffset));
        // But the task will filter this out and not emit a checkpoint
        assertCheckpointForTopic(mirrorCheckpointTask, tp, targetTP, consumerGroupOffset, false);

        // If then the upstream offset rewinds in the topic and is still translatable, a checkpoint will be emitted
        // also rewinding the downstream offsets to match. This will not affect auto-synced groups, only checkpoints.
        assertCheckpointForTopic(mirrorCheckpointTask, tp, targetTP, consumerGroupOffset - 1, true);
    }

    private Map<TopicPartition, Checkpoint> assertCheckpointForTopic(
            MirrorCheckpointTask task, TopicPartition tp, TopicPartition remoteTp, long consumerGroupOffset, boolean truth
    ) {
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = Collections.singletonMap(tp, new OffsetAndMetadata(consumerGroupOffset));
        Map<TopicPartition, Checkpoint> checkpoints = task.checkpointsForGroup(consumerGroupOffsets, "g1");
        assertEquals(truth, checkpoints.containsKey(remoteTp), "should" + (truth ? "" : " not") + " emit offset sync");
        return checkpoints;
    }
}
