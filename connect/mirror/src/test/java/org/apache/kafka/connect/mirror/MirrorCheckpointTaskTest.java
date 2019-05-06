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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MirrorCheckpointTaskTest {

    @Test
    public void testDownstreamTopicRenaming() {
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), null);
        assertEquals(new TopicPartition("source1.topic3", 4),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("topic3", 4)));
        assertEquals(new TopicPartition("topic3", 5),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("target2.topic3", 5)));
        assertEquals(new TopicPartition("source1.source6.topic7", 8),
            mirrorCheckpointTask.renameTopicPartition(new TopicPartition("source6.topic7", 8)));
    }

    @Test
    public void testCheckpoint() {
        OffsetSyncStoreTest.FakeOffsetSyncStore offsetSyncStore = new OffsetSyncStoreTest.FakeOffsetSyncStore();
        MirrorCheckpointTask mirrorCheckpointTask = new MirrorCheckpointTask("source1", "target2",
            new DefaultReplicationPolicy(), offsetSyncStore);
        offsetSyncStore.sync(new TopicPartition("topic1", 2), 3L, 4L);
        offsetSyncStore.sync(new TopicPartition("target2.topic5", 6), 7L, 8L);
        Checkpoint checkpoint1 = mirrorCheckpointTask.checkpoint("group9", new TopicPartition("topic1", 2),
            new OffsetAndMetadata(10, null));
        SourceRecord sourceRecord1 = mirrorCheckpointTask.checkpointRecord(checkpoint1, 123L);
        assertEquals(new TopicPartition("source1.topic1", 2), checkpoint1.topicPartition());
        assertEquals("group9", checkpoint1.consumerGroupId());
        assertEquals("group9", Checkpoint.unwrapGroup(sourceRecord1.sourcePartition()));
        assertEquals(10, checkpoint1.upstreamOffset());
        assertEquals(11, checkpoint1.downstreamOffset());
        assertEquals(123L, sourceRecord1.timestamp().longValue());
        Checkpoint checkpoint2 = mirrorCheckpointTask.checkpoint("group11", new TopicPartition("target2.topic5", 6),
            new OffsetAndMetadata(12, null));
        SourceRecord sourceRecord2 = mirrorCheckpointTask.checkpointRecord(checkpoint2, 234L);
        assertEquals(new TopicPartition("topic5", 6), checkpoint2.topicPartition());
        assertEquals("group11", checkpoint2.consumerGroupId());
        assertEquals("group11", Checkpoint.unwrapGroup(sourceRecord2.sourcePartition()));
        assertEquals(12, checkpoint2.upstreamOffset());
        assertEquals(13, checkpoint2.downstreamOffset());
        assertEquals(234L, sourceRecord2.timestamp().longValue());
    }
}
