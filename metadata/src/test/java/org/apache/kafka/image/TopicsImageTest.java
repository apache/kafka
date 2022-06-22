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

package org.apache.kafka.image;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.REMOVE_TOPIC_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.TOPIC_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class TopicsImageTest {
    static final TopicsImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final TopicsDelta DELTA1;

    static final TopicsImage IMAGE2;

    static final List<TopicImage> TOPIC_IMAGES1;

    private static TopicImage newTopicImage(String name, Uuid id, PartitionRegistration... partitions) {
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        int i = 0;
        for (PartitionRegistration partition : partitions) {
            partitionMap.put(i++, partition);
        }
        return new TopicImage(name, id, partitionMap);
    }

    private static Map<Uuid, TopicImage> newTopicsByIdMap(Collection<TopicImage> topics) {
        Map<Uuid, TopicImage> map = new HashMap<>();
        for (TopicImage topic : topics) {
            map.put(topic.id(), topic);
        }
        return map;
    }

    private static Map<String, TopicImage> newTopicsByNameMap(Collection<TopicImage> topics) {
        Map<String, TopicImage> map = new HashMap<>();
        for (TopicImage topic : topics) {
            map.put(topic.name(), topic);
        }
        return map;
    }

    private static final Uuid FOO_UUID = Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA");

    private static final Uuid BAR_UUID = Uuid.fromString("f62ptyETTjet8SL5ZeREiw");

    private static final Uuid BAZ_UUID = Uuid.fromString("tgHBnRglT5W_RlENnuG5vg");

    static {
        TOPIC_IMAGES1 = Arrays.asList(
            newTopicImage("foo", FOO_UUID,
                new PartitionRegistration(new int[] {2, 3, 4},
                    new int[] {2, 3}, Replicas.NONE, Replicas.NONE, 2, LeaderRecoveryState.RECOVERED, 1, 345),
                new PartitionRegistration(new int[] {3, 4, 5},
                    new int[] {3, 4, 5}, Replicas.NONE, Replicas.NONE, 3, LeaderRecoveryState.RECOVERED, 4, 684),
                new PartitionRegistration(new int[] {2, 4, 5},
                    new int[] {2, 4, 5}, Replicas.NONE, Replicas.NONE, 2, LeaderRecoveryState.RECOVERED, 10, 84)),
            newTopicImage("bar", BAR_UUID,
                new PartitionRegistration(new int[] {0, 1, 2, 3, 4},
                    new int[] {0, 1, 2, 3}, new int[] {1}, new int[] {3, 4}, 0, LeaderRecoveryState.RECOVERED, 1, 345)));

        IMAGE1 = new TopicsImage(newTopicsByIdMap(TOPIC_IMAGES1), newTopicsByNameMap(TOPIC_IMAGES1));

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveTopicRecord().
            setTopicId(FOO_UUID),
            REMOVE_TOPIC_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new PartitionChangeRecord().
            setTopicId(BAR_UUID).
            setPartitionId(0).setLeader(1),
            PARTITION_CHANGE_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new TopicRecord().
            setName("baz").setTopicId(BAZ_UUID),
            TOPIC_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new PartitionRecord().
            setPartitionId(0).
            setTopicId(BAZ_UUID).
            setReplicas(Arrays.asList(1, 2, 3, 4)).
            setIsr(Arrays.asList(3, 4)).
            setRemovingReplicas(Collections.singletonList(2)).
            setAddingReplicas(Collections.singletonList(1)).
            setLeader(3).
            setLeaderEpoch(2).
            setPartitionEpoch(1), PARTITION_RECORD.highestSupportedVersion()));

        DELTA1 = new TopicsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        List<TopicImage> topics2 = Arrays.asList(
            newTopicImage("bar", BAR_UUID,
                new PartitionRegistration(new int[] {0, 1, 2, 3, 4},
                    new int[] {0, 1, 2, 3}, new int[] {1}, new int[] {3, 4}, 1, LeaderRecoveryState.RECOVERED, 2, 346)),
            newTopicImage("baz", BAZ_UUID,
                new PartitionRegistration(new int[] {1, 2, 3, 4},
                    new int[] {3, 4}, new int[] {2}, new int[] {1}, 3, LeaderRecoveryState.RECOVERED, 2, 1)));
        IMAGE2 = new TopicsImage(newTopicsByIdMap(topics2), newTopicsByNameMap(topics2));
    }

    private ApiMessageAndVersion newPartitionRecord(Uuid topicId, int partitionId, List<Integer> replicas) {
        return new ApiMessageAndVersion(
            new PartitionRecord()
                .setPartitionId(partitionId)
                .setTopicId(topicId)
                .setReplicas(replicas)
                .setIsr(replicas)
                .setLeader(replicas.get(0))
                .setLeaderEpoch(1)
                .setPartitionEpoch(1),
            PARTITION_RECORD.highestSupportedVersion()
        );
    }

    private PartitionRegistration newPartition(int[] replicas) {
        return new PartitionRegistration(replicas, replicas, Replicas.NONE, Replicas.NONE, replicas[0], LeaderRecoveryState.RECOVERED, 1, 1);
    }

    @Test
    public void testBasicLocalChanges() {
        int localId = 3;
        /* Changes already include in DELTA1_RECORDS and IMAGE1:
         * foo - topic id deleted
         * bar-0 - stay as follower with different partition epoch
         * baz-0 - new topic to leader
         */
        List<ApiMessageAndVersion> topicRecords = new ArrayList<>(DELTA1_RECORDS);

        // Create a new foo topic with a different id
        Uuid newFooId = Uuid.fromString("b66ybsWIQoygs01vdjH07A");
        topicRecords.add(
            new ApiMessageAndVersion(
                new TopicRecord().setName("foo") .setTopicId(newFooId),
                TOPIC_RECORD.highestSupportedVersion()
            )
        );
        topicRecords.add(newPartitionRecord(newFooId, 0, Arrays.asList(0, 1, 2)));
        topicRecords.add(newPartitionRecord(newFooId, 1, Arrays.asList(0, 1, localId)));

        // baz-1 - new partition to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionRecord()
                    .setPartitionId(1)
                    .setTopicId(BAZ_UUID)
                    .setReplicas(Arrays.asList(4, 2, localId))
                    .setIsr(Arrays.asList(4, 2, localId))
                    .setLeader(4)
                    .setLeaderEpoch(2)
                    .setPartitionEpoch(1),
                PARTITION_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(IMAGE1);
        RecordTestUtils.replayAll(delta, topicRecords);

        LocalReplicaChanges changes = delta.localChanges(localId);
        assertEquals(
            new HashSet<>(Arrays.asList(new TopicPartition("foo", 0), new TopicPartition("foo", 1))),
            changes.deletes()
        );
        assertEquals(
            new HashSet<>(Arrays.asList(new TopicPartition("baz", 0))),
            changes.leaders().keySet()
        );
        assertEquals(
            new HashSet<>(
                Arrays.asList(new TopicPartition("baz", 1), new TopicPartition("bar", 0), new TopicPartition("foo", 1))
            ),
            changes.followers().keySet()
        );
    }

    @Test
    public void testDeleteAfterChanges() {
        int localId = 3;
        Uuid zooId = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");

        List<TopicImage> topics = new ArrayList<>();
        topics.add(
            newTopicImage(
                "zoo",
                zooId,
                newPartition(new int[] {localId, 1, 2})
            )
        );
        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics), newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        // leader to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(zooId).setPartitionId(0).setLeader(1),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // remove zoo topic
        topicRecords.add(
            new ApiMessageAndVersion(
                new RemoveTopicRecord().setTopicId(zooId),
                REMOVE_TOPIC_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);

        LocalReplicaChanges changes = delta.localChanges(localId);
        assertEquals(new HashSet<>(Arrays.asList(new TopicPartition("zoo", 0))), changes.deletes());
        assertEquals(Collections.emptyMap(), changes.leaders());
        assertEquals(Collections.emptyMap(), changes.followers());
    }

    @Test
    public void testLocalReassignmentChanges() {
        int localId = 3;
        Uuid zooId = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");

        List<TopicImage> topics = new ArrayList<>();
        topics.add(
            newTopicImage(
                "zoo",
                zooId,
                newPartition(new int[] {0, 1, localId}),
                newPartition(new int[] {localId, 1, 2}),
                newPartition(new int[] {0, 1, localId}),
                newPartition(new int[] {localId, 1, 2}),
                newPartition(new int[] {0, 1, 2}),
                newPartition(new int[] {0, 1, 2})
            )
        );
        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics), newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        // zoo-0 - follower to leader
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(zooId).setPartitionId(0).setLeader(localId),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // zoo-1 - leader to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(zooId).setPartitionId(1).setLeader(1),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // zoo-2 - follower to removed
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(zooId)
                  .setPartitionId(2)
                  .setIsr(Arrays.asList(0, 1, 2))
                  .setReplicas(Arrays.asList(0, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // zoo-3 - leader to removed
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(zooId)
                  .setPartitionId(3)
                  .setLeader(0)
                  .setIsr(Arrays.asList(0, 1, 2))
                  .setReplicas(Arrays.asList(0, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // zoo-4 - not replica to leader
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(zooId)
                  .setPartitionId(4)
                  .setLeader(localId)
                  .setIsr(Arrays.asList(localId, 1, 2))
                  .setReplicas(Arrays.asList(localId, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // zoo-5 - not replica to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(zooId)
                  .setPartitionId(5)
                  .setIsr(Arrays.asList(0, 1, localId))
                  .setReplicas(Arrays.asList(0, 1, localId)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);

        LocalReplicaChanges changes = delta.localChanges(localId);
        assertEquals(
            new HashSet<>(Arrays.asList(new TopicPartition("zoo", 2), new TopicPartition("zoo", 3))),
            changes.deletes()
        );
        assertEquals(
            new HashSet<>(Arrays.asList(new TopicPartition("zoo", 0), new TopicPartition("zoo", 4))),
            changes.leaders().keySet()
        );
        assertEquals(
            new HashSet<>(Arrays.asList(new TopicPartition("zoo", 1), new TopicPartition("zoo", 5))),
            changes.followers().keySet()
        );
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(TopicsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(TopicsImage image) throws Throwable {
        MockSnapshotConsumer writer = new MockSnapshotConsumer();
        image.write(writer);
        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY);
        RecordTestUtils.replayAllBatches(delta, writer.batches());
        TopicsImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }

    @Test
    public void testTopicNameToIdView() {
        Map<String, Uuid> map = IMAGE1.topicNameToIdView();
        assertTrue(map.containsKey("foo"));
        assertEquals(FOO_UUID, map.get("foo"));
        assertTrue(map.containsKey("bar"));
        assertEquals(BAR_UUID, map.get("bar"));
        assertFalse(map.containsKey("baz"));
        assertEquals(null, map.get("baz"));
        HashSet<Uuid> uuids = new HashSet<>();
        map.values().iterator().forEachRemaining(u -> uuids.add(u));
        HashSet<Uuid> expectedUuids = new HashSet<>(Arrays.asList(
            Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA"),
            Uuid.fromString("f62ptyETTjet8SL5ZeREiw")));
        assertEquals(expectedUuids, uuids);
        assertThrows(UnsupportedOperationException.class, () -> map.remove("foo"));
        assertThrows(UnsupportedOperationException.class, () -> map.put("bar", FOO_UUID));
    }

    @Test
    public void testTopicIdToNameView() {
        Map<Uuid, String> map = IMAGE1.topicIdToNameView();
        assertTrue(map.containsKey(FOO_UUID));
        assertEquals("foo", map.get(FOO_UUID));
        assertTrue(map.containsKey(BAR_UUID));
        assertEquals("bar", map.get(BAR_UUID));
        assertFalse(map.containsKey(BAZ_UUID));
        assertEquals(null, map.get(BAZ_UUID));
        HashSet<String> names = new HashSet<>();
        map.values().iterator().forEachRemaining(n -> names.add(n));
        HashSet<String> expectedNames = new HashSet<>(Arrays.asList("foo", "bar"));
        assertEquals(expectedNames, names);
        assertThrows(UnsupportedOperationException.class, () -> map.remove(FOO_UUID));
        assertThrows(UnsupportedOperationException.class, () -> map.put(FOO_UUID, "bar"));
    }
}
