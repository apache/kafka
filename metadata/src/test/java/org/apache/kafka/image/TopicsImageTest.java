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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.ClearElrRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.immutable.ImmutableMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.metadata.MetadataRecordType.CLEAR_ELR_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.REMOVE_TOPIC_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.TOPIC_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class TopicsImageTest {
    public static final TopicsImage IMAGE1;
    public static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final TopicsImage IMAGE2;

    private static final TopicsDelta DELTA1;
    private static final List<TopicImage> TOPIC_IMAGES1;

    private static TopicImage newTopicImage(String name, Uuid id, PartitionRegistration... partitions) {
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        int i = 0;
        for (PartitionRegistration partition : partitions) {
            partitionMap.put(i++, partition);
        }
        return new TopicImage(name, id, partitionMap);
    }

    private static ImmutableMap<Uuid, TopicImage> newTopicsByIdMap(Collection<TopicImage> topics) {
        ImmutableMap<Uuid, TopicImage> map = TopicsImage.EMPTY.topicsById();
        for (TopicImage topic : topics) {
            map = map.updated(topic.id(), topic);
        }
        return map;
    }

    private static ImmutableMap<String, TopicImage> newTopicsByNameMap(Collection<TopicImage> topics) {
        ImmutableMap<String, TopicImage> map = TopicsImage.EMPTY.topicsByName();
        for (TopicImage topic : topics) {
            map = map.updated(topic.name(), topic);
        }
        return map;
    }

    private static final Uuid FOO_UUID = Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA");
    private static final Uuid FOO_UUID2 = Uuid.fromString("9d3lha5qv8DoIl93jf8pbX");
    private static final Uuid BAR_UUID = Uuid.fromString("f62ptyETTjet8SL5ZeREiw");
    private static final Uuid BAZ_UUID = Uuid.fromString("tgHBnRglT5W_RlENnuG5vg");
    private static final Uuid BAM_UUID = Uuid.fromString("b66ybsWIQoygs01vdjH07A");
    private static final Uuid BAM_UUID2 = Uuid.fromString("yd6Sq3a9aK1G8snlKv7ag5");

    private static final TopicIdPartition FOO_0 = new TopicIdPartition(FOO_UUID, new TopicPartition("foo", 0));

    static {
        TOPIC_IMAGES1 = List.of(
            newTopicImage("foo", FOO_UUID,
                new PartitionRegistration.Builder().setReplicas(new int[] {2, 3, 4}).
                    setDirectories(DirectoryId.migratingArray(3)).
                    setIsr(new int[] {2, 3}).setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(345).build(),
                new PartitionRegistration.Builder().setReplicas(new int[] {3, 4, 5}).
                        setDirectories(DirectoryId.migratingArray(3)).
                    setIsr(new int[] {3, 4, 5}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(4).setPartitionEpoch(684).build(),
                new PartitionRegistration.Builder().setReplicas(new int[] {2, 4, 5}).
                        setDirectories(DirectoryId.migratingArray(3)).
                    setIsr(new int[] {2, 4, 5}).setLeader(2).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(10).setPartitionEpoch(84).build()),
            newTopicImage("bar", BAR_UUID,
                new PartitionRegistration.Builder().setReplicas(new int[] {0, 1, 2, 3, 4}).
                    setDirectories(DirectoryId.migratingArray(5)).
                    setIsr(new int[] {0, 1, 2, 3}).setRemovingReplicas(new int[] {1}).setAddingReplicas(new int[] {3, 4}).setLeader(0).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(345).build()));

        IMAGE1 = new TopicsImage(newTopicsByIdMap(TOPIC_IMAGES1), newTopicsByNameMap(TOPIC_IMAGES1));

        DELTA1_RECORDS = new ArrayList<>();
        // remove topic
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveTopicRecord().
            setTopicId(FOO_UUID),
            REMOVE_TOPIC_RECORD.highestSupportedVersion()));
        // change topic
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new PartitionChangeRecord().
            setTopicId(BAR_UUID).
            setPartitionId(0).setLeader(1),
            PARTITION_CHANGE_RECORD.highestSupportedVersion()));
        // add topic
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new TopicRecord().
            setName("baz").setTopicId(BAZ_UUID),
            TOPIC_RECORD.highestSupportedVersion()));
        // add partition record for new topic
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new PartitionRecord().
            setPartitionId(0).
            setTopicId(BAZ_UUID).
            setReplicas(List.of(1, 2, 3, 4)).
            setIsr(List.of(3, 4)).
            setRemovingReplicas(List.of(2)).
            setAddingReplicas(List.of(1)).
            setLeader(3).
            setLeaderEpoch(2).
            setPartitionEpoch(1), PARTITION_RECORD.highestSupportedVersion()));
        // re-add topic with different topic id
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new TopicRecord().
            setName("foo").setTopicId(FOO_UUID2),
            TOPIC_RECORD.highestSupportedVersion()));
        // add then remove topic
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new TopicRecord().
            setName("bam").setTopicId(BAM_UUID),
            TOPIC_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveTopicRecord().
            setTopicId(BAM_UUID),
            REMOVE_TOPIC_RECORD.highestSupportedVersion()));

        DELTA1 = new TopicsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        List<TopicImage> topics2 = List.of(
            newTopicImage("foo", FOO_UUID2),
            newTopicImage("bar", BAR_UUID,
                new PartitionRegistration.Builder().setReplicas(new int[] {0, 1, 2, 3, 4}).
                    setDirectories(DirectoryId.migratingArray(5)).
                    setIsr(new int[] {0, 1, 2, 3}).setRemovingReplicas(new int[] {1}).setAddingReplicas(new int[] {3, 4}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(2).setPartitionEpoch(346).build()),
            newTopicImage("baz", BAZ_UUID,
                new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4}).
                    setDirectories(DirectoryId.migratingArray(4)).
                    setIsr(new int[] {3, 4}).setRemovingReplicas(new int[] {2}).setAddingReplicas(new int[] {1}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(2).setPartitionEpoch(1).build()));
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

    private static PartitionRegistration newPartition(int[] replicas) {
        Uuid[] directories = new Uuid[replicas.length];
        for (int i = 0; i < replicas.length; i++) {
            directories[i] = DirectoryId.random();
        }
        return new PartitionRegistration.Builder().
            setReplicas(replicas).
            setDirectories(directories).
            setIsr(replicas).
            setLeader(replicas[0]).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(1).
            setPartitionEpoch(1).
            build();
    }

    @Test
    public void testBasicLocalChanges() {
        int localId = 3;
        /* Changes already include in DELTA1_RECORDS and IMAGE1:
         * foo - topic id deleted then recreated with different topic id
         * bar-0 - stay as follower with different partition epoch
         * baz-0 - new topic to leader
         * bam - topic id created then deleted
         */
        List<ApiMessageAndVersion> topicRecords = new ArrayList<>(DELTA1_RECORDS);

        // Create a new bam topic with a different id
        topicRecords.add(
            new ApiMessageAndVersion(
                new TopicRecord().setName("bam").setTopicId(BAM_UUID2),
                TOPIC_RECORD.highestSupportedVersion()
            )
        );
        topicRecords.add(newPartitionRecord(BAM_UUID2, 0, List.of(0, 1, 2)));
        topicRecords.add(newPartitionRecord(BAM_UUID2, 1, List.of(0, 1, localId)));

        // baz-1 - new partition to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionRecord()
                    .setPartitionId(1)
                    .setTopicId(BAZ_UUID)
                    .setReplicas(List.of(4, 2, localId))
                    .setIsr(List.of(4, 2, localId))
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
            Set.of(new TopicPartition("baz", 0)),
            changes.electedLeaders().keySet()
        );
        assertEquals(
            Set.of(new TopicPartition("baz", 0)),
            changes.leaders().keySet()
        );
        assertEquals(
            Set.of(new TopicPartition("baz", 1), new TopicPartition("bar", 0),
                    new TopicPartition("bam", 1)),
            changes.followers().keySet()
        );

        TopicsImage finalImage = delta.apply();
        List<ApiMessageAndVersion> imageRecords = getImageRecords(IMAGE1);
        imageRecords.addAll(topicRecords);
        testToImage(finalImage, Optional.of(imageRecords));
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
        assertEquals(Set.of(new TopicPartition("zoo", 0)), changes.deletes());
        assertEquals(Map.of(), changes.electedLeaders());
        assertEquals(Map.of(), changes.leaders());
        assertEquals(Map.of(), changes.followers());

        TopicsImage finalImage = delta.apply();
        List<ApiMessageAndVersion> imageRecords = getImageRecords(image);
        imageRecords.addAll(topicRecords);
        testToImage(finalImage, Optional.of(imageRecords));
    }

    @Test
    public void testUpdatedLeaders() {
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
        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics),
            newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(zooId).setPartitionId(0).setIsr(List.of(localId, 1)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);

        LocalReplicaChanges changes = delta.localChanges(localId);
        assertEquals(Set.of(), changes.deletes());
        assertEquals(Map.of(), changes.electedLeaders());
        assertEquals(
            Set.of(new TopicPartition("zoo", 0)),
            changes.leaders().keySet()
        );
        assertEquals(Map.of(), changes.followers());
    }

    @Test
    public void testClearElrRecords() {
        Uuid fooId = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");
        Uuid barId = Uuid.fromString("f62ptyETTjet8SL5ZeREiw");

        List<TopicImage> topics = new ArrayList<>();
        topics.add(
            newTopicImage(
                "foo",
                fooId,
                newPartition(new int[] {0, 1, 2, 3})
            )
        );
        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics),
            newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(fooId).setPartitionId(0).
                    setIsr(List.of(0, 1)).
                    setEligibleLeaderReplicas(List.of(2)).
                    setLastKnownElr(List.of(3)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);
        image = delta.apply();

        assertEquals(1, image.getTopic(fooId).partitions().get(0).elr.length);
        assertEquals(1, image.getTopic(fooId).partitions().get(0).lastKnownElr.length);

        topicRecords = new ArrayList<>();

        /* Test the following:
        1. The clear elr record should work on all existing topics(foo).
        2. The clear elr record should work on the new topic(bar) in the same batch.
        */
        topicRecords.addAll(List.of(
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(barId).
                    setName("bar"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(barId).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1)).
                    setEligibleLeaderReplicas(List.of(2)).
                    setLastKnownElr(List.of(3)),
                PARTITION_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new ClearElrRecord().setTopicName("bar"),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new ClearElrRecord(),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            ))
        );
        delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);
        image = delta.apply();

        assertEquals(0, image.getTopic(fooId).partitions().get(0).elr.length);
        assertEquals(0, image.getTopic(fooId).partitions().get(0).lastKnownElr.length);
        assertEquals(0, image.getTopic(barId).partitions().get(0).elr.length);
        assertEquals(0, image.getTopic(barId).partitions().get(0).lastKnownElr.length);
    }

    @Test
    public void testClearElrRecordOnNonExistingTopic() {
        TopicsImage image = TopicsImage.EMPTY;

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        topicRecords.addAll(List.of(
            new ApiMessageAndVersion(
                new ClearElrRecord().setTopicName("foo"),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            ))
        );
        TopicsDelta delta = new TopicsDelta(image);
        assertThrows(RuntimeException.class, () -> RecordTestUtils.replayAll(delta, topicRecords));
    }

    @Test
    public void testClearElrRecords_All_ForDeletedTopics() {
        Uuid fooId = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");
        Uuid fooId2 = Uuid.randomUuid();
        Uuid barId = Uuid.fromString("f62ptyETTjet8SL5ZeREiw");
        Uuid barId2 = Uuid.randomUuid();

        List<TopicImage> topics = new ArrayList<>();
        topics.add(
            newTopicImage(
                "foo",
                fooId,
                newPartition(new int[] {0, 1, 2, 3})
            )
        );
        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics),
            newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(fooId).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1, 2, 3)),
                PARTITION_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);
        image = delta.apply();

        topicRecords = new ArrayList<>();
        /* Test the following:
            1. Topic foo is deleted and created in the same delta, the clear elr applies on the new topic
            2. Topic bar is created, deleted, then created in the same delta, the clear elr applies on the new topic
        */
        topicRecords.addAll(List.of(
            new ApiMessageAndVersion(
                new RemoveTopicRecord().setTopicId(fooId),
                REMOVE_TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(fooId2).
                    setName("foo"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(fooId2).setPartitionId(0).
                    setIsr(List.of(0, 1)).
                    setEligibleLeaderReplicas(List.of(2)).
                    setLastKnownElr(List.of(3)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(barId).
                    setName("bar"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(barId).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1, 2, 3)),
                PARTITION_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new RemoveTopicRecord().setTopicId(barId),
                REMOVE_TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(barId2).
                    setName("bar"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(barId2).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1)).
                    setEligibleLeaderReplicas(List.of(2)).
                    setLastKnownElr(List.of(3)),
                PARTITION_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new ClearElrRecord(),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            ))
        );
        delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);
        image = delta.apply();
        assertEquals(2, image.topicsById().size());
        assertEquals(2, image.topicsByName().size());

        assertEquals(0, image.getTopic(fooId2).partitions().get(0).elr.length);
        assertEquals(0, image.getTopic(fooId2).partitions().get(0).lastKnownElr.length);
        assertEquals(0, image.getTopic(barId2).partitions().get(0).elr.length);
        assertEquals(0, image.getTopic(barId2).partitions().get(0).lastKnownElr.length);
    }

    @Test
    public void testClearElrRecords_Single_ForDeletedTopics() {
        Uuid fooId = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");
        Uuid fooId2 = Uuid.randomUuid();
        Uuid barId = Uuid.fromString("f62ptyETTjet8SL5ZeREiw");
        Uuid barId2 = Uuid.randomUuid();

        List<TopicImage> topics = new ArrayList<>();
        topics.add(
            newTopicImage(
                "foo",
                fooId,
                newPartition(new int[] {0, 1, 2, 3})
            )
        );
        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics),
            newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(fooId).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1, 2, 3)),
                PARTITION_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);
        image = delta.apply();

        topicRecords = new ArrayList<>();
        /* Test the following:
            1. Topic foo is deleted and created in the same delta, the clear elr applies on the new topic
            2. Topic bar is created, deleted, then created in the same delta, the clear elr applies on the new topic
        */
        topicRecords.addAll(List.of(
            new ApiMessageAndVersion(
                new RemoveTopicRecord().setTopicId(fooId),
                REMOVE_TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(fooId2).
                    setName("foo"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(fooId2).setPartitionId(0).
                    setIsr(List.of(0, 1)).
                    setEligibleLeaderReplicas(List.of(2)).
                    setLastKnownElr(List.of(3)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(barId).
                    setName("bar"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(barId).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1, 2, 3)),
                PARTITION_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new RemoveTopicRecord().setTopicId(barId),
                REMOVE_TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new TopicRecord().setTopicId(barId2).
                    setName("bar"),
                TOPIC_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new PartitionRecord().setTopicId(barId2).
                    setPartitionId(0).
                    setLeader(0).
                    setIsr(List.of(1)).
                    setEligibleLeaderReplicas(List.of(2)).
                    setLastKnownElr(List.of(3)),
                PARTITION_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new ClearElrRecord().setTopicName("foo"),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            ),
            new ApiMessageAndVersion(
                new ClearElrRecord().setTopicName("bar"),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            ))
        );
        delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);
        image = delta.apply();
        assertEquals(2, image.topicsById().size());
        assertEquals(2, image.topicsByName().size());

        assertEquals(0, image.getTopic(fooId2).partitions().get(0).elr.length);
        assertEquals(0, image.getTopic(fooId2).partitions().get(0).lastKnownElr.length);
        assertEquals(0, image.getTopic(barId2).partitions().get(0).elr.length);
        assertEquals(0, image.getTopic(barId2).partitions().get(0).lastKnownElr.length);
    }

    @Test
    public void testClearElrRecordForNonExistTopic() {
        TopicsImage image = new TopicsImage(newTopicsByIdMap(List.of()),
            newTopicsByNameMap(List.of()));
        TopicsDelta delta = new TopicsDelta(image);
        List<ApiMessageAndVersion> topicRecords = new ArrayList<>();
        topicRecords.add(
            new ApiMessageAndVersion(
                new ClearElrRecord().setTopicName("non-exist"),
                CLEAR_ELR_RECORD.highestSupportedVersion()
            )
        );
        assertThrows(RuntimeException.class, () -> RecordTestUtils.replayAll(delta, topicRecords));
    }

    @Test
    public void testTopicDeltaElectionStatsWithEmptyImage() {
        TopicImage image = new TopicImage("topic", Uuid.randomUuid(), Map.of());
        TopicDelta delta = new TopicDelta(image);
        delta.replay(new PartitionRecord().setPartitionId(0).setLeader(0).setIsr(List.of(0, 1)).setReplicas(List.of(0, 1, 2)));
        delta.replay(new PartitionChangeRecord().setPartitionId(0).setLeader(2).setIsr(List.of(2)).setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()));
        assertEquals(1, delta.partitionToUncleanLeaderElectionCount().get(0));
        delta.replay(new PartitionChangeRecord().setPartitionId(0).setIsr(List.of(1, 2)).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()));
        assertEquals(1, delta.partitionToUncleanLeaderElectionCount().get(0));
        delta.replay(new PartitionChangeRecord().setPartitionId(0).setLeader(0).setIsr(List.of(0)).setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()));
        assertEquals(2, delta.partitionToUncleanLeaderElectionCount().get(0));
        delta.replay(new PartitionChangeRecord().setPartitionId(0).setLeader(1).setIsr(List.of(1)).setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()));
        assertEquals(3, delta.partitionToUncleanLeaderElectionCount().get(0));
        assertTrue(delta.partitionToElrElectionCount().isEmpty());

        delta.replay(new PartitionRecord().setPartitionId(1).setLeader(0).setIsr(List.of(0, 1)).setReplicas(List.of(0, 1, 2)));
        delta.replay(new PartitionChangeRecord().setPartitionId(1).setLeader(-1).setIsr(List.of()).setEligibleLeaderReplicas(List.of(0, 1)));
        assertTrue(delta.partitionToElrElectionCount().isEmpty());
        delta.replay(new PartitionChangeRecord().setPartitionId(1).setLeader(1).setIsr(List.of(1)).setEligibleLeaderReplicas(List.of(0, 1)));
        assertEquals(1, delta.partitionToElrElectionCount().get(1));
    }

    @Test
    public void testTopicDeltaElectionStatsWithNonEmptyImage() {
        TopicImage image = new TopicImage("topic", Uuid.randomUuid(), Map.of(
            0, new PartitionRegistration(new PartitionRecord().setPartitionId(0).setLeader(0).setIsr(List.of(0, 1)).setReplicas(List.of(0, 1, 2))),
            1, new PartitionRegistration(new PartitionRecord().setPartitionId(1).setLeader(-1).setIsr(List.of()).setEligibleLeaderReplicas(List.of(0, 1)).setReplicas(List.of(0, 1, 2)))
        ));
        TopicDelta delta = new TopicDelta(image);
        delta.replay(new PartitionRecord().setPartitionId(0).setLeader(2).setIsr(List.of(2)).setReplicas(List.of(0, 1, 2)).setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value()));
        assertEquals(1, delta.partitionToUncleanLeaderElectionCount().get(0));
        assertTrue(delta.partitionToElrElectionCount().isEmpty());

        delta.replay(new PartitionChangeRecord().setPartitionId(1).setLeader(1).setIsr(List.of(1)).setEligibleLeaderReplicas(List.of(0, 1)));
        assertEquals(1, delta.partitionToElrElectionCount().get(1));
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
                  .setIsr(List.of(0, 1, 2))
                  .setReplicas(List.of(0, 1, 2)),
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
                  .setIsr(List.of(0, 1, 2))
                  .setReplicas(List.of(0, 1, 2)),
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
                  .setIsr(List.of(localId, 1, 2))
                  .setReplicas(List.of(localId, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // zoo-5 - not replica to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(zooId)
                  .setPartitionId(5)
                  .setIsr(List.of(0, 1, localId))
                  .setReplicas(List.of(0, 1, localId)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);

        LocalReplicaChanges changes = delta.localChanges(localId);
        assertEquals(
            Set.of(new TopicPartition("zoo", 2), new TopicPartition("zoo", 3)),
            changes.deletes()
        );
        assertEquals(
            Set.of(new TopicPartition("zoo", 0), new TopicPartition("zoo", 4)),
            changes.electedLeaders().keySet()
        );
        assertEquals(
            Set.of(new TopicPartition("zoo", 0), new TopicPartition("zoo", 4)),
            changes.leaders().keySet()
        );
        assertEquals(
            Set.of(new TopicPartition("zoo", 1), new TopicPartition("zoo", 5)),
            changes.followers().keySet()
        );


        TopicsImage finalImage = delta.apply();
        List<ApiMessageAndVersion> imageRecords = getImageRecords(image);
        imageRecords.addAll(topicRecords);
        testToImage(finalImage, Optional.of(imageRecords));
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(TopicsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1);
        records.addAll(DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() {
        testToImage(IMAGE2);
    }

    private static void testToImage(TopicsImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(TopicsImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(TopicsImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> TopicsImage.EMPTY,
            TopicsDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(TopicsImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build());
        return writer.records();
    }

    @Test
    public void testTopicNameToIdView() {
        Map<String, Uuid> map = IMAGE1.topicNameToIdView();
        assertTrue(map.containsKey("foo"));
        assertEquals(FOO_UUID, map.get("foo"));
        assertTrue(map.containsKey("bar"));
        assertEquals(BAR_UUID, map.get("bar"));
        assertFalse(map.containsKey("baz"));
        assertNull(map.get("baz"));
        Set<Uuid> uuids = new HashSet<>();
        map.values().iterator().forEachRemaining(uuids::add);
        Set<Uuid> expectedUuids = Set.of(
            Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA"),
            Uuid.fromString("f62ptyETTjet8SL5ZeREiw"));
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
        assertNull(map.get(BAZ_UUID));
        Set<String> names = new HashSet<>();
        map.values().iterator().forEachRemaining(names::add);
        Set<String> expectedNames = Set.of("foo", "bar");
        assertEquals(expectedNames, names);
        assertThrows(UnsupportedOperationException.class, () -> map.remove(FOO_UUID));
        assertThrows(UnsupportedOperationException.class, () -> map.put(FOO_UUID, "bar"));

        var result = IMAGE1.topicIdToNameView().get("zar");
        assertNull(result);
    }

    @Test
    public void testTopicsDeltaCreateThenDelete() {
        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY);
        delta.replay(new TopicRecord().setName("test").setTopicId(FOO_UUID));
        assertTrue(delta.createdTopicIds().contains(FOO_UUID));
        assertFalse(delta.deletedTopicIds().contains(FOO_UUID));
        delta.replay(new RemoveTopicRecord().setTopicId(FOO_UUID));
        assertFalse(delta.deletedTopicIds().contains(FOO_UUID));
        assertFalse(delta.createdTopicIds().contains(FOO_UUID));
    }

    @Test
    public void testPartitionReplicasWithEmptyImage() {
        TopicsImage image = topicsImage(List.of());
        assertTrue(image.partitionReplicas(FOO_UUID, 0).isEmpty());
    }

    @Test
    public void testPartitionReplicas() {
        TopicsImage image = topicsImage(List.of(
                newTopicImage(FOO_0.topic(), FOO_0.topicId(), newPartition(new int[]{0, 1, 2}))
        ));
        assertEquals(List.of(0, 1, 2), image.partitionReplicas(FOO_UUID, 0));
    }

    private static TopicsImage topicsImage(List<TopicImage> topics) {
        TopicsImage retval = TopicsImage.EMPTY;
        for (TopicImage topic : topics) {
            retval = retval.including(topic);
        }
        return retval;
    }
}
