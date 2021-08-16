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

    static {
        TOPIC_IMAGES1 = Arrays.asList(
            newTopicImage("foo", Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA"),
                new PartitionRegistration(new int[] {2, 3, 4},
                    new int[] {2, 3}, Replicas.NONE, Replicas.NONE, 2, 1, 345),
                new PartitionRegistration(new int[] {3, 4, 5},
                    new int[] {3, 4, 5}, Replicas.NONE, Replicas.NONE, 3, 4, 684)),
            newTopicImage("bar", Uuid.fromString("f62ptyETTjet8SL5ZeREiw"),
                new PartitionRegistration(new int[] {0, 1, 2, 3, 4},
                    new int[] {0, 1, 2, 3}, new int[] {1}, new int[] {3, 4}, 0, 1, 345)));

        IMAGE1 = new TopicsImage(newTopicsByIdMap(TOPIC_IMAGES1), newTopicsByNameMap(TOPIC_IMAGES1));

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveTopicRecord().
            setTopicId(Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA")),
            REMOVE_TOPIC_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new PartitionChangeRecord().
            setTopicId(Uuid.fromString("f62ptyETTjet8SL5ZeREiw")).
            setPartitionId(0).setLeader(1),
            PARTITION_CHANGE_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new TopicRecord().
            setName("baz").setTopicId(Uuid.fromString("tgHBnRglT5W_RlENnuG5vg")),
            TOPIC_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new PartitionRecord().
            setPartitionId(0).
            setTopicId(Uuid.fromString("tgHBnRglT5W_RlENnuG5vg")).
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
            newTopicImage("bar", Uuid.fromString("f62ptyETTjet8SL5ZeREiw"),
                new PartitionRegistration(new int[] {0, 1, 2, 3, 4},
                    new int[] {0, 1, 2, 3}, new int[] {1}, new int[] {3, 4}, 1, 2, 346)),
            newTopicImage("baz", Uuid.fromString("tgHBnRglT5W_RlENnuG5vg"),
                new PartitionRegistration(new int[] {1, 2, 3, 4},
                    new int[] {3, 4}, new int[] {2}, new int[] {1}, 3, 2, 1)));
        IMAGE2 = new TopicsImage(newTopicsByIdMap(topics2), newTopicsByNameMap(topics2));
    }

    private PartitionRegistration newPartition(int[] replicas) {
        return new PartitionRegistration(replicas, replicas, Replicas.NONE, Replicas.NONE, replicas[0], 1, 1);
    }

    @Test
    public void testLocalReplicaChanges() {
        int localId = 3;
        Uuid newFooId = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");

        List<TopicImage> topics = new ArrayList<>(TOPIC_IMAGES1);
        topics.add(
            newTopicImage(
                "foo",
                newFooId,
                newPartition(new int[] {0, 1, 3}),
                newPartition(new int[] {3, 1, 2}),
                newPartition(new int[] {0, 1, 3}),
                newPartition(new int[] {3, 1, 2}),
                newPartition(new int[] {0, 1, 2}),
                newPartition(new int[] {0, 1, 2})
            )
        );

        TopicsImage image = new TopicsImage(newTopicsByIdMap(topics), newTopicsByNameMap(topics));

        List<ApiMessageAndVersion> topicRecords = new ArrayList<>(DELTA1_RECORDS);
        // foo-0 - follower to leader
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(newFooId)
                  .setPartitionId(0)
                  .setLeader(3),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // foo-1 - leader to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(newFooId)
                  .setPartitionId(1)
                  .setLeader(1),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // foo-2 - follower to removed
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(newFooId)
                  .setPartitionId(2)
                  .setIsr(Arrays.asList(0, 1, 2))
                  .setReplicas(Arrays.asList(0, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // foo-3 - leader to removed
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(newFooId)
                  .setPartitionId(3)
                  .setLeader(0)
                  .setIsr(Arrays.asList(0, 1, 2))
                  .setReplicas(Arrays.asList(0, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // foo-4 - not replica to leader
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(newFooId)
                  .setPartitionId(4)
                  .setLeader(3)
                  .setIsr(Arrays.asList(3, 1, 2))
                  .setReplicas(Arrays.asList(3, 1, 2)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );
        // foo-5 - not replica to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionChangeRecord()
                  .setTopicId(newFooId)
                  .setPartitionId(5)
                  .setIsr(Arrays.asList(0, 1, 3))
                  .setReplicas(Arrays.asList(0, 1, 3)),
                PARTITION_CHANGE_RECORD.highestSupportedVersion()
            )
        );

        /* Changes already include in DELTA1_RECORDS:
         * foo - topic id deleted
         * bar-0 - stay as follower with different partition epoch
         * baz-0 - new topic to leader
         */

        // baz-1 - new topic to follower
        topicRecords.add(
            new ApiMessageAndVersion(
                new PartitionRecord()
                    .setPartitionId(1)
                    .setTopicId(Uuid.fromString("tgHBnRglT5W_RlENnuG5vg"))
                    .setReplicas(Arrays.asList(4, 2, 3))
                    .setIsr(Arrays.asList(4, 2, 3))
                    .setLeader(4)
                    .setLeaderEpoch(2)
                    .setPartitionEpoch(1),
                PARTITION_RECORD.highestSupportedVersion()
            )
        );

        TopicsDelta delta = new TopicsDelta(image);
        RecordTestUtils.replayAll(delta, topicRecords);

        LocalReplicaChanges changes = delta.localChanges(localId);
        assertEquals(
            new HashSet<>(
                Arrays.asList(
                    new TopicPartition("foo", 2),
                    new TopicPartition("foo", 3),
                    new TopicPartition("foo", 0), // from remove topic of the old id
                    new TopicPartition("foo", 1)  // from remove topic of the old id
                )
            ),
            changes.deletes()
        );
        assertEquals(
            new HashSet<>(
                Arrays.asList(
                    new TopicPartition("foo", 0),
                    new TopicPartition("foo", 4),
                    new TopicPartition("baz", 0)
                )
            ),
            changes.leaders().keySet()
        );
        assertEquals(
            new HashSet<>(
                Arrays.asList(
                    new TopicPartition("foo", 1),
                    new TopicPartition("foo", 5),
                    new TopicPartition("baz", 1),
                    new TopicPartition("bar", 0)
                )
            ),
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
}
