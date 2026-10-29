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
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.immutable.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.REMOVE_TOPIC_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.TOPIC_RECORD;

public final class TopicsImageFixtures {

    public static final Uuid FOO_UUID = Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA");
    public static final Uuid FOO_UUID2 = Uuid.fromString("9d3lha5qv8DoIl93jf8pbX");
    public static final Uuid BAR_UUID = Uuid.fromString("f62ptyETTjet8SL5ZeREiw");
    public static final Uuid BAZ_UUID = Uuid.fromString("tgHBnRglT5W_RlENnuG5vg");
    public static final Uuid BAM_UUID = Uuid.fromString("b66ybsWIQoygs01vdjH07A");
    public static final Uuid BAM_UUID2 = Uuid.fromString("yd6Sq3a9aK1G8snlKv7ag5");

    public static final TopicIdPartition FOO_0 = new TopicIdPartition(FOO_UUID, new TopicPartition("foo", 0));

    public static final TopicsImage IMAGE1 = newTopicsImage(List.of(
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
                            setIsr(new int[] {0, 1, 2, 3}).setRemovingReplicas(new int[] {1}).setAddingReplicas(new int[] {3, 4}).setLeader(0).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(1).setPartitionEpoch(345).build())));

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
            // remove topic
            new ApiMessageAndVersion(new RemoveTopicRecord().
                    setTopicId(FOO_UUID),
                    REMOVE_TOPIC_RECORD.highestSupportedVersion()),
            // change topic
            new ApiMessageAndVersion(new PartitionChangeRecord().
                    setTopicId(BAR_UUID).
                    setPartitionId(0).setLeader(1),
                    PARTITION_CHANGE_RECORD.highestSupportedVersion()),
            // add topic
            new ApiMessageAndVersion(new TopicRecord().
                    setName("baz").setTopicId(BAZ_UUID),
                    TOPIC_RECORD.highestSupportedVersion()),
            // add partition record for new topic
            new ApiMessageAndVersion(new PartitionRecord().
                    setPartitionId(0).
                    setTopicId(BAZ_UUID).
                    setReplicas(List.of(1, 2, 3, 4)).
                    setIsr(List.of(3, 4)).
                    setRemovingReplicas(List.of(2)).
                    setAddingReplicas(List.of(1)).
                    setLeader(3).
                    setLeaderEpoch(2).
                    setPartitionEpoch(1), PARTITION_RECORD.highestSupportedVersion()),
            // re-add topic with different topic id
            new ApiMessageAndVersion(new TopicRecord().
                    setName("foo").setTopicId(FOO_UUID2),
                    TOPIC_RECORD.highestSupportedVersion()),
            // add then remove topic
            new ApiMessageAndVersion(new TopicRecord().
                    setName("bam").setTopicId(BAM_UUID),
                    TOPIC_RECORD.highestSupportedVersion()),
            new ApiMessageAndVersion(new RemoveTopicRecord().
                    setTopicId(BAM_UUID),
                    REMOVE_TOPIC_RECORD.highestSupportedVersion())
    );

    public static final TopicsDelta DELTA1 = RecordTestUtils.replayAll(
            new TopicsDelta(IMAGE1),
            DELTA1_RECORDS
    );

    public static final TopicsImage IMAGE2 = newTopicsImage(List.of(
            newTopicImage("foo", FOO_UUID2),
            newTopicImage("bar", BAR_UUID,
                    new PartitionRegistration.Builder().setReplicas(new int[] {0, 1, 2, 3, 4}).
                            setDirectories(DirectoryId.migratingArray(5)).
                            setIsr(new int[] {0, 1, 2, 3}).setRemovingReplicas(new int[] {1}).setAddingReplicas(new int[] {3, 4}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(2).setPartitionEpoch(346).build()),
            newTopicImage("baz", BAZ_UUID,
                    new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3, 4}).
                            setDirectories(DirectoryId.migratingArray(4)).
                            setIsr(new int[] {3, 4}).setRemovingReplicas(new int[] {2}).setAddingReplicas(new int[] {1}).setLeader(3).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).setLeaderEpoch(2).setPartitionEpoch(1).build())));

    public static TopicImage newTopicImage(String name, Uuid id, PartitionRegistration... partitions) {
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        int i = 0;
        for (PartitionRegistration partition : partitions) {
            partitionMap.put(i++, partition);
        }
        return new TopicImage(name, id, partitionMap);
    }

    public static ImmutableMap<Uuid, TopicImage> newTopicsByIdMap(Collection<TopicImage> topics) {
        ImmutableMap<Uuid, TopicImage> map = TopicsImage.EMPTY.topicsById();
        for (TopicImage topic : topics) {
            map = map.updated(topic.id(), topic);
        }
        return map;
    }

    public static ImmutableMap<String, TopicImage> newTopicsByNameMap(Collection<TopicImage> topics) {
        ImmutableMap<String, TopicImage> map = TopicsImage.EMPTY.topicsByName();
        for (TopicImage topic : topics) {
            map = map.updated(topic.name(), topic);
        }
        return map;
    }

    private static TopicsImage newTopicsImage(List<TopicImage> topics) {
        return new TopicsImage(newTopicsByIdMap(topics), newTopicsByNameMap(topics));
    }

    private TopicsImageFixtures() {
    }
}
