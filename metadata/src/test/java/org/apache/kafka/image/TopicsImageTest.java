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
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_CHANGE_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.PARTITION_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.REMOVE_TOPIC_RECORD;
import static org.apache.kafka.common.metadata.MetadataRecordType.TOPIC_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class TopicsImageTest {
    final static TopicsImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static TopicsDelta DELTA1;

    final static TopicsImage IMAGE2;

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
        List<TopicImage> topics1 = Arrays.asList(
            newTopicImage("foo", Uuid.fromString("ThIaNwRnSM2Nt9Mx1v0RvA"),
            new PartitionRegistration(new int[] {2, 3, 4},
                new int[] {2, 3}, Replicas.NONE, Replicas.NONE, 2, 1, 345),
            new PartitionRegistration(new int[] {3, 4, 5},
                new int[] {3, 4, 5}, Replicas.NONE, Replicas.NONE, 3, 4, 684)),
            newTopicImage("bar", Uuid.fromString("f62ptyETTjet8SL5ZeREiw"),
                new PartitionRegistration(new int[] {0, 1, 2, 3, 4},
                    new int[] {0, 1, 2, 3}, new int[] {1}, new int[] {3, 4}, 0, 1, 345)));
        IMAGE1 = new TopicsImage(newTopicsByIdMap(topics1), newTopicsByNameMap(topics1));

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
