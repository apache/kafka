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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogManagerTest {

    private static final TopicIdPartition FOO_0 = new TopicIdPartition(Uuid.fromString("Sl08ZXU2QW6uF5hIoSzc8w"), new TopicPartition("foo", 0));
    private static final TopicIdPartition FOO_1 = new TopicIdPartition(Uuid.fromString("Sl08ZXU2QW6uF5hIoSzc8w"), new TopicPartition("foo", 1));
    private static final TopicIdPartition BAR_0 = new TopicIdPartition(Uuid.fromString("69O438ZkTSeqqclTtZO2KA"), new TopicPartition("bar", 0));
    private static final TopicIdPartition BAR_1 = new TopicIdPartition(Uuid.fromString("69O438ZkTSeqqclTtZO2KA"), new TopicPartition("bar", 1));
    private static final TopicIdPartition QUUX_0 = new TopicIdPartition(Uuid.fromString("YS9owjv5TG2OlsvBM0Qw6g"), new TopicPartition("quux", 0));

    @SuppressWarnings("unchecked")
    @Test
    public void testWaitForAllToComplete() throws ExecutionException, InterruptedException {
        AtomicInteger invokedCount = new AtomicInteger(0);
        Future<Boolean> success = mock(Future.class);
        when(success.get()).thenAnswer(a -> {
            invokedCount.incrementAndGet();
            return true;
        });
        Future<Boolean> failure = mock(Future.class);
        when(failure.get()).thenAnswer(a -> {
            invokedCount.incrementAndGet();
            throw new RuntimeException();
        });

        AtomicInteger failureCount = new AtomicInteger(0);
        // all futures should be evaluated
        assertFalse(LogManager.waitForAllToComplete(List.of(success, failure), t -> failureCount.incrementAndGet()));
        assertEquals(2, invokedCount.get());
        assertEquals(1, failureCount.get());
        assertFalse(LogManager.waitForAllToComplete(List.of(failure, success), t -> failureCount.incrementAndGet()));
        assertEquals(4, invokedCount.get());
        assertEquals(2, failureCount.get());
        assertTrue(LogManager.waitForAllToComplete(List.of(success, success), t -> failureCount.incrementAndGet()));
        assertEquals(6, invokedCount.get());
        assertEquals(2, failureCount.get());
        assertFalse(LogManager.waitForAllToComplete(List.of(failure, failure), t -> failureCount.incrementAndGet()));
        assertEquals(8, invokedCount.get());
        assertEquals(4, failureCount.get());
    }

    @Test
    public void testIsStrayKraftReplicaWithEmptyImage() {
        TopicsImage image = topicsImage(List.of());
        List<UnifiedLog> onDisk = Stream.of(FOO_0, FOO_1, BAR_0, BAR_1, QUUX_0).map(this::mockLog).toList();
        assertTrue(onDisk.stream().allMatch(log -> LogManager.isStrayKraftReplica(0, image, log)));
    }

    @Test
    public void testIsStrayKraftReplicaInImage() {
        TopicsImage image = topicsImage(List.of(
            topicImage(Map.of(
                    FOO_0, List.of(0, 1, 2))),
            topicImage(Map.of(
                    BAR_0, List.of(0, 1, 2),
                    BAR_1, List.of(0, 1, 2)))
        ));
        List<UnifiedLog> onDisk = Stream.of(FOO_0, FOO_1, BAR_0, BAR_1, QUUX_0).map(this::mockLog).toList();
        Set<TopicPartition> expectedStrays = Stream.of(FOO_1, QUUX_0).map(TopicIdPartition::topicPartition).collect(Collectors.toSet());

        onDisk.forEach(log -> assertEquals(expectedStrays.contains(log.topicPartition()), LogManager.isStrayKraftReplica(0, image, log)));
    }

    @Test
    public void testIsStrayKraftReplicaInImageWithRemoteReplicas() {
        TopicsImage image = topicsImage(List.of(
            topicImage(Map.of(
                    FOO_0, List.of(0, 1, 2))),
            topicImage(Map.of(
                    BAR_0, List.of(1, 2, 3),
                    BAR_1, List.of(2, 3, 0)))
        ));
        List<UnifiedLog> onDisk = Stream.of(FOO_0, BAR_0, BAR_1).map(this::mockLog).toList();
        Set<TopicPartition> expectedStrays = Stream.of(BAR_0).map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
        onDisk.forEach(log -> assertEquals(expectedStrays.contains(log.topicPartition()), LogManager.isStrayKraftReplica(0, image, log)));
    }

    @Test
    public void testIsStrayKraftMissingTopicId() {
        UnifiedLog log = mock(UnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.empty());
        assertTrue(LogManager.isStrayKraftReplica(0, topicsImage(List.of()), log));
    }

    private TopicsImage topicsImage(List<TopicImage> topics) {
        TopicsImage retval = TopicsImage.EMPTY;
        for (TopicImage topic : topics) {
            retval = retval.including(topic);
        }
        return retval;
    }

    private TopicImage topicImage(Map<TopicIdPartition, List<Integer>> partitions) {
        String topicName = null;
        Uuid topicId = null;
        for (TopicIdPartition partition : partitions.keySet()) {
            if (topicId == null) {
                topicId = partition.topicId();
            } else if (!topicId.equals(partition.topicId())) {
                throw new IllegalArgumentException("partition topic IDs did not match");
            }
            if (topicName == null) {
                topicName = partition.topic();
            } else if (!topicName.equals(partition.topic())) {
                throw new IllegalArgumentException("partition topic names did not match");
            }
        }
        if (topicId == null) {
            throw new IllegalArgumentException("Invalid empty partitions map.");
        }
        Map<Integer, PartitionRegistration> partitionRegistrations = partitions.entrySet().stream().collect(
                Collectors.toMap(
                        entry -> entry.getKey().partition(),
                        entry -> new PartitionRegistration.Builder()
                                .setReplicas(entry.getValue().stream().mapToInt(Integer::intValue).toArray())
                                .setDirectories(DirectoryId.unassignedArray(entry.getValue().size()))
                                .setIsr(entry.getValue().stream().mapToInt(Integer::intValue).toArray())
                                .setLeader(entry.getValue().get(0))
                                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                                .setLeaderEpoch(0)
                                .setPartitionEpoch(0)
                                .build()));
        return new TopicImage(topicName, topicId, partitionRegistrations);
    }

    private UnifiedLog mockLog(TopicIdPartition topicIdPartition) {
        UnifiedLog log = mock(UnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.of(topicIdPartition.topicId()));
        when(log.topicPartition()).thenReturn(topicIdPartition.topicPartition());
        return log;
    }
}
