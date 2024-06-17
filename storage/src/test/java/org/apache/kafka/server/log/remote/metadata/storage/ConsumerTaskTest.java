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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.server.log.remote.metadata.storage.ConsumerTask.UserTopicIdPartition;
import static org.apache.kafka.server.log.remote.metadata.storage.ConsumerTask.toRemoteLogPartition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ConsumerTaskTest {

    private final int numMetadataTopicPartitions = 5;
    private final RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(numMetadataTopicPartitions);
    private final DummyEventHandler handler = new DummyEventHandler();
    private final Set<TopicPartition> remoteLogPartitions = IntStream.range(0, numMetadataTopicPartitions).boxed()
        .map(ConsumerTask::toRemoteLogPartition).collect(Collectors.toSet());
    private final Uuid topicId = Uuid.randomUuid();
    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

    private ConsumerTask consumerTask;
    private MockConsumer<byte[], byte[]> consumer;
    private Thread thread;

    @BeforeEach
    public void beforeEach() {
        final Map<TopicPartition, Long> offsets = remoteLogPartitions.stream()
            .collect(Collectors.toMap(Function.identity(), e -> 0L));
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(offsets);
        consumerTask = new ConsumerTask(handler, partitioner, consumer, 10L, 300_000L, new SystemTime());
        thread = new Thread(consumerTask);
    }

    @AfterEach
    public void afterEach() throws InterruptedException {
        if (thread != null) {
            assertDoesNotThrow(() -> consumerTask.close(), "Close method threw exception");
            thread.join(10_000);
            assertFalse(thread.isAlive(), "Consumer task thread is still alive");
        }
    }

    /**
     * Tests that the consumer task shuts down gracefully when there were no assignments.
     */
    @Test
    public void testCloseOnNoAssignment() throws InterruptedException {
        thread.start();
        Thread.sleep(10);
        assertDoesNotThrow(() -> consumerTask.close(), "Close method threw exception");
    }

    @Test
    public void testIdempotentClose() {
        thread.start();
        consumerTask.close();
        consumerTask.close();
    }

    @Test
    public void testUserTopicIdPartitionEquals() {
        final TopicIdPartition tpId = new TopicIdPartition(topicId, new TopicPartition("sample", 0));
        final UserTopicIdPartition utp1 = new UserTopicIdPartition(tpId, partitioner.metadataPartition(tpId));
        final UserTopicIdPartition utp2 = new UserTopicIdPartition(tpId, partitioner.metadataPartition(tpId));
        utp1.isInitialized = true;
        utp1.isAssigned = true;

        assertFalse(utp2.isInitialized);
        assertFalse(utp2.isAssigned);
        assertEquals(utp1, utp2);
    }

    @Test
    public void testAddAssignmentsForPartitions() throws InterruptedException {
        final List<TopicIdPartition> idPartitions = getIdPartitions("sample", 3);
        final Map<TopicPartition, Long> endOffsets = idPartitions.stream()
            .map(idp -> toRemoteLogPartition(partitioner.metadataPartition(idp)))
            .collect(Collectors.toMap(Function.identity(), e -> 0L, (a, b) -> b));
        consumer.updateEndOffsets(endOffsets);
        consumerTask.addAssignmentsForPartitions(new HashSet<>(idPartitions));
        thread.start();
        for (final TopicIdPartition idPartition : idPartitions) {
            TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(idPartition), "Timed out waiting for " + idPartition + " to be assigned");
            assertTrue(consumerTask.isMetadataPartitionAssigned(partitioner.metadataPartition(idPartition)));
            assertTrue(handler.isPartitionLoaded.get(idPartition));
        }
    }

    @Test
    public void testRemoveAssignmentsForPartitions() throws InterruptedException {
        final List<TopicIdPartition> allPartitions = getIdPartitions("sample", 3);
        final Map<TopicPartition, Long> endOffsets = allPartitions.stream()
            .map(idp -> toRemoteLogPartition(partitioner.metadataPartition(idp)))
            .collect(Collectors.toMap(Function.identity(), e -> 0L, (a, b) -> b));
        consumer.updateEndOffsets(endOffsets);
        consumerTask.addAssignmentsForPartitions(new HashSet<>(allPartitions));
        thread.start();

        final TopicIdPartition tpId = allPartitions.get(0);
        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId), "Timed out waiting for " + tpId + " to be assigned");
        addRecord(consumer, partitioner.metadataPartition(tpId), tpId, 0);
        TestUtils.waitForCondition(() -> consumerTask.readOffsetForMetadataPartition(partitioner.metadataPartition(tpId)).isPresent(),
            "Couldn't read record");

        final Set<TopicIdPartition> removePartitions = Collections.singleton(tpId);
        consumerTask.removeAssignmentsForPartitions(removePartitions);
        for (final TopicIdPartition idPartition : allPartitions) {
            final TestCondition condition = () -> removePartitions.contains(idPartition) == !consumerTask.isUserPartitionAssigned(idPartition);
            TestUtils.waitForCondition(condition, "Timed out waiting for " + idPartition + " to be removed");
        }
        for (TopicIdPartition removePartition : removePartitions) {
            TestUtils.waitForCondition(() -> handler.isPartitionCleared.containsKey(removePartition),
                "Timed out waiting for " + removePartition + " to be cleared");
        }
    }

    @Test
    public void testConcurrentPartitionAssignments() throws InterruptedException, ExecutionException {
        final List<TopicIdPartition> allPartitions = getIdPartitions("sample", 100);
        final Map<TopicPartition, Long> endOffsets = allPartitions.stream()
            .map(idp -> toRemoteLogPartition(partitioner.metadataPartition(idp)))
            .collect(Collectors.toMap(Function.identity(), e -> 0L, (a, b) -> b));
        consumer.updateEndOffsets(endOffsets);

        final AtomicBoolean isAllPartitionsAssigned = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        Thread assignor = new Thread(() -> {
            int partitionsAssigned = 0;
            for (TopicIdPartition partition : allPartitions) {
                if (partitionsAssigned == 50) {
                    // Once half the topic partitions are assigned, wait for the consumer to catch up. This ensures
                    // that the consumer is already running when the rest of the partitions are assigned.
                    try {
                        latch.await(1, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    }
                }
                consumerTask.addAssignmentsForPartitions(Collections.singleton(partition));
                partitionsAssigned++;
            }
            isAllPartitionsAssigned.set(true);
        });
        Runnable consumerRunnable = () -> {
            try {
                while (!isAllPartitionsAssigned.get()) {
                    consumerTask.maybeWaitForPartitionAssignments();
                    latch.countDown();
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
        };

        ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
        Future<?> future = consumerExecutor.submit(consumerRunnable);
        assignor.start();

        assignor.join();
        future.get();
    }

    @Test
    public void testCanProcessRecord() throws InterruptedException {
        final Uuid topicId = Uuid.fromString("Bp9TDduJRGa9Q5rlvCJOxg");
        final TopicIdPartition tpId0 = new TopicIdPartition(topicId, new TopicPartition("sample", 0));
        final TopicIdPartition tpId1 = new TopicIdPartition(topicId, new TopicPartition("sample", 1));
        final TopicIdPartition tpId2 = new TopicIdPartition(topicId, new TopicPartition("sample", 2));
        assertEquals(partitioner.metadataPartition(tpId0), partitioner.metadataPartition(tpId1));
        assertEquals(partitioner.metadataPartition(tpId0), partitioner.metadataPartition(tpId2));

        final int metadataPartition = partitioner.metadataPartition(tpId0);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), 0L));
        final Set<TopicIdPartition> assignments = Collections.singleton(tpId0);
        consumerTask.addAssignmentsForPartitions(assignments);
        thread.start();
        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId0), "Timed out waiting for " + tpId0 + " to be assigned");

        addRecord(consumer, metadataPartition, tpId0, 0);
        addRecord(consumer, metadataPartition, tpId0, 1);
        TestUtils.waitForCondition(() -> consumerTask.readOffsetForMetadataPartition(metadataPartition).equals(Optional.of(1L)), "Couldn't read record");
        assertEquals(2, handler.metadataCounter);

        // should only read the tpId1 records
        consumerTask.addAssignmentsForPartitions(Collections.singleton(tpId1));
        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId1), "Timed out waiting for " + tpId1 + " to be assigned");
        addRecord(consumer, metadataPartition, tpId1, 2);
        TestUtils.waitForCondition(() -> consumerTask.readOffsetForMetadataPartition(metadataPartition).equals(Optional.of(2L)), "Couldn't read record");
        assertEquals(3, handler.metadataCounter);

        // shouldn't read tpId2 records because it's not assigned
        addRecord(consumer, metadataPartition, tpId2, 3);
        TestUtils.waitForCondition(() -> consumerTask.readOffsetForMetadataPartition(metadataPartition).equals(Optional.of(3L)), "Couldn't read record");
        assertEquals(3, handler.metadataCounter);
    }

    @Test
    public void testMaybeMarkUserPartitionsAsReady() throws InterruptedException {
        final TopicIdPartition tpId = getIdPartitions("hello", 1).get(0);
        final int metadataPartition = partitioner.metadataPartition(tpId);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), 2L));
        consumerTask.addAssignmentsForPartitions(Collections.singleton(tpId));
        thread.start();

        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId), "Waiting for " + tpId + " to be assigned");
        assertTrue(consumerTask.isMetadataPartitionAssigned(metadataPartition));
        assertFalse(handler.isPartitionInitialized.containsKey(tpId));
        IntStream.range(0, 5).forEach(offset -> addRecord(consumer, metadataPartition, tpId, offset));
        TestUtils.waitForCondition(() -> consumerTask.readOffsetForMetadataPartition(metadataPartition).equals(Optional.of(4L)), "Couldn't read record");
        assertTrue(handler.isPartitionInitialized.get(tpId));
    }

    @ParameterizedTest
    @CsvSource(value = {"0, 0", "500, 500"})
    public void testMaybeMarkUserPartitionAsReadyWhenTopicIsEmpty(long beginOffset,
                                                                  long endOffset) throws InterruptedException {
        final TopicIdPartition tpId = getIdPartitions("world", 1).get(0);
        final int metadataPartition = partitioner.metadataPartition(tpId);
        consumer.updateBeginningOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), beginOffset));
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), endOffset));
        consumerTask.addAssignmentsForPartitions(Collections.singleton(tpId));
        thread.start();

        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId), "Waiting for " + tpId + " to be assigned");
        assertTrue(consumerTask.isMetadataPartitionAssigned(metadataPartition));
        TestUtils.waitForCondition(() -> handler.isPartitionInitialized.containsKey(tpId),
            "should have initialized the partition");
        assertFalse(consumerTask.readOffsetForMetadataPartition(metadataPartition).isPresent());
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        thread.start();
        final CountDownLatch latch = new CountDownLatch(1);
        final TopicIdPartition tpId = getIdPartitions("concurrent", 1).get(0);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(partitioner.metadataPartition(tpId)), 0L));
        final Thread assignmentThread = new Thread(() -> {
            try {
                latch.await();
                consumerTask.addAssignmentsForPartitions(Collections.singleton(tpId));
            } catch (final InterruptedException e) {
                fail("Shouldn't have thrown an exception");
            }
        });
        final Thread closeThread = new Thread(() -> {
            try {
                latch.await();
                consumerTask.close();
            } catch (final InterruptedException e) {
                fail("Shouldn't have thrown an exception");
            }
        });
        assignmentThread.start();
        closeThread.start();

        latch.countDown();
        assignmentThread.join();
        closeThread.join();
    }

    @Test
    public void testConsumerShouldNotCloseOnRetriableError() throws InterruptedException {
        final TopicIdPartition tpId = getIdPartitions("world", 1).get(0);
        final int metadataPartition = partitioner.metadataPartition(tpId);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), 1L));
        consumerTask.addAssignmentsForPartitions(Collections.singleton(tpId));
        thread.start();

        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId), "Waiting for " + tpId + " to be assigned");
        assertTrue(consumerTask.isMetadataPartitionAssigned(metadataPartition));

        consumer.setPollException(new LeaderNotAvailableException("leader not available!"));
        addRecord(consumer, metadataPartition, tpId, 0);
        consumer.setPollException(new TimeoutException("Not able to complete the operation within the timeout"));
        addRecord(consumer, metadataPartition, tpId, 1);

        TestUtils.waitForCondition(() -> consumerTask.readOffsetForMetadataPartition(metadataPartition).equals(Optional.of(1L)), "Couldn't read record");
        assertEquals(2, handler.metadataCounter);
    }

    @Test
    public void testConsumerShouldCloseOnNonRetriableError() throws InterruptedException {
        final TopicIdPartition tpId = getIdPartitions("world", 1).get(0);
        final int metadataPartition = partitioner.metadataPartition(tpId);
        consumer.updateEndOffsets(Collections.singletonMap(toRemoteLogPartition(metadataPartition), 1L));
        consumerTask.addAssignmentsForPartitions(Collections.singleton(tpId));
        thread.start();

        TestUtils.waitForCondition(() -> consumerTask.isUserPartitionAssigned(tpId), "Waiting for " + tpId + " to be assigned");
        assertTrue(consumerTask.isMetadataPartitionAssigned(metadataPartition));

        consumer.setPollException(new AuthorizationException("Unauthorized to read the topic!"));
        TestUtils.waitForCondition(() -> consumer.closed(), "Should close the consume on non-retriable error");
    }

    private void addRecord(final MockConsumer<byte[], byte[]> consumer,
                           final int metadataPartition,
                           final TopicIdPartition idPartition,
                           final long recordOffset) {
        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(idPartition, Uuid.randomUuid());
        final RemoteLogMetadata metadata = new RemoteLogSegmentMetadata(segmentId, 0L, 1L, 0L, 0, 0L, 1, Collections.singletonMap(0, 0L));
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME, metadataPartition, recordOffset, null, serde.serialize(metadata));
        consumer.addRecord(record);
    }

    private List<TopicIdPartition> getIdPartitions(final String topic, final int partitionCount) {
        final List<TopicIdPartition> idPartitions = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            idPartitions.add(new TopicIdPartition(topicId, new TopicPartition(topic, partition)));
        }
        return idPartitions;
    }

    private static class DummyEventHandler extends RemotePartitionMetadataEventHandler {
        private int metadataCounter = 0;
        private final Map<TopicIdPartition, Boolean> isPartitionInitialized = new HashMap<>();
        private final Map<TopicIdPartition, Boolean> isPartitionLoaded = new HashMap<>();
        private final Map<TopicIdPartition, Boolean> isPartitionCleared = new HashMap<>();

        @Override
        protected void handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            metadataCounter++;
        }

        @Override
        protected void handleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) {
        }

        @Override
        protected void handleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) {
        }

        @Override
        public void clearTopicPartition(TopicIdPartition topicIdPartition) {
            isPartitionCleared.put(topicIdPartition, true);
        }

        @Override
        public void markInitialized(TopicIdPartition partition) {
            isPartitionInitialized.put(partition, true);
        }

        @Override
        public boolean isInitialized(TopicIdPartition partition) {
            return true;
        }

        @Override
        public void maybeLoadPartition(TopicIdPartition partition) {
            isPartitionLoaded.put(partition, true);
        }
    }
}
