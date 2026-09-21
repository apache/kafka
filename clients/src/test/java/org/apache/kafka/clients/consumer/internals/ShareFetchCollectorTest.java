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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createShareFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link ShareFetchCollector} functionality.
 */
public class ShareFetchCollectorTest {

    private static final int DEFAULT_RECORD_COUNT = 10;
    private static final int DEFAULT_MAX_POLL_RECORDS = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    private static final Optional<Integer> DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS = Optional.of(30000);
    private final TopicIdPartition topicAPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-a");
    private final TopicIdPartition topicAPartition1 = new TopicIdPartition(topicAPartition0.topicId(), 1, "topic-a");
    private LogContext logContext;

    private SubscriptionState subscriptions;
    private ShareFetchConfig shareFetchConfig;
    private ShareConsumerMetadata metadata;
    private ShareFetchBuffer fetchBuffer;
    private Deserializers<String, String> deserializers;
    private ShareFetchCollector<String, String> fetchCollector;
    private ShareCompletedFetchBuilder completedFetchBuilder;
    private ShareFetchMetricsAggregator shareFetchMetricsAggregator;

    @Test
    public void testFetchNormal() {
        int recordCount = DEFAULT_MAX_POLL_RECORDS;
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .recordCount(recordCount)
                .build();

        // Validate that the buffer is empty until after we add the fetch data.
        assertTrue(fetchBuffer.isEmpty());
        fetchBuffer.add(List.of(completedFetch));
        assertFalse(fetchBuffer.isEmpty());

        // Validate that the completed fetch isn't initialized just because we add it to the buffer.
        assertFalse(completedFetch.isInitialized());

        // Fetch the data and validate that we get all the records we want back.
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertFalse(fetch.isEmpty());
        assertEquals(recordCount, fetch.numRecords());

        // When we collected the data from the buffer, this will cause the completed fetch to get initialized.
        assertTrue(completedFetch.isInitialized());

        // However, even though we've collected the data, it isn't (completely) consumed yet.
        assertFalse(completedFetch.isConsumed());

        // The buffer is now considered "empty" because our queue is empty.
        assertTrue(fetchBuffer.isEmpty());
        assertNull(fetchBuffer.peek());
        assertNull(fetchBuffer.poll());

        // However, while the queue is "empty", the next-in-line fetch is actually still in the buffer.
        assertNotNull(fetchBuffer.nextInLineFetch());

        // Now attempt to collect more records from the fetch buffer.
        fetch = fetchCollector.collect(fetchBuffer);
        assertEquals(0, fetch.numRecords());
        assertTrue(fetch.isEmpty());

        // However, once we read *past* the end of the records in the ShareCompletedFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assertTrue(completedFetch.isConsumed());
    }

    @Test
    public void testWithRenew() {
        int recordCount = DEFAULT_MAX_POLL_RECORDS;
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
            .recordCount(recordCount)
            .build();

        // Validate that the buffer is empty until after we add the fetch data.
        assertTrue(fetchBuffer.isEmpty());
        fetchBuffer.add(List.of(completedFetch));
        assertFalse(fetchBuffer.isEmpty());

        // Validate that the completed fetch isn't initialized just because we add it to the buffer.
        assertFalse(completedFetch.isInitialized());

        // Fetch the data and validate that we get all the records we want back.
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertFalse(fetch.isEmpty());
        assertEquals(recordCount, fetch.numRecords());
        assertEquals(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, fetch.acquisitionLockTimeoutMs());

        // When we collected the data from the buffer, this will cause the completed fetch to get initialized.
        assertTrue(completedFetch.isInitialized());

        // However, even though we've collected the data, it isn't (completely) consumed yet.
        assertFalse(completedFetch.isConsumed());

        // The buffer is now considered "empty" because our queue is empty.
        assertTrue(fetchBuffer.isEmpty());
        assertNull(fetchBuffer.peek());
        assertNull(fetchBuffer.poll());

        // However, while the queue is "empty", the next-in-line fetch is actually still in the buffer.
        assertNotNull(fetchBuffer.nextInLineFetch());

        assertEquals(500, fetch.numRecords());
        ConsumerRecord<String, String> record = new ConsumerRecord<>(topicAPartition0.topic(), topicAPartition0.partition(), 0, "", "");
        fetch.acknowledge(record, AcknowledgeType.RENEW);
        assertEquals(DEFAULT_MAX_POLL_RECORDS, fetch.numRecords());
        assertFalse(fetch.hasRenewals());

        Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap = fetch.takeAcknowledgedRecords();
        assertTrue(fetch.hasRenewals());
        assertEquals(DEFAULT_MAX_POLL_RECORDS - 1, fetch.numRecords());

        Acknowledgements acks = acknowledgementsMap.get(topicAPartition0).acknowledgements();
        acks.complete(null);
        fetch.renew(Map.of(topicAPartition0, acks), Optional.of(20000));
        assertTrue(fetch.hasRenewals());
        fetch.takeRenewedRecords();
        assertFalse(fetch.hasRenewals());
        assertEquals(DEFAULT_MAX_POLL_RECORDS, fetch.numRecords());
        assertEquals(Optional.of(20000), fetch.acquisitionLockTimeoutMs());

        // Now attempt to collect more records from the fetch buffer.
        fetch = fetchCollector.collect(fetchBuffer);
        assertEquals(0, fetch.numRecords());
        assertTrue(fetch.isEmpty());

        // However, once we read *past* the end of the records in the ShareCompletedFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assertTrue(completedFetch.isConsumed());
    }

    @Test
    public void testSecondFetchForSamePartitionMergesWithoutLeaking() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        // Two completed fetches arrive for the same partition before either is drained by the application.
        ShareCompletedFetch firstFetch = completedFetchBuilder.baseOffset(0).recordCount(1).build();
        fetchBuffer.add(List.of(firstFetch));
        ShareCompletedFetch secondFetch = completedFetchBuilder.baseOffset(1).recordCount(1).build();
        fetchBuffer.add(List.of(secondFetch));

        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);

        // Both records were delivered to the application, merged into a single batch for the partition.
        assertEquals(2, fetch.numRecords());

        // The application acknowledges everything it was actually given.
        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        fetch.takeAcknowledgedRecords();

        // Nothing should remain buffered.
        assertTrue(fetchBuffer.bufferedPartitions().isEmpty());
        assertTrue(fetchBuffer.bufferedNodes().isEmpty());
    }

    @Test
    public void testEmptyFetchFollowedByDataFetchForSamePartitionDoesNotLeak() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch emptyFetch = completedFetchBuilder.recordCount(0).build();
        ShareCompletedFetch dataFetch = completedFetchBuilder.baseOffset(0).recordCount(DEFAULT_RECORD_COUNT).build();
        fetchBuffer.add(List.of(emptyFetch, dataFetch));

        assertEquals(DEFAULT_RECORD_COUNT, drainAndAcknowledge());

        // Every delivered record has been acknowledged, so the fetch that carried them must not report
        // pending acknowledgements, and the buffer must not retain it once it stops being next-in-line.
        assertFalse(dataFetch.hasPendingAcknowledgements());
        fetchBuffer.add(List.of(completedFetchBuilder.recordCount(0).build()));
        assertEquals(0, drainAndAcknowledge());
        assertTrue(fetchBuffer.bufferedPartitions().isEmpty());
        assertTrue(fetchBuffer.bufferedNodes().isEmpty());
    }

    @Test
    public void testRepeatedFetchesForSamePartitionDoNoAccumulateRetainedFetches() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        // Repeat empty-then-data pattern, acknowledging all each time. If a completed fetch were retained on each
        // iteration, the buffer would still consider the node buffered at the end.
        for (int i = 0; i < 50; i++) {
            fetchBuffer.add(List.of(
                completedFetchBuilder.recordCount(0).build(),
                completedFetchBuilder.baseOffset((long) i * DEFAULT_RECORD_COUNT).recordCount(DEFAULT_RECORD_COUNT).build()));
            assertEquals(DEFAULT_RECORD_COUNT, drainAndAcknowledge());
        }

        fetchBuffer.add(List.of(completedFetchBuilder.recordCount(0).build()));
        assertEquals(0, drainAndAcknowledge());
        assertTrue(fetchBuffer.bufferedPartitions().isEmpty());
        assertTrue(fetchBuffer.bufferedNodes().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("testErrorInInitializeSource")
    public void testErrorInInitializeDoesNotLoseRecordsFromEarlierPartitions(RuntimeException expectedException) {
        buildDependencies();
        subscriptions.subscribe(Set.of(topicAPartition0.topic()), Optional.empty());
        subscriptions.assignFromSubscribed(Set.of(topicAPartition0.topicPartition(), topicAPartition1.topicPartition()));

        // Create a ShareFetchCollector that fails on ShareCompletedFetch initialization for partition 1 only.
        fetchCollector = new ShareFetchCollector<>(logContext,
                metadata,
                subscriptions,
                shareFetchConfig,
                deserializers) {

            @Override
            protected ShareCompletedFetch initialize(final ShareCompletedFetch completedFetch) {
                if (completedFetch.partition.equals(topicAPartition1)) {
                    throw expectedException;
                }
                return super.initialize(completedFetch);
            }
        };

        // The failure is only discovered once partition 0's records have already been drained.
        ShareCompletedFetch goodFetch = completedFetchBuilder.recordCount(2).build();
        ShareCompletedFetch badFetch = completedFetchBuilder.partition(topicAPartition1).build();
        fetchBuffer.add(List.of(goodFetch, badFetch));

        // The records already collected must be returned, not dropped on the floor.
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertEquals(2, fetch.numRecords());
        assertEquals(2, fetch.records().get(topicAPartition0.topicPartition()).size());

        // The failing fetch stays at the head of the queue and is surfaced on the next collect.
        assertEquals(badFetch, fetchBuffer.peek());
        assertThrows(expectedException.getClass(), () -> fetchCollector.collect(fetchBuffer));
        assertTrue(fetchBuffer.isEmpty());

        // Acknowledging the records that were delivered releases the completed fetch which carried them.
        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        fetch.takeAcknowledgedRecords();
        assertTrue(fetchBuffer.bufferedPartitions().isEmpty());
        assertTrue(fetchBuffer.bufferedNodes().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("testErrorInInitializeSource")
    public void testErrorInInitialize(RuntimeException expectedException) {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        // Create a ShareFetchCollector that fails on ShareCompletedFetch initialization.
        fetchCollector = new ShareFetchCollector<>(logContext,
                metadata,
                subscriptions,
                shareFetchConfig,
                deserializers) {

            @Override
            protected ShareCompletedFetch initialize(final ShareCompletedFetch completedFetch) {
                throw expectedException;
            }
        };

        // Add the ShareCompletedFetch to the ShareFetchBuffer queue - the number of records doesn't matter
        ShareCompletedFetch completedFetch = completedFetchBuilder
                .recordCount(10)
                .build();
        fetchBuffer.add(List.of(completedFetch));

        // At first, the queue is populated
        assertFalse(fetchBuffer.isEmpty());

        // Now run our ill-fated collectFetch.
        assertThrows(expectedException.getClass(), () -> fetchCollector.collect(fetchBuffer));
    }

    @Test
    public void testFetchWithTopicAuthorizationFailed() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.TOPIC_AUTHORIZATION_FAILED)
                .build();
        fetchBuffer.add(List.of(completedFetch));
        assertThrows(TopicAuthorizationException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    @Test
    public void testFetchWithUnknownLeaderEpoch() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_LEADER_EPOCH)
                .build();
        fetchBuffer.add(List.of(completedFetch));
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithUnknownServerError() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_SERVER_ERROR)
                .build();
        fetchBuffer.add(List.of(completedFetch));
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithCorruptMessage() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.CORRUPT_MESSAGE)
                .build();
        fetchBuffer.add(List.of(completedFetch));
        assertThrows(KafkaException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    @ParameterizedTest
    @MethodSource("testFetchWithOtherErrorsSource")
    public void testFetchWithOtherErrors(final Errors error) {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .error(error)
                .build();
        fetchBuffer.add(List.of(completedFetch));
        assertThrows(KafkaException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    private void buildDependencies() {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_RECORDS));

        ConsumerConfig config = new ConsumerConfig(p);

        Metrics metrics = createMetrics(config, Time.SYSTEM);
        deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer(), metrics);
        ShareFetchMetricsManager shareFetchMetricsManager = createShareFetchMetricsManager(metrics);
        Set<TopicPartition> partitionSet = new HashSet<>();
        partitionSet.add(topicAPartition0.topicPartition());
        shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(shareFetchMetricsManager, partitionSet);

        subscriptions = createSubscriptionState(config, logContext);
        shareFetchConfig = new ShareFetchConfig(config);

        metadata = new ShareConsumerMetadata(
                0,
                1000,
                10000,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        fetchCollector = new ShareFetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                shareFetchConfig,
                deserializers);
        fetchBuffer = new ShareFetchBuffer(logContext);
        completedFetchBuilder = new ShareCompletedFetchBuilder();
    }

    private void subscribeAndAssign(TopicIdPartition tp) {
        subscriptions.subscribe(Set.of(tp.topic()), Optional.empty());
        subscriptions.assignFromSubscribed(Set.of(tp.topicPartition()));
    }

    private int drainAndAcknowledge() {
        int delivered = 0;
        for (int i = 0; i < 20; i++) {
            ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
            delivered += fetch.numRecords();
            fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
            fetch.takeAcknowledgedRecords();
            assertTrue(fetch.isEmpty());
            // The background thread's poll() prunes retained fetches which no longer have pending acknowledgements.
            fetchBuffer.bufferedNodes();
            ShareCompletedFetch nextInLine = fetchBuffer.nextInLineFetch();
            if (fetchBuffer.isEmpty() && (nextInLine == null || nextInLine.isConsumed())) {
                break;
            }
        }
        assertTrue(fetchBuffer.isEmpty());
        return delivered;
    }

    /**
     * Supplies the {@link Arguments} to {@link #testFetchWithOtherErrors(Errors)}.
     */
    private static Stream<Arguments> testFetchWithOtherErrorsSource() {
        List<Errors> errors = new ArrayList<>(Arrays.asList(Errors.values()));
        errors.removeAll(Arrays.asList(
                Errors.NONE,
                Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.REPLICA_NOT_AVAILABLE,
                Errors.KAFKA_STORAGE_ERROR,
                Errors.FENCED_LEADER_EPOCH,
                Errors.OFFSET_NOT_AVAILABLE,
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_ID,
                Errors.INCONSISTENT_TOPIC_ID,
                Errors.OFFSET_OUT_OF_RANGE,
                Errors.TOPIC_AUTHORIZATION_FAILED,
                Errors.UNKNOWN_LEADER_EPOCH,
                Errors.UNKNOWN_SERVER_ERROR,
                Errors.CORRUPT_MESSAGE
        ));

        return errors.stream().map(Arguments::of);
    }


    /**
     * Supplies the {@link Arguments} to {@link #testErrorInInitialize(RuntimeException)}.
     */
    private static Stream<Arguments> testErrorInInitializeSource() {
        return Stream.of(
                Arguments.of(new KafkaException()),
                Arguments.of(new TopicAuthorizationException(Set.of("topic-a")))
        );
    }

    private class ShareCompletedFetchBuilder {

        private int recordCount = DEFAULT_RECORD_COUNT;

        private long baseOffset = 0L;

        private Errors error = null;

        private TopicIdPartition partition = topicAPartition0;

        private ShareCompletedFetchBuilder partition(TopicIdPartition partition) {
            this.partition = partition;
            return this;
        }

        private ShareCompletedFetchBuilder recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        private ShareCompletedFetchBuilder baseOffset(long baseOffset) {
            this.baseOffset = baseOffset;
            return this;
        }

        private ShareCompletedFetchBuilder error(Errors error) {
            this.error = error;
            return this;
        }

        private ShareCompletedFetch build() {
            Records records;
            ByteBuffer allocate = ByteBuffer.allocate(1024);

            try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                    Compression.NONE,
                    TimestampType.CREATE_TIME,
                    baseOffset)) {
                for (int i = 0; i < recordCount; i++)
                    builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

                records = builder.build();
            }

            // A response which acquired no records carries no AcquiredRecords entries at all.
            List<ShareFetchResponseData.AcquiredRecords> acquiredRecords = recordCount == 0
                ? List.of()
                : ShareCompletedFetchTest.acquiredRecords(baseOffset, recordCount);

            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(partition.partition())
                    .setRecords(records)
                    .setAcquiredRecords(acquiredRecords);

            if (error != null)
                partitionData.setErrorCode(error.code());

            return new ShareCompletedFetch(
                    logContext,
                    BufferSupplier.create(),
                    0,
                    partition,
                    partitionData,
                    DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
                    shareFetchMetricsAggregator,
                    ApiKeys.SHARE_FETCH.latestVersion());
        }
    }
}