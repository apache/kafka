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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link FetchCollector} functionality in addition to what {@link FetcherTest} tests during the course
 * of its tests.
 */
public class FetchCollectorTest {

    private final static int DEFAULT_RECORD_COUNT = 10;
    private final static int DEFAULT_MAX_POLL_RECORDS = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    private final Time time = new MockTime(0, 0, 0);
    private final TopicPartition topicAPartition0 = new TopicPartition("topic-a", 0);
    private final TopicPartition topicAPartition1 = new TopicPartition("topic-a", 1);
    private final TopicPartition topicAPartition2 = new TopicPartition("topic-a", 2);
    private final Set<TopicPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private LogContext logContext;

    private SubscriptionState subscriptions;
    private FetchConfig fetchConfig;
    private FetchMetricsManager metricsManager;
    private ConsumerMetadata metadata;
    private FetchBuffer fetchBuffer;
    private Deserializers<String, String> deserializers;
    private FetchCollector<String, String> fetchCollector;
    private CompletedFetchBuilder completedFetchBuilder;

    @Test
    public void testFetchNormal() {
        int recordCount = DEFAULT_MAX_POLL_RECORDS;
        buildDependencies();
        assignAndSeek(topicAPartition0);

        CompletedFetch completedFetch = completedFetchBuilder
                .recordCount(recordCount)
                .build();

        // Validate that the buffer is empty until after we add the fetch data.
        assertTrue(fetchBuffer.isEmpty());
        fetchBuffer.add(completedFetch);
        assertFalse(fetchBuffer.isEmpty());

        // Validate that the completed fetch isn't initialized just because we add it to the buffer.
        assertFalse(completedFetch.isInitialized());

        // Fetch the data and validate that we get all the records we want back.
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
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

        // Validate that the next fetch position has been updated to point to the record after our last fetched
        // record.
        SubscriptionState.FetchPosition position = subscriptions.position(topicAPartition0);
        assertEquals(recordCount, position.offset);

        // Now attempt to collect more records from the fetch buffer.
        fetch = fetchCollector.collectFetch(fetchBuffer);

        // The Fetch object is non-null, but it's empty.
        assertEquals(0, fetch.numRecords());
        assertTrue(fetch.isEmpty());

        // However, once we read *past* the end of the records in the CompletedFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assertTrue(completedFetch.isConsumed());
    }

    @Test
    public void testFetchWithReadReplica() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // Set the preferred read replica and just to be safe, verify it was set.
        int preferredReadReplicaId = 67;
        subscriptions.updatePreferredReadReplica(topicAPartition0, preferredReadReplicaId, time::milliseconds);
        assertNotNull(subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));
        assertEquals(Optional.of(preferredReadReplicaId), subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));

        CompletedFetch completedFetch = completedFetchBuilder.build();
        fetchBuffer.add(completedFetch);
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);

        // The Fetch and read replica settings should be empty.
        assertEquals(DEFAULT_RECORD_COUNT, fetch.numRecords());
        assertEquals(Optional.of(preferredReadReplicaId), subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));
    }

    @Test
    public void testNoResultsIfInitializing() {
        buildDependencies();

        // Intentionally call assign (vs. assignAndSeek) so that we don't set the position. The SubscriptionState
        // will consider the partition as in the SubscriptionState.FetchStates.INITIALIZED state.
        assign(topicAPartition0);

        // The position should thus be null and considered un-fetchable and invalid.
        assertNull(subscriptions.position(topicAPartition0));
        assertFalse(subscriptions.isFetchable(topicAPartition0));
        assertFalse(subscriptions.hasValidPosition(topicAPartition0));

        // Add some valid CompletedFetch records to the FetchBuffer queue and collect them into the Fetch.
        CompletedFetch completedFetch = completedFetchBuilder.build();
        fetchBuffer.add(completedFetch);
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);

        // Verify that no records are fetched for the partition as it did not have a valid position set.
        assertEquals(0, fetch.numRecords());
    }

    @ParameterizedTest
    @MethodSource("testErrorInInitializeSource")
    public void testErrorInInitialize(int recordCount, RuntimeException expectedException) {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // Create a FetchCollector that fails on CompletedFetch initialization.
        fetchCollector = new FetchCollector<String, String>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers,
                metricsManager,
                time) {

            @Override
            protected CompletedFetch initialize(final CompletedFetch completedFetch) {
                throw expectedException;
            }
        };

        // Add the CompletedFetch to the FetchBuffer queue
        CompletedFetch completedFetch = completedFetchBuilder
                .recordCount(recordCount)
                .build();
        fetchBuffer.add(completedFetch);

        // At first, the queue is populated
        assertFalse(fetchBuffer.isEmpty());

        // Now run our ill-fated collectFetch.
        assertThrows(expectedException.getClass(), () -> fetchCollector.collectFetch(fetchBuffer));

        // If the number of records in the CompletedFetch was 0, the call to FetchCollector.collectFetch() will
        // remove it from the queue. If there are records in the CompletedFetch, FetchCollector.collectFetch will
        // leave it on the queue.
        assertEquals(recordCount == 0, fetchBuffer.isEmpty());
    }

    @Test
    public void testFetchingPausedPartitionsYieldsNoRecords() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // The partition should not be 'paused' in the SubscriptionState until we explicitly tell it to.
        assertFalse(subscriptions.isPaused(topicAPartition0));
        subscriptions.pause(topicAPartition0);
        assertTrue(subscriptions.isPaused(topicAPartition0));

        CompletedFetch completedFetch = completedFetchBuilder.build();

        // Set the CompletedFetch to the next-in-line fetch, *not* the queue.
        fetchBuffer.setNextInLineFetch(completedFetch);

        // The next-in-line CompletedFetch should reference the same object that was just created
        assertSame(fetchBuffer.nextInLineFetch(), completedFetch);

        // The FetchBuffer queue should be empty as the CompletedFetch was added to the next-in-line.
        // CompletedFetch, not the queue.
        assertTrue(fetchBuffer.isEmpty());

        // Ensure that the partition for the next-in-line CompletedFetch is still 'paused'.
        assertTrue(subscriptions.isPaused(completedFetch.partition));

        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);

        // There should be no records in the Fetch as the partition being fetched is 'paused'.
        assertEquals(0, fetch.numRecords());

        // The FetchBuffer queue should not be empty; the CompletedFetch is added to the FetchBuffer queue by
        // the FetchCollector when it detects a 'paused' partition.
        assertFalse(fetchBuffer.isEmpty());

        // The next-in-line CompletedFetch should be null; the CompletedFetch is added to the FetchBuffer
        // queue by the FetchCollector when it detects a 'paused' partition.
        assertNull(fetchBuffer.nextInLineFetch());
    }

    @ParameterizedTest
    @MethodSource("testFetchWithMetadataRefreshErrorsSource")
    public void testFetchWithMetadataRefreshErrors(final Errors error) {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        CompletedFetch completedFetch = completedFetchBuilder
                .error(error)
                .build();
        fetchBuffer.add(completedFetch);

        // Set the preferred read replica and just to be safe, verify it was set.
        int preferredReadReplicaId = 5;
        subscriptions.updatePreferredReadReplica(topicAPartition0, preferredReadReplicaId, time::milliseconds);
        assertNotNull(subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));
        assertEquals(Optional.of(preferredReadReplicaId), subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));

        // Fetch the data and validate that we get all the records we want back.
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        assertTrue(fetch.isEmpty());
        assertTrue(metadata.updateRequested());
        assertEquals(Optional.empty(), subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));
    }

    @Test
    public void testFetchWithOffsetOutOfRange() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        CompletedFetch completedFetch = completedFetchBuilder.build();
        fetchBuffer.add(completedFetch);

        // Fetch the data and validate that we get our first batch of records back.
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        assertFalse(fetch.isEmpty());
        assertEquals(DEFAULT_RECORD_COUNT, fetch.numRecords());

        // Try to fetch more data and validate that we get an empty Fetch back.
        completedFetch = completedFetchBuilder
                .fetchOffset(fetch.numRecords())
                .error(Errors.OFFSET_OUT_OF_RANGE)
                .build();
        fetchBuffer.add(completedFetch);
        fetch = fetchCollector.collectFetch(fetchBuffer);
        assertTrue(fetch.isEmpty());

        // Try to fetch more data and validate that we get an empty Fetch back.
        completedFetch = completedFetchBuilder
                .fetchOffset(fetch.numRecords())
                .error(Errors.OFFSET_OUT_OF_RANGE)
                .build();
        fetchBuffer.add(completedFetch);
        fetch = fetchCollector.collectFetch(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithOffsetOutOfRangeWithPreferredReadReplica() {
        int records = 10;
        buildDependencies(records);
        assignAndSeek(topicAPartition0);

        // Set the preferred read replica and just to be safe, verify it was set.
        int preferredReadReplicaId = 67;
        subscriptions.updatePreferredReadReplica(topicAPartition0, preferredReadReplicaId, time::milliseconds);
        assertNotNull(subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));
        assertEquals(Optional.of(preferredReadReplicaId), subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));

        CompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.OFFSET_OUT_OF_RANGE)
                .build();
        fetchBuffer.add(completedFetch);
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);

        // The Fetch and read replica settings should be empty.
        assertTrue(fetch.isEmpty());
        assertEquals(Optional.empty(), subscriptions.preferredReadReplica(topicAPartition0, time.milliseconds()));
    }

    @Test
    public void testFetchWithTopicAuthorizationFailed() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.TOPIC_AUTHORIZATION_FAILED)
                .build();
        fetchBuffer.add(completedFetch);
        assertThrows(TopicAuthorizationException.class, () -> fetchCollector.collectFetch(fetchBuffer));
    }

    @Test
    public void testFetchWithUnknownLeaderEpoch() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_LEADER_EPOCH)
                .build();
        fetchBuffer.add(completedFetch);
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithUnknownServerError() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_SERVER_ERROR)
                .build();
        fetchBuffer.add(completedFetch);
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithCorruptMessage() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.CORRUPT_MESSAGE)
                .build();
        fetchBuffer.add(completedFetch);
        assertThrows(KafkaException.class, () -> fetchCollector.collectFetch(fetchBuffer));
    }

    @ParameterizedTest
    @MethodSource("testFetchWithOtherErrorsSource")
    public void testFetchWithOtherErrors(final Errors error) {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        CompletedFetch completedFetch = completedFetchBuilder
                .error(error)
                .build();
        fetchBuffer.add(completedFetch);
        assertThrows(IllegalStateException.class, () -> fetchCollector.collectFetch(fetchBuffer));
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicPartition> partitions(TopicPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }

    private void buildDependencies() {
        buildDependencies(DEFAULT_MAX_POLL_RECORDS);
    }

    private void buildDependencies(int maxPollRecords) {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));

        ConsumerConfig config = new ConsumerConfig(p);

        deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer());

        subscriptions = createSubscriptionState(config, logContext);
        fetchConfig = new FetchConfig(config);

        Metrics metrics = createMetrics(config, time);
        metricsManager = createFetchMetricsManager(metrics);
        metadata = new ConsumerMetadata(
                0,
                1000,
                10000,
                false,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        fetchCollector = new FetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers,
                metricsManager,
                time);
        fetchBuffer = new FetchBuffer(logContext);
        completedFetchBuilder = new CompletedFetchBuilder();
    }

    private void assign(TopicPartition... partitions) {
        subscriptions.assignFromUser(partitions(partitions));
    }

    private void assignAndSeek(TopicPartition tp) {
        assign(tp);
        subscriptions.seek(tp, 0);
    }

    /**
     * Supplies the {@link Arguments} to {@link #testFetchWithMetadataRefreshErrors(Errors)}.
     */
    private static Stream<Arguments> testFetchWithMetadataRefreshErrorsSource() {
        List<Errors> errors = Arrays.asList(
                Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.REPLICA_NOT_AVAILABLE,
                Errors.KAFKA_STORAGE_ERROR,
                Errors.FENCED_LEADER_EPOCH,
                Errors.OFFSET_NOT_AVAILABLE,
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_ID,
                Errors.INCONSISTENT_TOPIC_ID
        );

        return errors.stream().map(Arguments::of);
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
     * Supplies the {@link Arguments} to {@link #testErrorInInitialize(int, RuntimeException)}.
     */
    private static Stream<Arguments> testErrorInInitializeSource() {
        return Stream.of(
                Arguments.of(10, new RuntimeException()),
                Arguments.of(0, new RuntimeException()),
                Arguments.of(10, new KafkaException()),
                Arguments.of(0, new KafkaException())
        );
    }

    private class CompletedFetchBuilder {

        private long fetchOffset = 0;

        private int recordCount = DEFAULT_RECORD_COUNT;

        private Errors error = null;

        private CompletedFetchBuilder fetchOffset(long fetchOffset) {
            this.fetchOffset = fetchOffset;
            return this;
        }

        private CompletedFetchBuilder recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        private CompletedFetchBuilder error(Errors error) {
            this.error = error;
            return this;
        }

        private CompletedFetch build() {
            Records records;
            ByteBuffer allocate = ByteBuffer.allocate(1024);

            try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                    CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    0)) {
                for (int i = 0; i < recordCount; i++)
                    builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

                records = builder.build();
            }

            FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(topicAPartition0.partition())
                    .setHighWatermark(1000)
                    .setRecords(records);

            if (error != null)
                partitionData.setErrorCode(error.code());

            FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
            return new CompletedFetch(
                    logContext,
                    subscriptions,
                    BufferSupplier.create(),
                    topicAPartition0,
                    partitionData,
                    metricsAggregator,
                    fetchOffset,
                    ApiKeys.FETCH.latestVersion());
        }
    }
}