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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    private final TopicIdPartition topicAPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-a");
    private LogContext logContext;

    private SubscriptionState subscriptions;
    private FetchConfig fetchConfig;
    private ConsumerMetadata metadata;
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
        fetchBuffer.add(completedFetch);
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

    @ParameterizedTest
    @MethodSource("testErrorInInitializeSource")
    public void testErrorInInitialize(RuntimeException expectedException) {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        // Create a ShareFetchCollector that fails on ShareCompletedFetch initialization.
        fetchCollector = new ShareFetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
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
        fetchBuffer.add(completedFetch);

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
        fetchBuffer.add(completedFetch);
        assertThrows(TopicAuthorizationException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    @Test
    public void testFetchWithUnknownLeaderEpoch() {
        buildDependencies();
        subscribeAndAssign(topicAPartition0);

        ShareCompletedFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_LEADER_EPOCH)
                .build();
        fetchBuffer.add(completedFetch);
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
        fetchBuffer.add(completedFetch);
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
        fetchBuffer.add(completedFetch);
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
        fetchBuffer.add(completedFetch);
        assertThrows(IllegalStateException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    private void buildDependencies() {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_RECORDS));

        ConsumerConfig config = new ConsumerConfig(p);

        deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer());
        Metrics metrics = createMetrics(config, Time.SYSTEM);
        ShareFetchMetricsManager shareFetchMetricsManager = createShareFetchMetricsManager(metrics);
        Set<TopicPartition> partitionSet = new HashSet<>();
        partitionSet.add(topicAPartition0.topicPartition());
        shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(shareFetchMetricsManager, partitionSet);

        subscriptions = createSubscriptionState(config, logContext);
        fetchConfig = new FetchConfig(config);

        metadata = new ConsumerMetadata(
                0,
                1000,
                10000,
                false,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        fetchCollector = new ShareFetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers);
        fetchBuffer = new ShareFetchBuffer(logContext);
        completedFetchBuilder = new ShareCompletedFetchBuilder();
    }

    private void subscribeAndAssign(TopicIdPartition tp) {
        subscriptions.subscribe(Collections.singleton(tp.topic()), Optional.empty());
        subscriptions.assignFromSubscribed(Collections.singleton(tp.topicPartition()));
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
                Arguments.of(new RuntimeException()),
                Arguments.of(new KafkaException())
        );
    }

    private class ShareCompletedFetchBuilder {

        private int recordCount = DEFAULT_RECORD_COUNT;

        private Errors error = null;

        private ShareCompletedFetchBuilder recordCount(int recordCount) {
            this.recordCount = recordCount;
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
                    0)) {
                for (int i = 0; i < recordCount; i++)
                    builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

                records = builder.build();
            }

            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(topicAPartition0.partition())
                    .setRecords(records)
                    .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(0L, recordCount));

            if (error != null)
                partitionData.setErrorCode(error.code());

            return new ShareCompletedFetch(
                    logContext,
                    BufferSupplier.create(),
                    topicAPartition0,
                    partitionData,
                    shareFetchMetricsAggregator,
                    ApiKeys.SHARE_FETCH.latestVersion());
        }
    }
}