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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ShareFetchResponseData;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

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

    private final static int DEFAULT_RECORD_COUNT = 10;
    private final static int DEFAULT_MAX_POLL_RECORDS = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    private final TopicIdPartition topicAPartition0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic-a");
    private LogContext logContext;

    private SubscriptionState subscriptions;
    private FetchConfig fetchConfig;
    private ConsumerMetadata metadata;
    private ShareFetchBuffer fetchBuffer;
    private Deserializers<String, String> deserializers;
    private ShareFetchCollector<String, String> fetchCollector;
    private CompletedShareFetchBuilder completedFetchBuilder;

    @Test
    public void testFetchNormal() {
        int recordCount = DEFAULT_MAX_POLL_RECORDS;
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        CompletedShareFetch completedFetch = completedFetchBuilder
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

        // However, once we read *past* the end of the records in the CompletedShareFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assertTrue(completedFetch.isConsumed());
    }

    @Test
    public void testNoResultsIfInitializing() {
        buildDependencies();

        // Intentionally call assign (vs. subscribeAndSeek) so that we don't set the position. The SubscriptionState
        // will consider the partition as in the SubscriptionState.FetchStates.INITIALIZED state.
        subscribeAndAssign(topicAPartition0);

        // The position should thus be null and considered un-fetchable and invalid.
        assertNull(subscriptions.position(topicAPartition0.topicPartition()));
        assertFalse(subscriptions.isFetchable(topicAPartition0.topicPartition()));
        assertFalse(subscriptions.hasValidPosition(topicAPartition0.topicPartition()));
    }

    @ParameterizedTest
    @MethodSource("testErrorInInitializeSource")
    public void testErrorInInitialize(int recordCount, RuntimeException expectedException) {
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        // Create a FetchCollector that fails on CompletedFetch initialization.
        fetchCollector = new ShareFetchCollector<String, String>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers) {

            @Override
            protected CompletedShareFetch initialize(final CompletedShareFetch completedFetch) {
                throw expectedException;
            }
        };

        // Add the CompletedFetch to the FetchBuffer queue
        CompletedShareFetch completedFetch = completedFetchBuilder
                .recordCount(recordCount)
                .build();
        fetchBuffer.add(completedFetch);

        // At first, the queue is populated
        assertFalse(fetchBuffer.isEmpty());

        // Now run our ill-fated collectFetch.
        assertThrows(expectedException.getClass(), () -> fetchCollector.collect(fetchBuffer));

        // If the number of records in the CompletedFetch was 0, the call to FetchCollector.collect() will
        // remove it from the queue. If there are records in the CompletedShareFetch, FetchCollector.collect will
        // leave it on the queue.
        assertEquals(recordCount == 0, fetchBuffer.isEmpty());
    }

    @Test
    public void testFetchWithTopicAuthorizationFailed() {
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedShareFetch completedFetch = completedFetchBuilder
                .error(Errors.TOPIC_AUTHORIZATION_FAILED)
                .build();
        fetchBuffer.add(completedFetch);
        assertThrows(TopicAuthorizationException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    @Test
    public void testFetchWithUnknownLeaderEpoch() {
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedShareFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_LEADER_EPOCH)
                .build();
        fetchBuffer.add(completedFetch);
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithUnknownServerError() {
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedShareFetch completedFetch = completedFetchBuilder
                .error(Errors.UNKNOWN_SERVER_ERROR)
                .build();
        fetchBuffer.add(completedFetch);
        ShareFetch<String, String> fetch = fetchCollector.collect(fetchBuffer);
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testFetchWithCorruptMessage() {
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        // Try to data and validate that we get an empty Fetch back.
        CompletedShareFetch completedFetch = completedFetchBuilder
                .error(Errors.CORRUPT_MESSAGE)
                .build();
        fetchBuffer.add(completedFetch);
        assertThrows(KafkaException.class, () -> fetchCollector.collect(fetchBuffer));
    }

    @ParameterizedTest
    @MethodSource("testFetchWithOtherErrorsSource")
    public void testFetchWithOtherErrors(final Errors error) {
        buildDependencies();
        subscribeAndSeek(topicAPartition0);

        CompletedShareFetch completedFetch = completedFetchBuilder
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
        completedFetchBuilder = new CompletedShareFetchBuilder();
    }

    private void subscribeAndAssign(TopicIdPartition tp) {
        subscriptions.subscribe(Collections.singleton(tp.topic()), Optional.empty());
        subscriptions.assignFromSubscribed(Collections.singleton(tp.topicPartition()));
    }

    private void subscribeAndSeek(TopicIdPartition tp) {
        subscriptions.subscribe(Collections.singleton(tp.topic()), Optional.empty());
        subscriptions.assignFromSubscribed(Collections.singleton(tp.topicPartition()));
        subscriptions.seek(tp.topicPartition(), 0);
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

    private class CompletedShareFetchBuilder {

        private int recordCount = DEFAULT_RECORD_COUNT;

        private Errors error = null;

        private CompletedShareFetchBuilder recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        private CompletedShareFetchBuilder error(Errors error) {
            this.error = error;
            return this;
        }

        private CompletedShareFetch build() {
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

            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(topicAPartition0.partition())
                    .setRecords(records);

            if (error != null)
                partitionData.setErrorCode(error.code());

            return new CompletedShareFetch(
                    logContext,
                    BufferSupplier.create(),
                    topicAPartition0,
                    partitionData,
                    ApiKeys.SHARE_FETCH.latestVersion());
        }
    }
}