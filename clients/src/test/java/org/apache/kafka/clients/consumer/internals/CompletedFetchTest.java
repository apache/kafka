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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompletedFetchTest {

    private static final String TOPIC_NAME = "test";
    private static final TopicPartition TP = new TopicPartition(TOPIC_NAME, 0);
    private static final long PRODUCER_ID = 1000L;
    private static final short PRODUCER_EPOCH = 0;

    @Test
    public void testSimple() {
        long fetchOffset = 5;
        int startingOffset = 10;
        int numRecords = 11;        // Records for 10-20
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords, fetchOffset));

        Deserializers<String, String> deserializers = newStringDeserializers();
        FetchConfig fetchConfig = newFetchConfig(IsolationLevel.READ_UNCOMMITTED, true);

        CompletedFetch completedFetch = newCompletedFetch(fetchOffset, partitionData);

        List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
        assertEquals(10, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(10, record.offset());

        records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
        assertEquals(1, records.size());
        record = records.get(0);
        assertEquals(20, record.offset());

        records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
        assertEquals(0, records.size());
    }

    @Test
    public void testAbortedTransactionRecordsRemoved() {
        int numRecords = 10;
        Records rawRecords = newTranscactionalRecords(ControlRecordType.ABORT, numRecords);

        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setRecords(rawRecords)
                .setAbortedTransactions(newAbortedTransactions());

        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            FetchConfig fetchConfig = newFetchConfig(IsolationLevel.READ_COMMITTED, true);
            CompletedFetch completedFetch = newCompletedFetch(0, partitionData);
            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
            assertEquals(0, records.size());

            fetchConfig = newFetchConfig(IsolationLevel.READ_UNCOMMITTED, true);
            completedFetch = newCompletedFetch(0, partitionData);
            records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
            assertEquals(numRecords, records.size());
        }
    }

    @Test
    public void testCommittedTransactionRecordsIncluded() {
        int numRecords = 10;
        Records rawRecords = newTranscactionalRecords(ControlRecordType.COMMIT, numRecords);
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setRecords(rawRecords);
        CompletedFetch completedFetch = newCompletedFetch(0, partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            FetchConfig fetchConfig = newFetchConfig(IsolationLevel.READ_COMMITTED, true);
            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
            assertEquals(10, records.size());
        }
    }

    @Test
    public void testNegativeFetchCount() {
        long fetchOffset = 0;
        int startingOffset = 0;
        int numRecords = 10;
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setRecords(newRecords(startingOffset, numRecords, fetchOffset));

        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            CompletedFetch completedFetch = newCompletedFetch(fetchOffset, partitionData);
            FetchConfig fetchConfig = newFetchConfig(IsolationLevel.READ_UNCOMMITTED, true);

            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(fetchConfig, deserializers, -10);
            assertEquals(0, records.size());
        }
    }

    @Test
    public void testNoRecordsInFetch() {
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(10)
                .setLastStableOffset(20)
                .setLogStartOffset(0);

        CompletedFetch completedFetch = newCompletedFetch(1, partitionData);
        try (final Deserializers<String, String> deserializers = newStringDeserializers()) {
            FetchConfig fetchConfig = newFetchConfig(IsolationLevel.READ_UNCOMMITTED, true);
            List<ConsumerRecord<String, String>> records = completedFetch.fetchRecords(fetchConfig, deserializers, 10);
            assertEquals(0, records.size());
        }
    }

    @Test
    public void testCorruptedMessage() {
        // Create one good record and then one "corrupted" record.
        try (final MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 0);
             final UUIDSerializer serializer = new UUIDSerializer()) {
            builder.append(new SimpleRecord(serializer.serialize(TOPIC_NAME, UUID.randomUUID())));
            builder.append(0L, "key".getBytes(), "value".getBytes());
            builder.append(new SimpleRecord(serializer.serialize(TOPIC_NAME, UUID.randomUUID())));
            Headers headers = new RecordHeaders();
            headers.add("hkey", "hvalue".getBytes());
            builder.append(10L, serializer.serialize("key", UUID.randomUUID()), "otherValue".getBytes(), headers.toArray());
            Records records = builder.build();

            FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(0)
                    .setHighWatermark(10)
                    .setLastStableOffset(20)
                    .setLogStartOffset(0)
                    .setRecords(records);

            try (final Deserializers<UUID, UUID> deserializers = newUuidDeserializers()) {
                FetchConfig fetchConfig = newFetchConfig(IsolationLevel.READ_COMMITTED, false);
                CompletedFetch completedFetch = newCompletedFetch(0, partitionData);

                completedFetch.fetchRecords(fetchConfig, deserializers, 10);

                RecordDeserializationException thrown = assertThrows(RecordDeserializationException.class,
                        () -> completedFetch.fetchRecords(fetchConfig, deserializers, 10));
                assertEquals(RecordDeserializationException.DeserializationExceptionOrigin.KEY, thrown.origin());
                assertEquals(1, thrown.offset());
                assertEquals(TOPIC_NAME, thrown.topicPartition().topic());
                assertEquals(0, thrown.topicPartition().partition());
                assertEquals(0, thrown.timestamp());
                assertArrayEquals("key".getBytes(), Utils.toNullableArray(thrown.keyBuffer()));
                assertArrayEquals("value".getBytes(), Utils.toNullableArray(thrown.valueBuffer()));
                assertEquals(0, thrown.headers().toArray().length);

                CompletedFetch completedFetch2 = newCompletedFetch(2, partitionData);
                completedFetch2.fetchRecords(fetchConfig, deserializers, 10);
                RecordDeserializationException valueThrown = assertThrows(RecordDeserializationException.class,
                        () -> completedFetch2.fetchRecords(fetchConfig, deserializers, 10));
                assertEquals(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, valueThrown.origin());
                assertEquals(3, valueThrown.offset());
                assertEquals(TOPIC_NAME, valueThrown.topicPartition().topic());
                assertEquals(0, valueThrown.topicPartition().partition());
                assertEquals(10L, valueThrown.timestamp());
                assertNotNull(valueThrown.keyBuffer());
                assertArrayEquals("otherValue".getBytes(), Utils.toNullableArray(valueThrown.valueBuffer()));
                assertEquals(headers, valueThrown.headers());
            }
        }
    }

    private CompletedFetch newCompletedFetch(long fetchOffset,
                                             FetchResponseData.PartitionData partitionData) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.NONE);
        FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry();
        FetchMetricsManager metrics = new FetchMetricsManager(new Metrics(), metricsRegistry);
        FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metrics, Collections.singleton(TP));

        return new CompletedFetch(
                logContext,
                subscriptions,
                BufferSupplier.create(),
                TP,
                partitionData,
                metricAggregator,
                fetchOffset,
                ApiKeys.FETCH.latestVersion());
    }

    private static Deserializers<UUID, UUID> newUuidDeserializers() {
        return new Deserializers<>(new UUIDDeserializer(), new UUIDDeserializer());
    }

    private static Deserializers<String, String> newStringDeserializers() {
        return new Deserializers<>(new StringDeserializer(), new StringDeserializer());
    }

    private static FetchConfig newFetchConfig(IsolationLevel isolationLevel, boolean checkCrcs) {
        return new FetchConfig(
                ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
                checkCrcs,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                isolationLevel
        );
    }

    private Records newRecords(long baseOffset, int count, long firstMessageId) {
        try (final MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, baseOffset)) {
            for (int i = 0; i < count; i++)
                builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
            return builder.build();
        }
    }

    private Records newTranscactionalRecords(ControlRecordType controlRecordType, int numRecords) {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0,
                time.milliseconds(),
                PRODUCER_ID,
                PRODUCER_EPOCH,
                0,
                true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            for (int i = 0; i < numRecords; i++)
                builder.append(new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

            builder.build();
        }

        writeTransactionMarker(buffer, controlRecordType, numRecords, time);
        buffer.flip();

        return MemoryRecords.readableRecords(buffer);
    }

    private void writeTransactionMarker(ByteBuffer buffer,
                                        ControlRecordType controlRecordType,
                                        int offset,
                                        Time time) {
        MemoryRecords.writeEndTransactionalMarker(buffer,
                offset,
                time.milliseconds(),
                0,
                PRODUCER_ID,
                PRODUCER_EPOCH,
                new EndTransactionMarker(controlRecordType, 0));
    }

    private List<FetchResponseData.AbortedTransaction> newAbortedTransactions() {
        FetchResponseData.AbortedTransaction abortedTransaction = new FetchResponseData.AbortedTransaction();
        abortedTransaction.setFirstOffset(0);
        abortedTransaction.setProducerId(PRODUCER_ID);
        return Collections.singletonList(abortedTransaction);
    }
}
