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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.errors.StreamsException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.kafka.streams.processor.internals.ClientUtils.consumerRecordSizeInBytes;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchCommittedOffsets;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchEndOffsets;
import static org.apache.kafka.streams.processor.internals.ClientUtils.producerRecordSizeInBytes;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ClientUtilsTest {

    // consumer and producer records use utf8 encoding for topic name, header keys, etc
    private static final String TOPIC = "topic";
    private static final int TOPIC_BYTES = 5;

    private static final byte[] KEY = "key".getBytes();
    private static final int KEY_BYTES = 3;

    private static final byte[] VALUE = "value".getBytes();
    private static final int VALUE_BYTES = 5;

    private static final Headers HEADERS = new RecordHeaders(asList(
        new RecordHeader("h1", "headerVal1".getBytes()),   // 2 + 10 --> 12 bytes
        new RecordHeader("h2", "headerVal2".getBytes())    // 2 + 10 --> 12 bytes
    ));
    private static final int HEADERS_BYTES = 24;

    // 20 bytes
    private static final int RECORD_METADATA_BYTES =
        8 + // timestamp
        8 + // offset
        4;  // partition

    // 57 bytes
    private static final long SIZE_IN_BYTES =
        KEY_BYTES +
        VALUE_BYTES +
        TOPIC_BYTES +
        HEADERS_BYTES +
        RECORD_METADATA_BYTES;

    // 54 bytes
    private static final long NULL_KEY_SIZE_IN_BYTES =
        VALUE_BYTES +
        TOPIC_BYTES +
        HEADERS_BYTES +
        RECORD_METADATA_BYTES;

    // 52 bytes
    private static final long TOMBSTONE_SIZE_IN_BYTES =
        KEY_BYTES +
        TOPIC_BYTES +
        HEADERS_BYTES +
        RECORD_METADATA_BYTES;

    private static final Set<TopicPartition> PARTITIONS = Set.of(
        new TopicPartition(TOPIC, 1),
        new TopicPartition(TOPIC, 2)
    );

    @Test
    public void fetchCommittedOffsetsShouldRethrowKafkaExceptionAsStreamsException() {
        @SuppressWarnings("unchecked")
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.committed(PARTITIONS)).thenThrow(new KafkaException());
        assertThrows(StreamsException.class, () -> fetchCommittedOffsets(PARTITIONS, consumer));
    }

    @Test
    public void fetchCommittedOffsetsShouldRethrowTimeoutException() {
        @SuppressWarnings("unchecked")
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.committed(PARTITIONS)).thenThrow(new TimeoutException());
        assertThrows(TimeoutException.class, () -> fetchCommittedOffsets(PARTITIONS, consumer));
    }

    @Test
    public void fetchCommittedOffsetsShouldReturnEmptyMapIfPartitionsAreEmpty() {
        @SuppressWarnings("unchecked")
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        assertTrue(fetchCommittedOffsets(emptySet(), consumer).isEmpty());
    }

    @Test
    public void fetchEndOffsetsShouldReturnEmptyMapIfPartitionsAreEmpty() {
        final Admin adminClient = mock(AdminClient.class);
        assertTrue(fetchEndOffsets(emptySet(), adminClient).isEmpty());
    }

    @Test
    public void fetchEndOffsetsShouldRethrowRuntimeExceptionAsStreamsException() throws Exception {
        final Admin adminClient = mock(AdminClient.class);
        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        @SuppressWarnings("unchecked")
        final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = mock(KafkaFuture.class);

        when(adminClient.listOffsets(any())).thenReturn(result);
        when(result.all()).thenReturn(allFuture);
        when(allFuture.get()).thenThrow(new RuntimeException());

        assertThrows(StreamsException.class, () -> fetchEndOffsets(PARTITIONS, adminClient));
    }

    @Test
    public void fetchEndOffsetsShouldRethrowInterruptedExceptionAsStreamsException() throws Exception {
        final Admin adminClient = mock(AdminClient.class);
        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        @SuppressWarnings("unchecked")
        final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = mock(KafkaFuture.class);

        when(adminClient.listOffsets(any())).thenReturn(result);
        when(result.all()).thenReturn(allFuture);
        when(allFuture.get()).thenThrow(new InterruptedException());

        assertThrows(StreamsException.class, () -> fetchEndOffsets(PARTITIONS, adminClient));
    }

    @Test
    public void fetchEndOffsetsShouldRethrowExecutionExceptionAsStreamsException() throws Exception {
        final Admin adminClient = mock(AdminClient.class);
        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        @SuppressWarnings("unchecked")
        final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = mock(KafkaFuture.class);

        when(adminClient.listOffsets(any())).thenReturn(result);
        when(result.all()).thenReturn(allFuture);
        when(allFuture.get()).thenThrow(new ExecutionException(new RuntimeException()));

        assertThrows(StreamsException.class, () -> fetchEndOffsets(PARTITIONS, adminClient));
    }
    
    @Test
    public void shouldComputeSizeInBytesForConsumerRecord() {
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            TOPIC,
            1,
            0L,
            0L,
            TimestampType.CREATE_TIME,
            KEY_BYTES,
            VALUE_BYTES,
            KEY,
            VALUE,
            HEADERS,
            Optional.empty()
        );

        assertThat(consumerRecordSizeInBytes(record), equalTo(SIZE_IN_BYTES));
    }

    @Test
    public void shouldComputeSizeInBytesForProducerRecord() {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
            TOPIC,
            1,
            0L,
            KEY,
            VALUE,
            HEADERS
        );
        assertThat(producerRecordSizeInBytes(record), equalTo(SIZE_IN_BYTES));
    }

    @Test
    public void shouldComputeSizeInBytesForConsumerRecordWithNullKey() {
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            TOPIC,
            1,
            0,
            0L,
            TimestampType.CREATE_TIME,
            0,
            5,
            null,
            VALUE,
            HEADERS,
            Optional.empty()
        );
        assertThat(consumerRecordSizeInBytes(record), equalTo(NULL_KEY_SIZE_IN_BYTES));
    }

    @Test
    public void shouldComputeSizeInBytesForProducerRecordWithNullKey() {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
            TOPIC,
            1,
            0L,
            null,
            VALUE,
            HEADERS
        );
        assertThat(producerRecordSizeInBytes(record), equalTo(NULL_KEY_SIZE_IN_BYTES));
    }

    @Test
    public void shouldComputeSizeInBytesForConsumerRecordWithNullValue() {
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            TOPIC,
            1,
            0,
            0L,
            TimestampType.CREATE_TIME,
            KEY_BYTES,
            0,
            KEY,
            null,
            HEADERS,
            Optional.empty()
        );
        assertThat(consumerRecordSizeInBytes(record), equalTo(TOMBSTONE_SIZE_IN_BYTES));
    }

    @Test
    public void shouldComputeSizeInBytesForProducerRecordWithNullValue() {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
            TOPIC,
            1,
            0L,
            KEY,
            null,
            HEADERS
        );
        assertThat(producerRecordSizeInBytes(record), equalTo(TOMBSTONE_SIZE_IN_BYTES));
    }
}
