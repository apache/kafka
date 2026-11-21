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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ShareSinkDeliveryTest {
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final TopicPartition PAYMENTS = new TopicPartition("payments", 2);
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(5);

    private final TopicIdPartition ordersId = new TopicIdPartition(Uuid.randomUuid(), ORDERS);
    private final TopicIdPartition paymentsId = new TopicIdPartition(Uuid.randomUuid(), PAYMENTS);
    private final ConsumerRecords<byte[], byte[]> input = records();

    @Mock private ShareSinkTask task;
    @Mock private ShareConsumer<byte[], byte[]> consumer;

    @Test
    void completesSparseOriginalsOnceAfterDurablePut() {
        Map<TopicIdPartition, Optional<KafkaException>> responses = new LinkedHashMap<>(successfulAcknowledgements());
        when(consumer.commitSync(ACK_TIMEOUT)).thenReturn(responses);
        ShareSinkDelivery delivery = delivery();
        ShareSinkDelivery.CompletedDelivery completed = delivery.complete(task);
        assertSame(completed, delivery.complete(task));
        verifyNoInteractions(consumer);

        ShareSinkDelivery.AcknowledgementResult result = completed.acknowledge(consumer, ACK_TIMEOUT);
        assertSame(result, completed.acknowledge(consumer, ACK_TIMEOUT));
        responses.clear();

        InOrder order = inOrder(task, consumer);
        order.verify(task).put(anyCollection());
        for (ConsumerRecord<byte[], byte[]> original : input) {
            order.verify(consumer).acknowledge(same(original), eq(AcknowledgeType.ACCEPT));
        }
        order.verify(consumer).commitSync(ACK_TIMEOUT);
        verifyNoMoreInteractions(consumer);
        assertEquals(List.of(100L, 104L, 7L), deliveredRecords().stream().map(SinkRecord::kafkaOffset).toList());
        assertEquals(List.of("routed-orders", "routed-orders", "routed-payments"),
                deliveredRecords().stream().map(SinkRecord::topic).toList());
        assertNull(deliveredRecords().get(2).value());
        assertStatuses(result, ShareSinkDelivery.AcknowledgementStatus.CONFIRMED,
                ShareSinkDelivery.AcknowledgementStatus.CONFIRMED);
        assertTrue(result.allConfirmed());
        assertTrue(result.cause(ORDERS).isEmpty());
        assertTrue(result.cause(PAYMENTS).isEmpty());
        assertEquals(successfulAcknowledgements(), result.responses());
        assertThrows(UnsupportedOperationException.class, () -> result.responses().clear());
    }

    @Test
    void doesNotReturnCompletionBeforePutActuallyReturns() throws Exception {
        CountDownLatch enteredPut = new CountDownLatch(1);
        CountDownLatch allowPutToReturn = new CountDownLatch(1);
        doAnswer(invocation -> {
            enteredPut.countDown();
            assertTrue(allowPutToReturn.await(5, TimeUnit.SECONDS));
            return null;
        }).when(task).put(anyCollection());
        ShareSinkDelivery delivery = delivery();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<ShareSinkDelivery.CompletedDelivery> completion = executor.submit(() -> delivery.complete(task));

        try {
            assertTrue(enteredPut.await(5, TimeUnit.SECONDS));
            assertFalse(completion.isDone());
            verifyNoInteractions(consumer);
            allowPutToReturn.countDown();
            ShareSinkDelivery.CompletedDelivery completed = completion.get(5, TimeUnit.SECONDS);
            when(consumer.commitSync(ACK_TIMEOUT)).thenReturn(successfulAcknowledgements());
            assertTrue(completed.acknowledge(consumer, ACK_TIMEOUT).allConfirmed());
        } finally {
            allowPutToReturn.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest(name = "allFiltered={0}")
    @ValueSource(booleans = {false, true})
    void completesFilteredOriginalsWithoutCollapsingMappedOccurrences(boolean allFiltered) {
        SinkRecord transformed = convert(input.records(ORDERS).get(0));
        ShareSinkDelivery delivery = new ShareSinkDelivery(input, original -> allFiltered || original.offset() == 104
                ? ShareSinkDelivery.ConversionResult.filtered()
                : ShareSinkDelivery.ConversionResult.deliver(transformed));
        when(consumer.commitSync(ACK_TIMEOUT)).thenReturn(successfulAcknowledgements());

        ShareSinkDelivery.CompletedDelivery completed = delivery.complete(task);
        verifyNoInteractions(consumer);
        if (allFiltered) {
            verifyNoInteractions(task);
        } else {
            List<SinkRecord> delivered = deliveredRecords();
            assertEquals(2, delivered.size());
            delivered.forEach(record -> assertSame(transformed, record));
        }
        assertTrue(completed.acknowledge(consumer, ACK_TIMEOUT).allConfirmed());
        for (ConsumerRecord<byte[], byte[]> original : input) {
            verify(consumer).acknowledge(same(original), eq(AcknowledgeType.ACCEPT));
        }
        verify(consumer).commitSync(ACK_TIMEOUT);
        verifyNoMoreInteractions(consumer);
    }

    @Test
    void failedConversionPreventsPartialDestinationWrite() {
        ConnectException failure = new ConnectException("value conversion failed");

        assertThrows(ConnectException.class, () -> new ShareSinkDelivery(input, original -> {
            if (original.offset() == 104) {
                return ShareSinkDelivery.ConversionResult.failed(failure);
            }
            return ShareSinkDelivery.ConversionResult.deliver(convert(original));
        }));

        verifyNoInteractions(task, consumer);
    }

    @Test
    void retriesPreparedUnitAfterPartialDestinationEffect() {
        List<Long> destinationWrites = new ArrayList<>();
        List<Collection<SinkRecord>> suppliedBatches = new ArrayList<>();
        AtomicInteger putAttempts = new AtomicInteger();
        AtomicInteger conversions = new AtomicInteger();
        RetriableException failure = new RetriableException("reply lost after first destination write");
        doAnswer(invocation -> {
            Collection<SinkRecord> records = invocation.getArgument(0);
            assertEquals(input.count(), records.size());
            suppliedBatches.add(records);
            if (putAttempts.getAndIncrement() == 0) {
                destinationWrites.add(records.iterator().next().kafkaOffset());
                records.clear();
                throw failure;
            }
            records.forEach(record -> destinationWrites.add(record.kafkaOffset()));
            records.clear();
            return null;
        }).when(task).put(anyCollection());
        ShareSinkDelivery delivery = new ShareSinkDelivery(input, original -> {
            conversions.incrementAndGet();
            return ShareSinkDelivery.ConversionResult.deliver(convert(original));
        });

        assertSame(failure, assertThrows(RetriableException.class, () -> delivery.complete(task)));
        assertEquals(List.of(100L), destinationWrites);
        verifyNoInteractions(consumer);

        ShareSinkDelivery.CompletedDelivery completed = delivery.complete(task);
        when(consumer.commitSync(ACK_TIMEOUT)).thenReturn(successfulAcknowledgements());
        assertTrue(completed.acknowledge(consumer, ACK_TIMEOUT).allConfirmed());
        assertEquals(List.of(100L, 100L, 104L, 7L), destinationWrites);
        assertEquals(input.count(), conversions.get());
        assertNotSame(suppliedBatches.get(0), suppliedBatches.get(1));
        verify(task, times(2)).put(anyCollection());
    }

    @ParameterizedTest(name = "response={0}")
    @ValueSource(strings = {"error", "missing", "empty"})
    void preservesConfirmedPartitionAcrossIncompleteAcknowledgement(String responseKind) {
        KafkaException failure = new InvalidRecordStateException("payments acquisition expired");
        Map<TopicIdPartition, Optional<KafkaException>> responses = switch (responseKind) {
            case "error" -> Map.of(ordersId, Optional.empty(), paymentsId, Optional.of(failure));
            case "missing" -> Map.of(ordersId, Optional.empty());
            case "empty" -> Map.of();
            default -> throw new IllegalArgumentException(responseKind);
        };
        when(consumer.commitSync(ACK_TIMEOUT)).thenReturn(responses);

        ShareSinkDelivery.AcknowledgementResult result = delivery().complete(task)
                .acknowledge(consumer, ACK_TIMEOUT);

        assertStatuses(result, "empty".equals(responseKind) ? ShareSinkDelivery.AcknowledgementStatus.UNKNOWN
                        : ShareSinkDelivery.AcknowledgementStatus.CONFIRMED,
                "error".equals(responseKind) ? ShareSinkDelivery.AcknowledgementStatus.REPORTED_ERROR
                        : ShareSinkDelivery.AcknowledgementStatus.UNKNOWN);
        assertFalse(result.allConfirmed());
        if (!"empty".equals(responseKind)) {
            assertTrue(result.cause(ORDERS).isEmpty());
        }
        if ("error".equals(responseKind)) {
            assertEquals(Optional.of(failure), result.cause(PAYMENTS));
        }
        assertEquals(responses, result.responses());
    }

    @Test
    void acknowledgementTimeoutIsCapturedWithoutRestagingOnRetry() {
        TimeoutException timeout = new TimeoutException("acknowledgement response unavailable");
        when(consumer.commitSync(ACK_TIMEOUT)).thenThrow(timeout);
        ShareSinkDelivery.CompletedDelivery completed = delivery().complete(task);

        ShareSinkDelivery.AcknowledgementResult result = completed.acknowledge(consumer, ACK_TIMEOUT);

        assertStatuses(result, ShareSinkDelivery.AcknowledgementStatus.UNKNOWN,
                ShareSinkDelivery.AcknowledgementStatus.UNKNOWN);
        assertFalse(result.allConfirmed());
        assertEquals(Optional.of(timeout), result.cause(ORDERS));
        assertEquals(Optional.of(timeout), result.cause(PAYMENTS));
        assertTrue(result.responses().isEmpty());
        assertSame(result, completed.acknowledge(consumer, ACK_TIMEOUT));
        for (ConsumerRecord<byte[], byte[]> original : input) {
            verify(consumer).acknowledge(same(original), eq(AcknowledgeType.ACCEPT));
        }
        verify(consumer).commitSync(ACK_TIMEOUT);
        verifyNoMoreInteractions(consumer);
        verify(task).put(anyCollection());
    }

    @Test
    void acknowledgementStagingFailureDoesNotCommitOrRestage() {
        ConsumerRecord<byte[], byte[]> rejected = input.records(ORDERS).get(1);
        RuntimeException failure = new IllegalStateException("record is no longer actionable");
        doNothing().when(consumer).acknowledge(same(input.records(ORDERS).get(0)), eq(AcknowledgeType.ACCEPT));
        doThrow(failure).when(consumer).acknowledge(same(rejected), eq(AcknowledgeType.ACCEPT));
        ShareSinkDelivery.CompletedDelivery completed = delivery().complete(task);

        ShareSinkDelivery.AcknowledgementResult result = completed.acknowledge(consumer, ACK_TIMEOUT);

        assertStatuses(result, ShareSinkDelivery.AcknowledgementStatus.UNKNOWN,
                ShareSinkDelivery.AcknowledgementStatus.UNKNOWN);
        assertFalse(result.allConfirmed());
        assertEquals(Optional.of(failure), result.cause(ORDERS));
        assertTrue(result.responses().isEmpty());
        assertSame(result, completed.acknowledge(consumer, ACK_TIMEOUT));
        verify(consumer).acknowledge(same(input.records(ORDERS).get(0)), eq(AcknowledgeType.ACCEPT));
        verify(consumer).acknowledge(same(rejected), eq(AcknowledgeType.ACCEPT));
        verify(consumer, never()).acknowledge(same(input.records(PAYMENTS).get(0)), eq(AcknowledgeType.ACCEPT));
        verify(consumer, never()).commitSync(any(Duration.class));
        verifyNoMoreInteractions(consumer);
    }

    private ShareSinkDelivery delivery() {
        return new ShareSinkDelivery(input, original -> ShareSinkDelivery.ConversionResult.deliver(convert(original)));
    }

    private static ConsumerRecords<byte[], byte[]> records() {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new LinkedHashMap<>();
        records.put(ORDERS, List.of(
                new ConsumerRecord<>(ORDERS.topic(), ORDERS.partition(), 100, new byte[] {1}, new byte[] {10}),
                new ConsumerRecord<>(ORDERS.topic(), ORDERS.partition(), 104, new byte[] {2}, new byte[] {20})));
        records.put(PAYMENTS, List.of(
                new ConsumerRecord<>(PAYMENTS.topic(), PAYMENTS.partition(), 7, new byte[] {3}, null)));
        return new ConsumerRecords<>(records, Map.of());
    }

    private static SinkRecord convert(ConsumerRecord<byte[], byte[]> original) {
        return new SinkRecord("routed-" + original.topic(), 99, Schema.BYTES_SCHEMA, original.key(),
                Schema.OPTIONAL_BYTES_SCHEMA, original.value(), original.offset());
    }

    private Map<TopicIdPartition, Optional<KafkaException>> successfulAcknowledgements() {
        return Map.of(ordersId, Optional.empty(), paymentsId, Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private List<SinkRecord> deliveredRecords() {
        ArgumentCaptor<Collection<SinkRecord>> records = ArgumentCaptor.forClass(Collection.class);
        verify(task).put(records.capture());
        return List.copyOf(records.getValue());
    }

    private static void assertStatuses(ShareSinkDelivery.AcknowledgementResult result,
                                       ShareSinkDelivery.AcknowledgementStatus orders,
                                       ShareSinkDelivery.AcknowledgementStatus payments) {
        assertEquals(Map.of(ORDERS, orders, PAYMENTS, payments), result.statuses());
    }
}
