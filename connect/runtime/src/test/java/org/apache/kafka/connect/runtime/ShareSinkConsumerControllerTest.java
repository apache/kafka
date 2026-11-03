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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(30)
class ShareSinkConsumerControllerTest {
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final TopicPartition PAYMENTS = new TopicPartition("payments", 1);
    private static final long INCARNATION = 42;
    private static final int INITIAL_LOCK_MS = 10_000;
    private static final int SHORTENED_LOCK_MS = 4_000;
    private static final long RENEW_AT_MS = 4_000;
    private static final long RESPONSE_DELAY_MS = 800;
    private static final long REATTACH_DELAY_MS = 20;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    @Test
    void holdsCompletionUntilRenewalReattachesAndUsesTheShortenedLeaseBudget() throws Exception {
        try (Harness harness = new Harness("success")) {
            ShareSinkConsumerController.Unit unit = harness.controller.awaitUnit();
            assertNotNull(unit);
            assertEquals(INCARNATION, unit.id().incarnation());
            assertEquals(1, unit.id().attempt());
            assertEquals(0, unit.startedMs());
            assertEquals(6, unit.retainedBytes());
            assertEquals(harness.input.records(ORDERS), unit.records().records(ORDERS));
            assertEquals(harness.input.records(PAYMENTS), unit.records().records(PAYMENTS));

            harness.time.sleep(RENEW_AT_MS);
            await(harness.renewalEntered);
            assertTrue(harness.publishCompletion(unit));
            verify(harness.consumer, never()).wakeup();
            verify(harness.consumer, never()).acknowledge(any(), eq(AcknowledgeType.ACCEPT));
            assertEquals(2, harness.polls.get());
            assertTrue(harness.controller.lastAcknowledgement().isEmpty());

            harness.releaseRenewal.countDown();
            await(harness.idlePollEntered);

            assertTrue(harness.controller.lastAcknowledgement().orElseThrow().allConfirmed());
            assertTrue(harness.controller.failure().isEmpty());
            long remainingBudgetMs = SHORTENED_LOCK_MS - SHORTENED_LOCK_MS / 4
                    - RESPONSE_DELAY_MS - REATTACH_DELAY_MS;
            assertTrue(harness.terminalAckTimeout.get().toMillis() > 0);
            assertTrue(harness.terminalAckTimeout.get().toMillis() <= remainingBudgetMs);
            assertEquals(4, harness.polls.get());
            assertFalse(harness.plugin.submit(() -> harness.controller.publishResult(unit, harness.receipt.get()))
                    .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            verify(harness.task).put(anyCollection());
            for (ConsumerRecord<byte[], byte[]> original : harness.input) {
                verify(harness.consumer).acknowledge(original, AcknowledgeType.ACCEPT);
            }
        }
    }

    @ParameterizedTest(name = "failure={0}")
    @ValueSource(strings = {"partial", "uuid", "shortened-duration", "accept-expiry"})
    void rejectsMismatchedCompletionAndClosesBeforeUsingUncertainOwnership(String failure) throws Exception {
        try (Harness harness = new Harness(failure)) {
            ShareSinkConsumerController.Unit unit = harness.controller.awaitUnit();
            assertNotNull(unit);
            ShareSinkDelivery.CompletedDelivery mismatched = harness.plugin.submit(() ->
                    new ShareSinkDelivery(copyRecords(unit.records()), ignored -> ShareSinkDelivery.ConversionResult.filtered())
                            .complete(harness.task)).get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertFalse(harness.plugin.submit(() -> harness.controller.publishResult(unit, mismatched))
                    .get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

            harness.time.sleep(RENEW_AT_MS);
            await(harness.renewalEntered);
            assertTrue(harness.publishCompletion(unit));
            verify(harness.consumer, never()).wakeup();
            verify(harness.consumer, never()).acknowledge(any(), eq(AcknowledgeType.ACCEPT));
            harness.releaseRenewal.countDown();

            if ("partial".equals(failure)) {
                await(harness.closeEntered);
                assertFalse(harness.controller.awaitStopped(Duration.ZERO));
                assertFalse(harness.controller.cleanupComplete());
                assertThrows(ConnectException.class, harness.controller::awaitUnit);
                harness.releaseClose.countDown();
            }
            assertTrue(harness.controller.awaitStopped(TEST_TIMEOUT));
            assertTrue(harness.controller.cleanupComplete());
            assertTrue(harness.controller.failure().isPresent());
            if ("accept-expiry".equals(failure)) {
                ShareSinkDelivery.AcknowledgementResult result = harness.controller.lastAcknowledgement().orElseThrow();
                assertFalse(result.allConfirmed());
                assertTrue(result.responses().isEmpty());
                assertEquals(Map.of(ORDERS, ShareSinkDelivery.AcknowledgementStatus.UNKNOWN,
                        PAYMENTS, ShareSinkDelivery.AcknowledgementStatus.UNKNOWN), result.statuses());
                assertTrue(result.cause(ORDERS).orElseThrow().getMessage().contains("expired"));
            } else {
                assertTrue(harness.controller.lastAcknowledgement().isEmpty());
            }
            assertThrows(ConnectException.class, harness.controller::awaitUnit);
            assertFalse(harness.controller.publishResult(unit, harness.receipt.get()));
            boolean reattached = "shortened-duration".equals(failure) || "accept-expiry".equals(failure);
            assertEquals(reattached ? 3 : 2, harness.polls.get());
            assertEquals(2, harness.commits.get());
            verify(harness.consumer, times("accept-expiry".equals(failure) ? 1 : 0))
                    .acknowledge(any(), eq(AcknowledgeType.ACCEPT));
            verify(harness.consumer).close(any(Duration.class));
            verify(harness.task).put(anyCollection());
        }
    }

    @Test
    void drainsAnUnfinishedRetryWithoutDispatchingOrAcknowledgingIt() throws Exception {
        try (Harness harness = new Harness("retry-drain")) {
            ShareSinkConsumerController.Unit unit = harness.controller.awaitUnit();
            assertNotNull(unit);
            assertTrue(harness.controller.publishRetry(unit, 1_000));

            harness.controller.requestDrain();

            assertTrue(harness.controller.awaitStopped(TEST_TIMEOUT));
            assertTrue(harness.controller.cleanupComplete());
            assertTrue(harness.controller.failure().isEmpty());
            assertTrue(harness.controller.lastAcknowledgement().isEmpty());
            assertEquals(2, harness.polls.get());
            assertEquals(1, harness.commits.get());
            verify(harness.consumer, never()).acknowledge(any(), eq(AcknowledgeType.ACCEPT));
            assertFalse(harness.controller.publishResult(unit,
                    new ShareSinkDelivery(unit.records(), ignored -> ShareSinkDelivery.ConversionResult.filtered()).complete(harness.task)));
        }
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    }

    private static ConsumerRecords<byte[], byte[]> records() {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new LinkedHashMap<>();
        records.put(ORDERS, List.of(
                new ConsumerRecord<>(ORDERS.topic(), ORDERS.partition(), 100, new byte[] {1}, new byte[] {10}),
                new ConsumerRecord<>(ORDERS.topic(), ORDERS.partition(), 104, new byte[] {2}, new byte[] {20})));
        records.put(PAYMENTS, List.of(
                new ConsumerRecord<>(PAYMENTS.topic(), PAYMENTS.partition(), 7, new byte[] {3}, new byte[] {30})));
        return new ConsumerRecords<>(records, Map.of());
    }

    private static ConsumerRecords<byte[], byte[]> copyRecords(ConsumerRecords<byte[], byte[]> originals) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> copies = new LinkedHashMap<>();
        for (TopicPartition partition : originals.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> original : originals.records(partition)) {
                records.add(new ConsumerRecord<>(original.topic(), original.partition(), original.offset(), original.key(), original.value()));
            }
            copies.put(partition, records);
        }
        return new ConsumerRecords<>(copies, Map.of());
    }

    private static final class Harness implements AutoCloseable {
        private final ConsumerRecords<byte[], byte[]> input = records();
        @SuppressWarnings("unchecked")
        private final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        private final ShareSinkTask task = mock(ShareSinkTask.class);
        private final MockTime time = new MockTime(0, 0, 0);
        private final ExecutorService owner = Executors.newSingleThreadExecutor();
        private final ExecutorService plugin = Executors.newSingleThreadExecutor();
        private final AtomicReference<Thread> ownerThread = new AtomicReference<>();
        private final AtomicReference<ShareSinkDelivery.CompletedDelivery> receipt = new AtomicReference<>();
        private final AtomicReference<Duration> terminalAckTimeout = new AtomicReference<>();
        private final AtomicInteger polls = new AtomicInteger();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger lockDuration = new AtomicInteger(INITIAL_LOCK_MS);
        private final AtomicBoolean reattached = new AtomicBoolean();
        private final CountDownLatch renewalEntered = new CountDownLatch(1);
        private final CountDownLatch releaseRenewal = new CountDownLatch(1);
        private final CountDownLatch idlePollEntered = new CountDownLatch(1);
        private final CountDownLatch releaseIdlePoll = new CountDownLatch(1);
        private final CountDownLatch closeEntered = new CountDownLatch(1);
        private final CountDownLatch releaseClose = new CountDownLatch(1);
        private final List<AcknowledgeType> pendingAcknowledgements = new ArrayList<>();
        private final TopicIdPartition ordersId = new TopicIdPartition(Uuid.randomUuid(), ORDERS);
        private final TopicIdPartition paymentsId = new TopicIdPartition(Uuid.randomUuid(), PAYMENTS);
        private final ShareSinkConsumerController controller;
        private final String scenario;

        private Harness(String failure) {
            scenario = failure;
            when(consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
                assertOwner();
                int poll = polls.incrementAndGet();
                if (poll <= 2) {
                    return input;
                }
                if (poll == 3) {
                    time.sleep(REATTACH_DELAY_MS);
                    lockDuration.set("shortened-duration".equals(failure) ? 1_000 : SHORTENED_LOCK_MS);
                    reattached.set(true);
                    return input;
                }
                assertEquals(4, poll);
                idlePollEntered.countDown();
                await(releaseIdlePoll);
                throw new WakeupException();
            });
            when(consumer.acquisitionLockTimeoutMs()).thenAnswer(invocation -> {
                assertOwner();
                return Optional.of(lockDuration.get());
            });
            doAnswer(invocation -> {
                assertOwner();
                AcknowledgeType type = invocation.getArgument(1);
                if (type == AcknowledgeType.ACCEPT) {
                    assertTrue(reattached.get());
                    if ("accept-expiry".equals(failure)) {
                        time.sleep(30_000);
                    }
                }
                pendingAcknowledgements.add(type);
                return null;
            }).when(consumer).acknowledge(any(), any(AcknowledgeType.class));
            when(consumer.commitSync(any(Duration.class))).thenAnswer(invocation -> {
                assertOwner();
                int commit = commits.incrementAndGet();
                AcknowledgeType expected = commit <= 2 ? AcknowledgeType.RENEW : AcknowledgeType.ACCEPT;
                assertEquals(List.of(expected, expected, expected), pendingAcknowledgements);
                pendingAcknowledgements.clear();
                if (commit == 2) {
                    renewalEntered.countDown();
                    await(releaseRenewal);
                    time.sleep(RESPONSE_DELAY_MS);
                    if ("partial".equals(failure)) {
                        return Map.of(ordersId, Optional.empty(), paymentsId,
                                Optional.of(new InvalidRecordStateException("payments acquisition expired")));
                    }
                    if ("uuid".equals(failure)) {
                        return Map.of(new TopicIdPartition(Uuid.randomUuid(), ORDERS), Optional.empty(), paymentsId, Optional.empty());
                    }
                } else if (commit == 3) {
                    terminalAckTimeout.set(invocation.getArgument(0));
                } else {
                    assertEquals(1, commit);
                }
                return Map.<TopicIdPartition, Optional<KafkaException>>of(ordersId, Optional.empty(), paymentsId, Optional.empty());
            });
            doAnswer(invocation -> {
                assertOwner();
                return null;
            }).when(consumer).subscribe(List.of(ORDERS.topic(), PAYMENTS.topic()));
            doAnswer(invocation -> {
                assertOwner();
                closeEntered.countDown();
                if ("partial".equals(failure)) {
                    await(releaseClose);
                }
                return null;
            }).when(consumer).close(any(Duration.class));
            doAnswer(invocation -> {
                releaseIdlePoll.countDown();
                return null;
            }).when(consumer).wakeup();
            controller = new ShareSinkConsumerController(() -> {
                assertTrue(ownerThread.compareAndSet(null, Thread.currentThread()));
                return consumer;
            }, List.of(ORDERS.topic(), PAYMENTS.topic()), INCARNATION,
                    new ShareSinkConsumerController.Settings(30_000, 25, 5_000, 10_000, 3, 1_024), time);
            time.addListener(controller::signal);
            owner.submit(controller);
        }

        private boolean publishCompletion(ShareSinkConsumerController.Unit unit) throws Exception {
            return plugin.submit(() -> {
                ShareSinkDelivery.CompletedDelivery completed = new ShareSinkDelivery(unit.records(), original ->
                        ShareSinkDelivery.ConversionResult.deliver(new SinkRecord(original.topic(), original.partition(),
                                Schema.BYTES_SCHEMA, original.key(), Schema.BYTES_SCHEMA, original.value(), original.offset())))
                        .complete(task);
                receipt.set(completed);
                return controller.publishResult(unit, completed);
            }).get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void assertOwner() {
            assertSame(ownerThread.get(), Thread.currentThread());
        }

        @Override
        public void close() throws Exception {
            releaseRenewal.countDown();
            releaseClose.countDown();
            controller.requestStop();
            releaseIdlePoll.countDown();
            try {
                assertTrue(controller.awaitStopped(TEST_TIMEOUT));
                assertTrue(controller.cleanupComplete());
                if ("success".equals(scenario)) {
                    assertTrue(controller.failure().isEmpty());
                }
                verify(consumer).close(any(Duration.class));
            } finally {
                plugin.shutdownNow();
                owner.shutdownNow();
                assertTrue(plugin.awaitTermination(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                assertTrue(owner.awaitTermination(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            }
        }
    }
}
