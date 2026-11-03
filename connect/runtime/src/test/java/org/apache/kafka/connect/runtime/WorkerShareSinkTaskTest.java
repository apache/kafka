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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.isolation.TestPlugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(30)
class WorkerShareSinkTaskTest {

    @Test
    void retainsAnUninterruptibleRetryUntilActualExitAndKeepsItsFailureStatus() throws Exception {
        try (Fixture fixture = new Fixture()) {
            assertTrue(fixture.enteredPut.await(10, TimeUnit.SECONDS));
            fixture.time.sleep(500);
            assertTrue(fixture.consumerClosed.await(10, TimeUnit.SECONDS));
            assertTrue(fixture.ignoredInterrupt.await(10, TimeUnit.SECONDS));
            assertTrue(fixture.failed.await(10, TimeUnit.SECONDS));
            assertTrue(hasCauseMessage(fixture.failure.get(), "budget expired"));
            assertTrue(fixture.activePut.get());
            assertFalse(fixture.execution.isDone());
            assertFalse(fixture.workerTask.awaitStop(0));
            assertTrue(fixture.registry.hasOrphans());
            assertTrue(fixture.registry.contains(fixture.id));
            assertThrows(ConnectException.class, () -> fixture.registry.reserve(fixture.id));
            assertThrows(ConnectException.class, () -> fixture.registry.reserve(new ConnectorTaskId("rotated-orders", 0)));
            verify(fixture.task, never()).stop();
            verify(fixture.key, never()).close();
            verify(fixture.value, never()).close();
            verify(fixture.header, never()).close();

            fixture.permitPutReturn.countDown();
            assertTrue(fixture.exitedPut.await(10, TimeUnit.SECONDS));
            fixture.execution.get(10, TimeUnit.SECONDS);
            fixture.reservation.released().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertTrue(fixture.workerTask.awaitStop(0));
            assertFalse(fixture.registry.hasOrphans());
            assertFalse(fixture.registry.contains(fixture.id));
            assertEquals("FAILED", fixture.lastStatus.get());
            verify(fixture.listener).onFailure(eq(fixture.id), any(Throwable.class));
            verify(fixture.listener, never()).onShutdown(fixture.id);
            verify(fixture.task).stop();
            verify(fixture.task, times(2)).put(anyCollection());
            verify(fixture.key).toConnectData("input", fixture.original.key());
            verify(fixture.value).toConnectData("input", fixture.original.value());
            verify(fixture.key).close();
            verify(fixture.value).close();
            verify(fixture.header).close();
            verify(fixture.consumer, never()).acknowledge(any(), eq(AcknowledgeType.ACCEPT));
            verify(fixture.consumer).commitSync(any(Duration.class));
            verify(fixture.consumer, times(2)).poll(any(Duration.class));
            verify(fixture.consumer).close(any(Duration.class));
            ShareSinkExecutionRegistry.Reservation replacement = fixture.registry.reserve(fixture.id);
            assertTrue(replacement.incarnation() > fixture.reservation.incarnation());
            replacement.controllerExited(true);
            replacement.pluginExited(true);
        }
    }

    private static boolean hasCauseMessage(Throwable error, String message) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(message)) {
                return true;
            }
        }
        return false;
    }

    private static final class Fixture implements AutoCloseable {
        private final MockTime time = new MockTime(0, 0, 0);
        private final MockConnectMetrics metrics = new MockConnectMetrics(time);
        private final ConnectorTaskId id = new ConnectorTaskId("orders", 0);
        private final ShareSinkExecutionRegistry registry = new ShareSinkExecutionRegistry();
        private final ShareSinkExecutionRegistry.Reservation reservation = registry.reserve(id);
        @SuppressWarnings("unchecked")
        private final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        private final ShareSinkTask task = mock(ShareSinkTask.class);
        private final ConsumerRecord<byte[], byte[]> original = new ConsumerRecord<>("input", 0, 0,
                "key".getBytes(UTF_8), "value".getBytes(UTF_8));
        private final TopicPartition partition = new TopicPartition("input", 0);
        private final TopicIdPartition partitionId = new TopicIdPartition(Uuid.randomUuid(), partition);
        private final ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(partition, List.of(original)), Map.of());
        private final CountDownLatch enteredPut = new CountDownLatch(1);
        private final CountDownLatch ignoredInterrupt = new CountDownLatch(1);
        private final CountDownLatch permitPutReturn = new CountDownLatch(1);
        private final CountDownLatch exitedPut = new CountDownLatch(1);
        private final CountDownLatch consumerClosed = new CountDownLatch(1);
        private final CountDownLatch failed = new CountDownLatch(1);
        private final AtomicBoolean activePut = new AtomicBoolean();
        private final AtomicInteger putAttempts = new AtomicInteger();
        private final AtomicReference<SinkTaskContext> context = new AtomicReference<>();
        private final AtomicReference<Thread> consumerOwner = new AtomicReference<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicReference<String> lastStatus = new AtomicReference<>();
        private final TaskStatus.Listener listener = mock(TaskStatus.Listener.class);
        private final StringConverter key = spy(new StringConverter());
        private final StringConverter value = spy(new StringConverter());
        private final SimpleHeaderConverter header = spy(new SimpleHeaderConverter());
        private final ExecutorService plugin = Executors.newSingleThreadExecutor();
        private final WorkerShareSinkTask workerTask;
        private final Future<?> execution;

        private Fixture() {
            when(task.version()).thenReturn("1");
            configureLifecycle();
            configureConsumer();
            workerTask = createTask();
            reservation.onRetirement(workerTask::removeMetrics);
            workerTask.initialize(new TaskConfig(Map.of(
                    TaskConfig.TASK_CLASS_CONFIG, ShareSinkTask.class.getName(),
                    SinkConnectorConfig.TOPICS_CONFIG, "input")));
            execution = plugin.submit(workerTask);
        }

        private void configureLifecycle() {
            doAnswer(invocation -> {
                context.set(invocation.getArgument(0));
                return null;
            }).when(task).initialize(any(SinkTaskContext.class));
            doAnswer(invocation -> {
                lastStatus.set("RUNNING");
                return null;
            }).when(listener).onStartup(id);
            doAnswer(invocation -> {
                failure.set(invocation.getArgument(1));
                lastStatus.set("FAILED");
                failed.countDown();
                return null;
            }).when(listener).onFailure(eq(id), any(Throwable.class));
            doAnswer(invocation -> {
                lastStatus.set("SHUTDOWN");
                return null;
            }).when(listener).onShutdown(id);
            doAnswer(invocation -> {
                if (putAttempts.getAndIncrement() == 0) {
                    context.get().timeout(0);
                    throw new RetriableException("Retry after a partial destination write");
                }
                activePut.set(true);
                enteredPut.countDown();
                try {
                    boolean permitted = false;
                    while (!permitted) {
                        try {
                            permitPutReturn.await();
                            permitted = true;
                        } catch (InterruptedException ignored) {
                            ignoredInterrupt.countDown();
                        }
                    }
                } finally {
                    activePut.set(false);
                    exitedPut.countDown();
                }
                return null;
            }).when(task).put(anyCollection());
        }

        private void configureConsumer() {
            when(consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
                assertSame(consumerOwner.get(), Thread.currentThread());
                return records;
            });
            when(consumer.acquisitionLockTimeoutMs()).thenReturn(Optional.of(1_000));
            when(consumer.commitSync(any(Duration.class))).thenAnswer(invocation -> {
                assertSame(consumerOwner.get(), Thread.currentThread());
                return Map.of(partitionId, Optional.empty());
            });
            doAnswer(invocation -> {
                assertSame(consumerOwner.get(), Thread.currentThread());
                consumerClosed.countDown();
                return null;
            }).when(consumer).close(any(Duration.class));
        }

        private WorkerShareSinkTask createTask() {
            key.configure(Map.of(), true);
            value.configure(Map.of(), false);
            header.configure(Map.of());
            ErrorHandlingMetrics errorMetrics = new ErrorHandlingMetrics(id, metrics);
            RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retry =
                    new RetryWithToleranceOperator<>(0, 100, ToleranceType.NONE, time, errorMetrics);
            TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformations = new TransformationChain<>(List.of(), retry);
            WorkerConfig config = new StandaloneConfig(Map.of(
                    WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
                    WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
                    "offset.storage.file.filename", "/tmp/share-sink-test.offsets"));
            WorkerShareSinkTask.Resources resources = new WorkerShareSinkTask.Resources(
                    config, ClusterConfigState.EMPTY, metrics, metrics.wrap(key, id, true), metrics.wrap(value, id, false),
                    errorMetrics, metrics.wrap(header, id), transformations, retry, mock(StatusBackingStore.class),
                    List::of, null, TestPlugins.noOpLoaderSwap());
            return new WorkerShareSinkTask(id, task, listener, TargetState.STARTED, resources, () -> {
                assertTrue(consumerOwner.compareAndSet(null, Thread.currentThread()));
                return consumer;
            }, new ShareSinkConsumerController.Settings(200, 25, 100, 10_000, 1, 1_024), 10,
                    getClass().getClassLoader(), time, reservation);
        }

        @Override
        public void close() throws Exception {
            permitPutReturn.countDown();
            workerTask.cancel();
            plugin.shutdownNow();
            try {
                assertTrue(plugin.awaitTermination(10, TimeUnit.SECONDS));
            } finally {
                metrics.stop();
            }
        }
    }
}
