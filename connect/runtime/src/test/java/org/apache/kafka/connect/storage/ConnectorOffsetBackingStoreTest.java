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
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ConnectorOffsetBackingStoreTest {

    // Serialized
    private static final byte[] OFFSET_KEY_SERIALIZED = "key-serialized".getBytes();

    private static final byte[] OFFSET_KEY_SERIALIZED_1 = "key-serialized-1".getBytes();
    private static final byte[] OFFSET_VALUE_SERIALIZED = "value-serialized".getBytes();

    private static final KafkaException PRODUCE_EXCEPTION = new KafkaException();

    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();

    @Test
    public void testFlushFailureWhenWriteToSecondaryStoreFailsForTombstoneOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(
            mkEntry(OFFSET_KEY_SERIALIZED, null),
            mkEntry(OFFSET_KEY_SERIALIZED_1, OFFSET_VALUE_SERIALIZED))
        ), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        workerStoreProducer.errorNext(PRODUCE_EXCEPTION);
        connectorStoreProducer.completeNext();
        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture, false);
    }

    @Test
    public void testFlushSuccessWhenWritesSucceedToBothPrimaryAndSecondaryStores() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(
            mkEntry(OFFSET_KEY_SERIALIZED, null),
            mkEntry(OFFSET_KEY_SERIALIZED_1, OFFSET_VALUE_SERIALIZED)
        )), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        workerStoreProducer.completeNext();
        connectorStoreProducer.completeNext();
        connectorStoreProducer.completeNext();
        workerStoreProducer.completeNext();
        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushSuccessWhenWriteToSecondaryStoreFailsForRegularOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(
            mkEntry(OFFSET_KEY_SERIALIZED, OFFSET_VALUE_SERIALIZED),
            mkEntry(OFFSET_KEY_SERIALIZED_1, null))
        ), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        // tombstone offset write succeeds.
        workerStoreProducer.completeNext();
        connectorStoreProducer.completeNext();
        connectorStoreProducer.completeNext();
        workerStoreProducer.errorNext(PRODUCE_EXCEPTION);
        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceedsForRegularOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(
            mkEntry(OFFSET_KEY_SERIALIZED, OFFSET_VALUE_SERIALIZED),
            mkEntry(OFFSET_KEY_SERIALIZED_1, OFFSET_VALUE_SERIALIZED)
        )), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        workerStoreProducer.completeNext();
        connectorStoreProducer.errorNext(PRODUCE_EXCEPTION);
        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture, false);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceedsForRegularAndTombstoneOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(
            mkEntry(OFFSET_KEY_SERIALIZED, null),
            mkEntry(OFFSET_KEY_SERIALIZED_1, OFFSET_VALUE_SERIALIZED)
        )), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        workerStoreProducer.completeNext();
        connectorStoreProducer.errorNext(PRODUCE_EXCEPTION);
        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture, false);
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryStoreSucceedsWithNoSecondaryStore() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(mkEntry(OFFSET_KEY_SERIALIZED, OFFSET_VALUE_SERIALIZED))), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        connectorStoreProducer.completeNext();
        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsWithNoSecondaryStore() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
            () -> LoggingContext.forConnector("source-connector"),
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(mkEntry(OFFSET_KEY_SERIALIZED, OFFSET_VALUE_SERIALIZED))), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        connectorStoreProducer.errorNext(PRODUCE_EXCEPTION);
        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture, false);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreTimesoutAndSecondarySucceedsForTombstoneOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(mkEntry(OFFSET_KEY_SERIALIZED, null))), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        workerStoreProducer.completeNext();
        // We don't invoke completeNext for Primary store producer which means the request isn't completed
        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture, true);
    }

    @Test
    public void testFlushFailureWhenWritesToSecondaryStoreTimesoutForTombstoneOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(mkEntry(OFFSET_KEY_SERIALIZED, null))), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        // We don't invoke completeNext for Primary or secondary store producer which means the request isn't completed
        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture, true);
    }

    @Test
    public void testFlushSuccessWhenWritesToSecondaryStoreTimesoutForRegularOffsets() {
        MockProducer<byte[], byte[]> connectorStoreProducer = createMockProducer();
        MockProducer<byte[], byte[]> workerStoreProducer = createMockProducer();
        KafkaOffsetBackingStore connectorStore = createStore("topic1", connectorStoreProducer);
        KafkaOffsetBackingStore workerStore = createStore("topic2", workerStoreProducer);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(mkMap(
            mkEntry(OFFSET_KEY_SERIALIZED, null),
            mkEntry(OFFSET_KEY_SERIALIZED_1, OFFSET_VALUE_SERIALIZED)
        )), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertNoPrematureCallbackInvocation(callbackInvoked);
        workerStoreProducer.completeNext();
        connectorStoreProducer.completeNext();
        connectorStoreProducer.completeNext();
        // We don't invoke completeNext for secondary store write of regular offset to throw a timeout
        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    private void assertNoPrematureCallbackInvocation(AtomicBoolean callbackInvoked) {
        assertFalse("Store callback should not be invoked before underlying producer callback", callbackInvoked.get());
    }

    private void assertFlushFailure(AtomicBoolean callbackInvoked, AtomicReference<Object> callbackResult, AtomicReference<Throwable> callbackError, Future<Void> setFuture, boolean timeout) {
        if (timeout) {
            assertThrows(TimeoutException.class, () -> setFuture.get(1000L, TimeUnit.MILLISECONDS));
        } else {
            ExecutionException e = assertThrows(ExecutionException.class, () -> setFuture.get(1000L, TimeUnit.MILLISECONDS));
            assertNotNull(e.getCause());
            assertEquals(PRODUCE_EXCEPTION, e.getCause());
            assertTrue(callbackInvoked.get());
            assertNull(callbackResult.get());
            assertEquals(PRODUCE_EXCEPTION, callbackError.get());
        }
    }

    private void assertFlushSuccess(AtomicBoolean callbackInvoked, AtomicReference<Object> callbackResult, AtomicReference<Throwable> callbackError, Future<Void> setFuture) {
        assertDoesNotThrow(() -> setFuture.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(callbackInvoked.get());
        assertNull(callbackResult.get());
        assertNull(callbackError.get());
    }

    @SuppressWarnings("unchecked")
    private KafkaOffsetBackingStore createStore(String topic, Producer<byte[], byte[]> producer) {
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(() -> mock(TopicAdmin.class), () -> "connect",  mock(Converter.class));
        KafkaBasedLog<byte[], byte[]> kafkaBasedLog = new KafkaBasedLog<byte[], byte[]>(
            topic, new HashMap<>(), new HashMap<>(),
            () -> mock(TopicAdmin.class), mock(Callback.class), new MockTime(), null) {
            @Override
            protected Producer<byte[], byte[]> createProducer() {
                return producer;
            }

            @Override
            protected Consumer<byte[], byte[]> createConsumer() {
                return createMockConsumer(topic);
            }
        };
        kafkaBasedLog.start();
        offsetBackingStore.offsetLog = kafkaBasedLog;
        return offsetBackingStore;
    }

    private MockConsumer<byte[], byte[]> createMockConsumer(String topic) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        Node noNode = Node.noNode();
        Node[] nodes = new Node[]{noNode};
        consumer.updatePartitions(topic, Collections.singletonList(new PartitionInfo(topic, 0, noNode, nodes, nodes)));
        consumer.updateBeginningOffsets(mkMap(mkEntry(new TopicPartition(topic, 0), 100L)));
        return consumer;
    }

    private MockProducer<byte[], byte[]> createMockProducer() {
        return new MockProducer<>(Cluster.empty(), false, null, byteArraySerializer, byteArraySerializer);
    }

    private Map<ByteBuffer, ByteBuffer> getSerialisedOffsets(Map<byte[], byte[]> offsets) {
        Map<ByteBuffer, ByteBuffer> serialisedOffsets = new HashMap<>();
        for (Map.Entry<byte[], byte[]> offsetEntry: offsets.entrySet()) {
            serialisedOffsets.put(ByteBuffer.wrap(offsetEntry.getKey()),
                offsetEntry.getValue() == null ? null : ByteBuffer.wrap(offsetEntry.getValue()));
        }
        return serialisedOffsets;
    }
}
