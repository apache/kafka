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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ConnectorOffsetBackingStoreTest {

    private static final String NAMESPACE = "namespace";
    // Connect format - any types should be accepted here
    private static final Map<String, Object> OFFSET_KEY = Collections.singletonMap("key", "key");
    private static final Map<String, Object> OFFSET_VALUE = Collections.singletonMap("key", 12);

    // Serialized
    private static final byte[] OFFSET_KEY_SERIALIZED = "key-serialized".getBytes();
    private static final byte[] OFFSET_VALUE_SERIALIZED = "value-serialized".getBytes();

    private static final Exception PRODUCE_EXCEPTION = new KafkaException();
    private static final Exception TIMEOUT_EXCEPTION = new TimeoutException();

    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;

    @Test
    public void testFlushFailureWhenWriteToSecondaryStoreFailsForTombstoneOffsets() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", false);
        KafkaOffsetBackingStore workerStore = createStore("topic2", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushFailureWhenWriteToSecondaryStoreTimesoutForTombstoneOffsets() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", false);
        KafkaOffsetBackingStore workerStore = createStore("topic2", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushSuccessWhenWritesSucceedToBothPrimaryAndSecondaryStoresForTombstoneOffsets() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", false);
        KafkaOffsetBackingStore workerStore = createStore("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushSuccessWhenWriteToSecondaryStoreFailsForNonTombstoneOffsets() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", false);
        KafkaOffsetBackingStore workerStore = createStore("topic2", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryAndSecondaryStoreSucceeds() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", false);
        KafkaOffsetBackingStore workerStore = createStore("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceeds() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", true);
        KafkaOffsetBackingStore workerStore = createStore("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceedsForTombstoneRecords() {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", true);
        KafkaOffsetBackingStore workerStore = createStore("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryStoreSucceedsWithNoSecondaryStore() throws Exception {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushSuccess(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsWithNoSecondaryStore() throws Exception {
        KafkaOffsetBackingStore connectorStore = createStore("topic1", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        AtomicReference<Object> callbackResult = new AtomicReference<>();
        AtomicReference<Throwable> callbackError = new AtomicReference<>();

        Future<Void> setFuture = offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            callbackInvoked.set(true);
            callbackResult.set(result);
            callbackError.set(error);
        });

        assertFlushFailure(callbackInvoked, callbackResult, callbackError, setFuture);
    }

    private void assertFlushFailure(AtomicBoolean callbackInvoked, AtomicReference<Object> callbackResult, AtomicReference<Throwable> callbackError, Future<Void> setFuture) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> setFuture.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(callbackInvoked.get());
        assertNull(callbackResult.get());
        assertEquals(PRODUCE_EXCEPTION, callbackError.get());
        assertNotNull(e.getCause());
        assertEquals(PRODUCE_EXCEPTION, e.getCause());
    }

    private void assertFlushSuccess(AtomicBoolean callbackInvoked, AtomicReference<Object> callbackResult, AtomicReference<Throwable> callbackError, Future<Void> setFuture) {
        assertDoesNotThrow(() -> setFuture.get(1000L, TimeUnit.MILLISECONDS));
        assertTrue(callbackInvoked.get());
        assertNull(callbackResult.get());
        assertNull(callbackError.get());
    }

    @SuppressWarnings("unchecked")
    private KafkaOffsetBackingStore createStore(String topic, boolean throwingProducer) {
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(() -> mock(TopicAdmin.class), () -> "connect",  mock(Converter.class));
        KafkaBasedLog<byte[], byte[]> kafkaBasedLog = new KafkaBasedLog<byte[], byte[]>(
                topic, new HashMap<>(), new HashMap<>(),
                () -> mock(TopicAdmin.class), mock(Callback.class), new MockTime(), null) {
            @Override
            protected Producer<byte[], byte[]> createProducer() {
                return createMockProducer(throwingProducer);
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

    private Producer<byte[], byte[]> createMockProducer(boolean throwingProducer) {
        return new MockProducer<byte[], byte[]>() {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final org.apache.kafka.clients.producer.Callback callback) {
                if (throwingProducer) {
                    callback.onCompletion(null, PRODUCE_EXCEPTION);
                } else {
                    callback.onCompletion(null, null);
                }
                return null;
            }
        };
    }

    private Map<ByteBuffer, ByteBuffer> getSerialisedOffsets(Map<String, Object> key, byte[] keySerialized,
                                                             Map<String, Object> value, byte[] valueSerialized) {
        List<Object> keyWrapped = Arrays.asList(NAMESPACE, key);
        when(keyConverter.fromConnectData(NAMESPACE, null, keyWrapped)).thenReturn(keySerialized);
        when(valueConverter.fromConnectData(NAMESPACE, null, value)).thenReturn(valueSerialized);

        return Collections.singletonMap(
            keySerialized == null ? null : ByteBuffer.wrap(keySerialized),
            valueSerialized == null ? null : ByteBuffer.wrap(valueSerialized));
    }
}
