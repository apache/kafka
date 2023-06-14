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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class OffsetStorageWriterTest {
    private static final String NAMESPACE = "namespace";
    // Connect format - any types should be accepted here
    private static final Map<String, Object> OFFSET_KEY = Collections.singletonMap("key", "key");
    private static final Map<String, Object> OFFSET_VALUE = Collections.singletonMap("key", 12);

    // Serialized
    private static final byte[] OFFSET_KEY_SERIALIZED = "key-serialized".getBytes();
    private static final byte[] OFFSET_VALUE_SERIALIZED = "value-serialized".getBytes();

    private static final Exception EXCEPTION = new RuntimeException("error");

    private static final Exception PRODUCE_EXCEPTION = new KafkaException();

    private final OffsetBackingStore store = mock(OffsetBackingStore.class);
    private final Converter keyConverter = mock(Converter.class);
    private final Converter valueConverter = mock(Converter.class);
    private OffsetStorageWriter writer;

    private ExecutorService service;

    @Before
    public void setup() {
        writer = new OffsetStorageWriter(store, NAMESPACE, keyConverter, valueConverter);
        service = Executors.newFixedThreadPool(1);
    }

    @After
    public void teardown() {
        service.shutdownNow();
    }

    @Test
    public void testWriteFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        expectStore(store, OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, null);

        writer.offset(OFFSET_KEY, OFFSET_VALUE);

        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());
    }

    // It should be possible to set offset values to null
    @Test
    public void testWriteNullValueFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        expectStore(store, OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null, false, null);

        writer.offset(OFFSET_KEY, null);

        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());
    }

    // It should be possible to use null keys. These aren't actually stored as null since the key is wrapped to include
    // info about the namespace (connector)
    @Test
    public void testWriteNullKeyFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        expectStore(store, null, null, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, null);

        writer.offset(null, OFFSET_VALUE);

        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());
    }

    @Test
    public void testNoOffsetsToFlush() throws InterruptedException, TimeoutException {
        assertFalse(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        assertFalse(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));

        // If no offsets are flushed, we should finish immediately and not have made any calls to the
        // underlying storage layer
        verifyNoInteractions(store);
    }

    @Test
    public void testFlushFailureReplacesOffsets() throws Exception {
        // When a flush fails, we shouldn't just lose the offsets. Instead, they should be restored
        // such that a subsequent flush will write them.

        @SuppressWarnings("unchecked")
        final Callback<Void> callback = mock(Callback.class);
        // First time the write fails
        expectStore(store, OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, true, null);
        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(eq(EXCEPTION), isNull());

        // Second time it succeeds
        expectStore(store, OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, null);
        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());

        // Third time it has no data to flush so we won't get past beginFlush()
        assertFalse(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAlreadyFlushing() throws InterruptedException, TimeoutException {
        @SuppressWarnings("unchecked")
        final Callback<Void> callback = mock(Callback.class);
        // Trigger the send, but don't invoke the callback so we'll still be mid-flush
        CountDownLatch allowStoreCompleteCountdown = new CountDownLatch(1);
        expectStore(store, OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, allowStoreCompleteCountdown);

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        assertThrows(TimeoutException.class, () -> writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.doFlush(callback);
        assertThrows(TimeoutException.class, () -> writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        allowStoreCompleteCountdown.countDown();
        assertFalse(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCancelBeforeAwaitFlush() throws InterruptedException, TimeoutException {
        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        writer.cancelFlush();
    }

    @Test
    public void testCancelAfterAwaitFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        CountDownLatch allowStoreCompleteCountdown = new CountDownLatch(1);
        // In this test, the write should be cancelled so the callback will not be invoked and is not
        // passed to the expectStore call
        expectStore(store, OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, allowStoreCompleteCountdown);

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush(1000L, TimeUnit.MILLISECONDS));
        // Start the flush, then immediately cancel before allowing the mocked store request to finish
        Future<Void> flushFuture = writer.doFlush(callback);
        writer.cancelFlush();
        allowStoreCompleteCountdown.countDown();
        flushFuture.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushFailureWhenWriteToSecondaryStoreFailsForTombstoneOffsets() throws InterruptedException, TimeoutException, ExecutionException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", true);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        Future<Void> flushFuture = offsetStorageWriter.doFlush((error, result) -> {
            assertEquals(PRODUCE_EXCEPTION, error);
            assertNull(result);
        });
        assertThrows(ExecutionException.class, () -> flushFuture.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testFlushSuccessWhenWritesSucceedToBothPrimaryAndSecondaryStoresForTombstoneOffsets() throws InterruptedException, TimeoutException, ExecutionException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        offsetStorageWriter.doFlush((error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushSuccessWhenWriteToSecondaryStoreFailsForNonTombstoneOffsets() throws InterruptedException, TimeoutException, ExecutionException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", true);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        offsetStorageWriter.doFlush((error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryAndSecondaryStoreSucceeds() throws InterruptedException, TimeoutException, ExecutionException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        offsetStorageWriter.doFlush((error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceeds() throws InterruptedException, TimeoutException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", true);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        Future<Void> flushFuture = offsetStorageWriter.doFlush((error, result) -> {
            assertEquals(PRODUCE_EXCEPTION, error);
            assertNull(result);
        });
        assertThrows(ExecutionException.class, () -> flushFuture.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceedsForTombstoneRecords() throws InterruptedException, TimeoutException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", true);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        Future<Void> flushFuture = offsetStorageWriter.doFlush((error, result) -> {
            assertEquals(PRODUCE_EXCEPTION, error);
            assertNull(result);
        });
        assertThrows(ExecutionException.class, () -> flushFuture.get(1000L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryStoreSucceedsWithNoSecondaryStore() throws InterruptedException, TimeoutException, ExecutionException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        offsetStorageWriter.doFlush((error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsWithNoSecondaryStore() throws InterruptedException, TimeoutException {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", true);

        extractKeyValue(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        OffsetStorageWriter offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, NAMESPACE, keyConverter, valueConverter);

        offsetStorageWriter.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(offsetStorageWriter.beginFlush(1000L, TimeUnit.MILLISECONDS));
        Future<Void> flushFuture = offsetStorageWriter.doFlush((error, result) -> {
            assertEquals(PRODUCE_EXCEPTION, error);
            assertNull(result);
        });
        assertThrows(ExecutionException.class, () -> flushFuture.get(1000L, TimeUnit.MILLISECONDS));
    }

    @SuppressWarnings("unchecked")
    private KafkaOffsetBackingStore setupOffsetBackingStoreWithProducer(String topic, boolean throwingProducer) {
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(() -> mock(TopicAdmin.class), () -> "connect",  mock(Converter.class));
        KafkaBasedLog<byte[], byte[]> kafkaBasedLog = new KafkaBasedLog<byte[], byte[]>(topic, new HashMap<>(), new HashMap<>(),
                () -> mock(TopicAdmin.class), mock(Callback.class), new MockTime(), null);
        Whitebox.setInternalState(kafkaBasedLog, "producer", Optional.of(createProducer(throwingProducer)));
        offsetBackingStore.offsetLog = kafkaBasedLog;
        return offsetBackingStore;
    }


    private Producer<byte[], byte[]> createProducer(boolean throwingProducer) {
        if (throwingProducer) {
            return new MockProducer<byte[], byte[]>() {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final org.apache.kafka.clients.producer.Callback callback) {
                    callback.onCompletion(null, PRODUCE_EXCEPTION);
                    return null;
                }
            };
        }
        return new MockProducer<byte[], byte[]>() {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final org.apache.kafka.clients.producer.Callback callback) {
                callback.onCompletion(null, null);
                return null;
            }
        };
    }

    /**
     * Expect a request to store data to the underlying OffsetBackingStore.
     *
     * @param key the key for the offset
     * @param keySerialized serialized version of the key
     * @param value the value for the offset
     * @param valueSerialized serialized version of the value
     * @param fail if true, treat
     * @param waitForCompletion if non-null, a CountDownLatch that should be awaited on before
     *                          invoking the callback. A (generous) timeout is still imposed to
     *                          ensure tests complete.
     */
    @SuppressWarnings("unchecked")
    private void expectStore(OffsetBackingStore store, Map<String, Object> key, byte[] keySerialized,
                             Map<String, Object> value, byte[] valueSerialized,
                             final boolean fail,
                             final CountDownLatch waitForCompletion) {
        extractKeyValue(key, keySerialized, value, valueSerialized);

        final ArgumentCaptor<Callback<Void>> storeCallback = ArgumentCaptor.forClass(Callback.class);
        final Map<ByteBuffer, ByteBuffer> offsetsSerialized = Collections.singletonMap(
                keySerialized == null ? null : ByteBuffer.wrap(keySerialized),
                valueSerialized == null ? null : ByteBuffer.wrap(valueSerialized));
        when(store.set(eq(offsetsSerialized), storeCallback.capture())).thenAnswer(invocation -> {
            final Callback<Void> cb = invocation.getArgument(1);
            return service.submit(() -> {
                if (waitForCompletion != null)
                    assertTrue(waitForCompletion.await(10000, TimeUnit.MILLISECONDS));

                if (fail) {
                    cb.onCompletion(EXCEPTION, null);
                } else {
                    cb.onCompletion(null, null);
                }
                return null;
            });
        });
    }

    private void extractKeyValue(Map<String, Object> key, byte[] keySerialized, Map<String, Object> value, byte[] valueSerialized) {
        List<Object> keyWrapped = Arrays.asList(NAMESPACE, key);
        when(keyConverter.fromConnectData(NAMESPACE, null, keyWrapped)).thenReturn(keySerialized);
        when(valueConverter.fromConnectData(NAMESPACE, null, value)).thenReturn(valueSerialized);
    }

}
