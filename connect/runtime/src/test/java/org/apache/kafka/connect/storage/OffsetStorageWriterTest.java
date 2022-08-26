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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OffsetStorageWriterTest {
    private static final String NAMESPACE = "namespace";
    // Connect format - any types should be accepted here
    private static final Map<String, Object> OFFSET_KEY = Collections.singletonMap("key", "key");
    private static final Map<String, Object> OFFSET_VALUE = Collections.singletonMap("key", 12);

    // Serialized
    private static final byte[] OFFSET_KEY_SERIALIZED = "key-serialized".getBytes();
    private static final byte[] OFFSET_VALUE_SERIALIZED = "value-serialized".getBytes();

    private static final Exception EXCEPTION = new RuntimeException("error");

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
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, null);

        writer.offset(OFFSET_KEY, OFFSET_VALUE);

        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());
    }

    // It should be possible to set offset values to null
    @Test
    public void testWriteNullValueFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null, false, null);

        writer.offset(OFFSET_KEY, null);

        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());
    }

    // It should be possible to use null keys. These aren't actually stored as null since the key is wrapped to include
    // info about the namespace (connector)
    @Test
    public void testWriteNullKeyFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        expectStore(null, null, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, null);

        writer.offset(null, OFFSET_VALUE);

        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());
    }

    @Test
    public void testNoOffsetsToFlush() {
        assertFalse(writer.beginFlush());

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
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, true, null);
        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(eq(EXCEPTION), isNull());

        // Second time it succeeds
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, null);
        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        verify(callback).onCompletion(isNull(), isNull());

        // Third time it has no data to flush so we won't get past beginFlush()
        assertFalse(writer.beginFlush());
    }

    @Test
    public void testAlreadyFlushing() {
        @SuppressWarnings("unchecked")
        final Callback<Void> callback = mock(Callback.class);
        // Trigger the send, but don't invoke the callback so we'll still be mid-flush
        CountDownLatch allowStoreCompleteCountdown = new CountDownLatch(1);
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, allowStoreCompleteCountdown);

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        writer.doFlush(callback);
        assertThrows(ConnectException.class, writer::beginFlush);
    }

    @Test
    public void testCancelBeforeAwaitFlush() {
        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        writer.cancelFlush();
    }

    @Test
    public void testCancelAfterAwaitFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = mock(Callback.class);
        CountDownLatch allowStoreCompleteCountdown = new CountDownLatch(1);
        // In this test, the write should be cancelled so the callback will not be invoked and is not
        // passed to the expectStore call
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, false, allowStoreCompleteCountdown);

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        // Start the flush, then immediately cancel before allowing the mocked store request to finish
        Future<Void> flushFuture = writer.doFlush(callback);
        writer.cancelFlush();
        allowStoreCompleteCountdown.countDown();
        flushFuture.get(1000, TimeUnit.MILLISECONDS);
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
    private void expectStore(Map<String, Object> key, byte[] keySerialized,
                             Map<String, Object> value, byte[] valueSerialized,
                             final boolean fail,
                             final CountDownLatch waitForCompletion) {
        List<Object> keyWrapped = Arrays.asList(NAMESPACE, key);
        when(keyConverter.fromConnectData(NAMESPACE, null, keyWrapped)).thenReturn(keySerialized);
        when(valueConverter.fromConnectData(NAMESPACE, null, value)).thenReturn(valueSerialized);

        final ArgumentCaptor<Callback<Void>> storeCallback = ArgumentCaptor.forClass(Callback.class);
        final Map<ByteBuffer, ByteBuffer> offsetsSerialized = Collections.singletonMap(
                keySerialized == null ? null : ByteBuffer.wrap(keySerialized),
                valueSerialized == null ? null : ByteBuffer.wrap(valueSerialized));
        when(store.set(eq(offsetsSerialized), storeCallback.capture())).thenAnswer(invocation -> {
            final Callback<Void> cb = storeCallback.getValue();
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

}
