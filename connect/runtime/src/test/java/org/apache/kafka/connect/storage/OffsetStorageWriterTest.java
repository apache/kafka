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
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

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
import java.util.concurrent.Callable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class OffsetStorageWriterTest {
    private static final String NAMESPACE = "namespace";
    // Connect format - any types should be accepted here
    private static final Map<String, String> OFFSET_KEY = Collections.singletonMap("key", "key");
    private static final Map<String, Integer> OFFSET_VALUE = Collections.singletonMap("key", 12);

    // Serialized
    private static final byte[] OFFSET_KEY_SERIALIZED = "key-serialized".getBytes();
    private static final byte[] OFFSET_VALUE_SERIALIZED = "value-serialized".getBytes();

    @Mock private OffsetBackingStore store;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    private OffsetStorageWriter writer;

    private static Exception exception = new RuntimeException("error");

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
        Callback<Void> callback = PowerMock.createMock(Callback.class);
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, callback, false, null);

        PowerMock.replayAll();

        writer.offset(OFFSET_KEY, OFFSET_VALUE);

        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    // It should be possible to set offset values to null
    @Test
    public void testWriteNullValueFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = PowerMock.createMock(Callback.class);
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null, callback, false, null);

        PowerMock.replayAll();

        writer.offset(OFFSET_KEY, null);

        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    // It should be possible to use null keys. These aren't actually stored as null since the key is wrapped to include
    // info about the namespace (connector)
    @Test
    public void testWriteNullKeyFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = PowerMock.createMock(Callback.class);
        expectStore(null, null, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, callback, false, null);

        PowerMock.replayAll();

        writer.offset(null, OFFSET_VALUE);

        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testNoOffsetsToFlush() {
        // If no offsets are flushed, we should finish immediately and not have made any calls to the
        // underlying storage layer

        PowerMock.replayAll();

        // Should not return a future
        assertFalse(writer.beginFlush());

        PowerMock.verifyAll();
    }

    @Test
    public void testFlushFailureReplacesOffsets() throws Exception {
        // When a flush fails, we shouldn't just lose the offsets. Instead, they should be restored
        // such that a subsequent flush will write them.

        @SuppressWarnings("unchecked")
        final Callback<Void> callback = PowerMock.createMock(Callback.class);
        // First time the write fails
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, callback, true, null);
        // Second time it succeeds
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, callback, false, null);
        // Third time it has no data to flush so we won't get past beginFlush()

        PowerMock.replayAll();

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        assertTrue(writer.beginFlush());
        writer.doFlush(callback).get(1000, TimeUnit.MILLISECONDS);
        assertFalse(writer.beginFlush());

        PowerMock.verifyAll();
    }

    @Test(expected = ConnectException.class)
    public void testAlreadyFlushing() throws Exception {
        @SuppressWarnings("unchecked")
        final Callback<Void> callback = PowerMock.createMock(Callback.class);
        // Trigger the send, but don't invoke the callback so we'll still be mid-flush
        CountDownLatch allowStoreCompleteCountdown = new CountDownLatch(1);
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, null, false, allowStoreCompleteCountdown);

        PowerMock.replayAll();

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        writer.doFlush(callback);
        assertTrue(writer.beginFlush()); // should throw

        PowerMock.verifyAll();
    }

    @Test
    public void testCancelBeforeAwaitFlush() {
        PowerMock.replayAll();

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        writer.cancelFlush();

        PowerMock.verifyAll();
    }

    @Test
    public void testCancelAfterAwaitFlush() throws Exception {
        @SuppressWarnings("unchecked")
        Callback<Void> callback = PowerMock.createMock(Callback.class);
        CountDownLatch allowStoreCompleteCountdown = new CountDownLatch(1);
        // In this test, the write should be cancelled so the callback will not be invoked and is not
        // passed to the expectStore call
        expectStore(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED, null, false, allowStoreCompleteCountdown);

        PowerMock.replayAll();

        writer.offset(OFFSET_KEY, OFFSET_VALUE);
        assertTrue(writer.beginFlush());
        // Start the flush, then immediately cancel before allowing the mocked store request to finish
        Future<Void> flushFuture = writer.doFlush(callback);
        writer.cancelFlush();
        allowStoreCompleteCountdown.countDown();
        flushFuture.get(1000, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    /**
     * Expect a request to store data to the underlying OffsetBackingStore.
     *
     * @param key the key for the offset
     * @param keySerialized serialized version of the key
     * @param value the value for the offset
     * @param valueSerialized serialized version of the value
     * @param callback the callback to invoke when completed, or null if the callback isn't
     *                 expected to be invoked
     * @param fail if true, treat
     * @param waitForCompletion if non-null, a CountDownLatch that should be awaited on before
     *                          invoking the callback. A (generous) timeout is still imposed to
     *                          ensure tests complete.
     * @return the captured set of ByteBuffer key-value pairs passed to the storage layer
     */
    private void expectStore(Map<String, String> key, byte[] keySerialized,
                             Map<String, Integer> value, byte[] valueSerialized,
                             final Callback<Void> callback,
                             final boolean fail,
                             final CountDownLatch waitForCompletion) {
        List<Object> keyWrapped = Arrays.asList(NAMESPACE, key);
        EasyMock.expect(keyConverter.fromConnectData(NAMESPACE, null, keyWrapped)).andReturn(keySerialized);
        EasyMock.expect(valueConverter.fromConnectData(NAMESPACE, null, value)).andReturn(valueSerialized);

        final Capture<Callback<Void>> storeCallback = Capture.newInstance();
        final Map<ByteBuffer, ByteBuffer> offsetsSerialized = Collections.singletonMap(
                keySerialized == null ? null : ByteBuffer.wrap(keySerialized),
                valueSerialized == null ? null : ByteBuffer.wrap(valueSerialized));
        EasyMock.expect(store.set(EasyMock.eq(offsetsSerialized), EasyMock.capture(storeCallback)))
                .andAnswer(new IAnswer<Future<Void>>() {
                    @Override
                    public Future<Void> answer() throws Throwable {
                        return service.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                if (waitForCompletion != null)
                                    assertTrue(waitForCompletion.await(10000, TimeUnit.MILLISECONDS));

                                if (fail) {
                                    storeCallback.getValue().onCompletion(exception, null);
                                } else {
                                    storeCallback.getValue().onCompletion(null, null);
                                }
                                return null;
                            }
                        });
                    }
                });
        if (callback != null) {
            if (fail) {
                callback.onCompletion(EasyMock.eq(exception), EasyMock.eq((Void) null));
            } else {
                callback.onCompletion(null, null);
            }
        }
        PowerMock.expectLastCall();
    }

}
