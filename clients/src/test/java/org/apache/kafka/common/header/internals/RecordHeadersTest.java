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
package org.apache.kafka.common.header.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordHeadersTest {

    @Test
    public void testAdd() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));

        Header header = headers.iterator().next();
        assertHeader("key", "value", header);

        headers.add(new RecordHeader("key2", "value2".getBytes()));

        assertHeader("key2", "value2", headers.lastHeader("key2"));
        assertEquals(2, getCount(headers));
    }

    @Test
    public void testAddHeadersPreserveOrder() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));
        headers.add(new RecordHeader("key3", "value3".getBytes()));

        Header[] headersArr = headers.toArray();
        assertHeader("key", "value", headersArr[0]);
        assertHeader("key2", "value2", headersArr[1]);
        assertHeader("key3", "value3", headersArr[2]);

        assertEquals(3, getCount(headers));
    }

    @Test
    public void testRemove() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));

        assertTrue(headers.iterator().hasNext());

        headers.remove("key");

        assertFalse(headers.iterator().hasNext());
    }

    @Test
    public void testPreserveOrderAfterRemove() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));
        headers.add(new RecordHeader("key3", "value3".getBytes()));

        headers.remove("key");
        Header[] headersArr = headers.toArray();
        assertHeader("key2", "value2", headersArr[0]);
        assertHeader("key3", "value3", headersArr[1]);
        assertEquals(2, getCount(headers));

        headers.add(new RecordHeader("key4", "value4".getBytes()));
        headers.remove("key3");
        headersArr = headers.toArray();
        assertHeader("key2", "value2", headersArr[0]);
        assertHeader("key4", "value4", headersArr[1]);
        assertEquals(2, getCount(headers));
    }

    @Test
    public void testAddRemoveInterleaved() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));

        assertTrue(headers.iterator().hasNext());

        headers.remove("key");

        assertEquals(1, getCount(headers));

        headers.add(new RecordHeader("key3", "value3".getBytes()));

        assertNull(headers.lastHeader("key"));

        assertHeader("key2", "value2", headers.lastHeader("key2"));

        assertHeader("key3", "value3", headers.lastHeader("key3"));

        assertEquals(2, getCount(headers));

        headers.remove("key2");

        assertNull(headers.lastHeader("key"));

        assertNull(headers.lastHeader("key2"));

        assertHeader("key3", "value3", headers.lastHeader("key3"));

        assertEquals(1, getCount(headers));

        headers.add(new RecordHeader("key3", "value4".getBytes()));

        assertHeader("key3", "value4", headers.lastHeader("key3"));

        assertEquals(2, getCount(headers));

        headers.add(new RecordHeader("key", "valueNew".getBytes()));

        assertEquals(3, getCount(headers));


        assertHeader("key", "valueNew", headers.lastHeader("key"));

        headers.remove("key3");

        assertEquals(1, getCount(headers));

        assertNull(headers.lastHeader("key2"));

        headers.remove("key");

        assertFalse(headers.iterator().hasNext());
    }

    @Test
    public void testLastHeader() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key", "value2".getBytes()));
        headers.add(new RecordHeader("key", "value3".getBytes()));

        assertHeader("key", "value3", headers.lastHeader("key"));
        assertEquals(3, getCount(headers));

    }

    @Test
    public void testHeadersIteratorRemove() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));

        Iterator<Header> headersIterator = headers.headers("key").iterator();
        headersIterator.next();
        assertThrows(UnsupportedOperationException.class,
            headersIterator::remove);
    }

    @Test
    public void testReadOnly() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        Iterator<Header> headerIteratorBeforeClose = headers.iterator();
        headers.setReadOnly();

        assertThrows(IllegalStateException.class,
            () -> headers.add(new RecordHeader("key", "value".getBytes())),
            "IllegalStateException expected as headers are closed.");

        assertThrows(IllegalStateException.class,
            () -> headers.remove("key"),
            "IllegalStateException expected as headers are closed.");

        Iterator<Header> headerIterator = headers.iterator();
        headerIterator.next();

        assertThrows(IllegalStateException.class,
            headerIterator::remove,
            "IllegalStateException expected as headers are closed.");

        headerIteratorBeforeClose.next();

        assertThrows(IllegalStateException.class,
            headerIterator::remove,
            "IllegalStateException expected as headers are closed.");
    }

    @Test
    public void testHeaders() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key1", "key1value".getBytes()));
        headers.add(new RecordHeader("key", "value2".getBytes()));
        headers.add(new RecordHeader("key2", "key2value".getBytes()));


        Iterator<Header> keyHeaders = headers.headers("key").iterator();
        assertHeader("key", "value", keyHeaders.next());
        assertHeader("key", "value2", keyHeaders.next());
        assertFalse(keyHeaders.hasNext());

        keyHeaders = headers.headers("key1").iterator();
        assertHeader("key1", "key1value", keyHeaders.next());
        assertFalse(keyHeaders.hasNext());

        keyHeaders = headers.headers("key2").iterator();
        assertHeader("key2", "key2value", keyHeaders.next());
        assertFalse(keyHeaders.hasNext());

    }

    @Test
    public void testNew() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.setReadOnly();

        RecordHeaders newHeaders = new RecordHeaders(headers);
        newHeaders.add(new RecordHeader("key", "value2".getBytes()));

        //Ensure existing headers are not modified
        assertHeader("key", "value", headers.lastHeader("key"));
        assertEquals(1, getCount(headers));

        //Ensure new headers are modified
        assertHeader("key", "value2", newHeaders.lastHeader("key"));
        assertEquals(2, getCount(newHeaders));
    }

    @Test
    public void shouldThrowNpeWhenAddingNullHeader() {
        final RecordHeaders recordHeaders = new RecordHeaders();
        assertThrows(NullPointerException.class, () -> recordHeaders.add(null));
    }

    @Test
    public void shouldThrowNpeWhenAddingCollectionWithNullHeader() {
        assertThrows(NullPointerException.class, () -> new RecordHeaders(new Header[1]));
    }

    private int getCount(Headers headers) {
        return headers.toArray().length;
    }

    static void assertHeader(String key, String value, Header actual) {
        assertEquals(key, actual.key());
        assertArrayEquals(value.getBytes(), actual.value());
    }

    private void assertRecordHeaderReadThreadSafe(RecordHeader header) {
        int threadCount = 16;
        CountDownLatch startLatch = new CountDownLatch(1);

        var futures = IntStream.range(0, threadCount)
            .mapToObj(i -> CompletableFuture.runAsync(() -> {
                try {
                    startLatch.await();
                    header.key();
                    header.value();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            })).collect(Collectors.toUnmodifiableList());

        startLatch.countDown();
        futures.forEach(CompletableFuture::join);
    }

    @RepeatedTest(100)
    public void testRecordHeaderIsReadThreadSafe() throws Exception {
        RecordHeader header = new RecordHeader(
            ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value".getBytes(StandardCharsets.UTF_8))
        );
        assertRecordHeaderReadThreadSafe(header);
    }

    @RepeatedTest(100)
    public void testRecordHeaderWithNullValueIsReadThreadSafe() throws Exception {
        RecordHeader header = new RecordHeader(
            ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)),
            null
        );
        assertRecordHeaderReadThreadSafe(header);
    }
}
