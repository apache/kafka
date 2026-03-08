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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SerializedHeadersTest {

    // -------------------------------------------------------------------------
    // Helper: serialize headers to raw bytes using HeadersSerializer
    // -------------------------------------------------------------------------

    private static byte[] serialize(final Headers headers) {
        return HeadersSerializer.serialize(headers);
    }

    // =========================================================================
    // Construction and basic toArray()
    // =========================================================================

    @Test
    public void shouldReturnEmptyArrayForNullBytes() {
        final SerializedHeaders headers = new SerializedHeaders(null);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void shouldReturnEmptyArrayForEmptyBytes() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void shouldDeserializeSingleHeader() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        final Header[] array = headers.toArray();

        assertEquals(1, array.length);
        assertEquals("key1", array[0].key());
        assertArrayEquals("value1".getBytes(), array[0].value());
    }

    @Test
    public void shouldDeserializeMultipleHeaders() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());
        original.add("key2", "value2".getBytes());
        original.add("key3", "value3".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        final Header[] array = headers.toArray();

        assertEquals(3, array.length);
        assertEquals("key1", array[0].key());
        assertEquals("key2", array[1].key());
        assertEquals("key3", array[2].key());
        assertArrayEquals("value1".getBytes(), array[0].value());
        assertArrayEquals("value2".getBytes(), array[1].value());
        assertArrayEquals("value3".getBytes(), array[2].value());
    }

    @Test
    public void shouldHandleHeaderWithNullValue() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", null);

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        final Header[] array = headers.toArray();

        assertEquals(1, array.length);
        assertEquals("key1", array[0].key());
        assertNull(array[0].value());
    }

    @Test
    public void shouldPreserveHeaderOrder() {
        final RecordHeaders original = new RecordHeaders();
        original.add("z-key", "first".getBytes());
        original.add("a-key", "second".getBytes());
        original.add("m-key", "third".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        final Header[] array = headers.toArray();

        assertEquals("z-key", array[0].key());
        assertEquals("a-key", array[1].key());
        assertEquals("m-key", array[2].key());
    }

    @Test
    public void shouldHandleDuplicateKeys() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value-a".getBytes());
        original.add("key1", "value-b".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        final Header[] array = headers.toArray();

        assertEquals(2, array.length);
        assertEquals("key1", array[0].key());
        assertArrayEquals("value-a".getBytes(), array[0].value());
        assertEquals("key1", array[1].key());
        assertArrayEquals("value-b".getBytes(), array[1].value());
    }

    // =========================================================================
    // add() — lazy behavior (should NOT trigger deserialization)
    // =========================================================================

    @Test
    public void shouldAddHeaderWithoutDeserializing() {
        final RecordHeaders original = new RecordHeaders();
        original.add("existing", "data".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        // toString shows materialized=false before any read
        assertTrue(headers.toString().contains("materialized=false"));

        // add() should NOT trigger deserialization
        headers.add(new RecordHeader("new-key", "new-value".getBytes()));
        assertTrue(headers.toString().contains("materialized=false"));
        assertTrue(headers.toString().contains("pendingCount=1"));
    }

    @Test
    public void shouldAddMultiplePendingHeaders() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);

        headers.add("key1", "value1".getBytes());
        headers.add("key2", "value2".getBytes());
        headers.add("key3", "value3".getBytes());

        assertTrue(headers.toString().contains("pendingCount=3"));
    }

    @Test
    public void shouldCombineOriginalAndPendingOnToArray() {
        final RecordHeaders original = new RecordHeaders();
        original.add("existing", "data".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        headers.add("added1", "val1".getBytes());
        headers.add("added2", "val2".getBytes());

        final Header[] array = headers.toArray();

        assertEquals(3, array.length);
        assertEquals("existing", array[0].key());
        assertArrayEquals("data".getBytes(), array[0].value());
        assertEquals("added1", array[1].key());
        assertArrayEquals("val1".getBytes(), array[1].value());
        assertEquals("added2", array[2].key());
        assertArrayEquals("val2".getBytes(), array[2].value());
    }

    @Test
    public void shouldAddToEmptySerializedHeaders() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        headers.add("key1", "value1".getBytes());

        final Header[] array = headers.toArray();
        assertEquals(1, array.length);
        assertEquals("key1", array[0].key());
        assertArrayEquals("value1".getBytes(), array[0].value());
    }

    @Test
    public void shouldAddToNullSerializedHeaders() {
        final SerializedHeaders headers = new SerializedHeaders(null);
        headers.add("key1", "value1".getBytes());

        final Header[] array = headers.toArray();
        assertEquals(1, array.length);
        assertEquals("key1", array[0].key());
    }

    @Test
    public void shouldRejectNullHeaderInAdd() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        assertThrows(NullPointerException.class, () -> headers.add((Header) null));
    }

    @Test
    public void shouldReturnThisOnAdd() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        final Headers returned = headers.add("key", "value".getBytes());
        assertEquals(headers, returned);
    }

    // =========================================================================
    // Simulates the vector clock scenario (the main use case)
    // =========================================================================

    @Test
    public void shouldSupportVectorClockScenario() {
        // Simulate: store has headers from the record
        final RecordHeaders storeHeaders = new RecordHeaders();
        storeHeaders.add("correlation-id", "abc-123".getBytes());
        storeHeaders.add("schema-id", "42".getBytes());

        // Changelog store wraps raw bytes in SerializedHeaders (no deserialization)
        final SerializedHeaders headers = new SerializedHeaders(serialize(storeHeaders));

        // ProcessorContextImpl adds vector clock headers (no deserialization needed)
        headers.add("__changelog.version", new byte[]{1});
        headers.add("__changelog.position", "position-data".getBytes());

        // Producer calls toArray() — NOW deserialization happens
        final Header[] array = headers.toArray();

        assertEquals(4, array.length);
        assertEquals("correlation-id", array[0].key());
        assertEquals("schema-id", array[1].key());
        assertEquals("__changelog.version", array[2].key());
        assertEquals("__changelog.position", array[3].key());
    }

    // =========================================================================
    // iterator()
    // =========================================================================

    @Test
    public void shouldIterateOverAllHeaders() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());
        original.add("key2", "value2".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        headers.add("key3", "value3".getBytes());

        final List<String> keys = new ArrayList<>();
        for (final Header h : headers) {
            keys.add(h.key());
        }

        assertEquals(List.of("key1", "key2", "key3"), keys);
    }

    @Test
    public void shouldReturnEmptyIteratorForNoHeaders() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        assertFalse(headers.iterator().hasNext());
    }

    // =========================================================================
    // lastHeader()
    // =========================================================================

    @Test
    public void shouldFindLastHeaderByKey() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "first".getBytes());
        original.add("key1", "second".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        final Header last = headers.lastHeader("key1");
        assertNotNull(last);
        assertArrayEquals("second".getBytes(), last.value());
    }

    @Test
    public void shouldReturnNullForMissingKey() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        assertNull(headers.lastHeader("nonexistent"));
    }

    @Test
    public void shouldFindLastHeaderIncludingPending() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "original".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        headers.add("key1", "pending".getBytes());

        final Header last = headers.lastHeader("key1");
        assertNotNull(last);
        assertArrayEquals("pending".getBytes(), last.value());
    }

    @Test
    public void shouldRejectNullKeyInLastHeader() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        assertThrows(NullPointerException.class, () -> headers.lastHeader(null));
    }

    // =========================================================================
    // headers(String key)
    // =========================================================================

    @Test
    public void shouldFilterHeadersByKey() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "a".getBytes());
        original.add("key2", "b".getBytes());
        original.add("key1", "c".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        final List<Header> filtered = new ArrayList<>();
        headers.headers("key1").forEach(filtered::add);

        assertEquals(2, filtered.size());
        assertArrayEquals("a".getBytes(), filtered.get(0).value());
        assertArrayEquals("c".getBytes(), filtered.get(1).value());
    }

    @Test
    public void shouldReturnEmptyIterableForMissingKey() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        final List<Header> filtered = new ArrayList<>();
        headers.headers("nonexistent").forEach(filtered::add);
        assertTrue(filtered.isEmpty());
    }

    @Test
    public void shouldRejectNullKeyInHeaders() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        assertThrows(NullPointerException.class, () -> headers.headers(null));
    }

    // =========================================================================
    // remove()
    // =========================================================================

    @Test
    public void shouldRemoveHeadersByKey() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "a".getBytes());
        original.add("key2", "b".getBytes());
        original.add("key1", "c".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));
        headers.remove("key1");

        final Header[] array = headers.toArray();
        assertEquals(1, array.length);
        assertEquals("key2", array[0].key());
    }

    @Test
    public void shouldRejectNullKeyInRemove() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);
        assertThrows(NullPointerException.class, () -> headers.remove(null));
    }

    // =========================================================================
    // Caching and invalidation
    // =========================================================================

    @Test
    public void shouldCacheDeserializedResult() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        // First call triggers deserialization
        final Header[] first = headers.toArray();
        // Second call should return cached result
        final Header[] second = headers.toArray();

        assertEquals(first.length, second.length);
        assertEquals(first[0].key(), second[0].key());
    }

    @Test
    public void shouldInvalidateCacheOnAdd() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        // Materialize
        assertEquals(1, headers.toArray().length);

        // Add invalidates cache
        headers.add("key2", "value2".getBytes());
        assertEquals(2, headers.toArray().length);
    }

    @Test
    public void shouldHandleAddAfterToArray() {
        final RecordHeaders original = new RecordHeaders();
        original.add("key1", "value1".getBytes());

        final SerializedHeaders headers = new SerializedHeaders(serialize(original));

        // Materialize first
        assertEquals(1, headers.toArray().length);

        // Then add — should still work correctly
        headers.add("key2", "value2".getBytes());

        final Header[] array = headers.toArray();
        assertEquals(2, array.length);
        assertEquals("key1", array[0].key());
        assertEquals("key2", array[1].key());
    }

    // =========================================================================
    // toString()
    // =========================================================================

    @Test
    public void shouldShowMaterializationStateInToString() {
        final SerializedHeaders headers = new SerializedHeaders(new byte[0]);

        assertTrue(headers.toString().contains("materialized=false"));
        assertTrue(headers.toString().contains("pendingCount=0"));

        headers.add("key", "value".getBytes());
        assertTrue(headers.toString().contains("pendingCount=1"));

        headers.toArray(); // materialize
        assertTrue(headers.toString().contains("materialized=true"));
        assertTrue(headers.toString().contains("pendingCount=0"));
    }
}
