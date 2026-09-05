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

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LazyHeadersTest {

    private byte[] serializeHeaders(final Headers headers) {
        return HeadersSerializer.serialize(headers);
    }

    @Test
    void shouldNotDeserializeOnConstruction() {
        final Headers original = new RecordHeaders()
            .add("key1", "value1".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);

        assertFalse(lazy.isDeserialized());
    }

    @Test
    void shouldDeserializeOnFirstReadAccess() {
        final Headers original = new RecordHeaders()
            .add("key1", "value1".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);
        assertFalse(lazy.isDeserialized());

        final Header[] array = lazy.toArray();
        assertTrue(lazy.isDeserialized());
        assertEquals(1, array.length);
        assertEquals("key1", array[0].key());
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), array[0].value());
    }

    @Test
    void shouldDeserializeOnIterator() {
        final Headers original = new RecordHeaders()
            .add("key1", "value1".getBytes(StandardCharsets.UTF_8))
            .add("key2", "value2".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);
        assertFalse(lazy.isDeserialized());

        final Iterator<Header> iter = lazy.iterator();
        assertTrue(lazy.isDeserialized());
        assertTrue(iter.hasNext());

        final Header h1 = iter.next();
        assertEquals("key1", h1.key());

        final Header h2 = iter.next();
        assertEquals("key2", h2.key());

        assertFalse(iter.hasNext());
    }

    @Test
    void shouldDeserializeOnLastHeader() {
        final Headers original = new RecordHeaders()
            .add("key", "v1".getBytes(StandardCharsets.UTF_8))
            .add("key", "v2".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);

        final Header last = lazy.lastHeader("key");
        assertTrue(lazy.isDeserialized());
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), last.value());
    }

    @Test
    void shouldDeserializeOnHeadersByKey() {
        final Headers original = new RecordHeaders()
            .add("key", "v1".getBytes(StandardCharsets.UTF_8))
            .add("other", "v2".getBytes(StandardCharsets.UTF_8))
            .add("key", "v3".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);

        int count = 0;
        for (final Header h : lazy.headers("key")) {
            count++;
            assertEquals("key", h.key());
        }
        assertEquals(2, count);
        assertTrue(lazy.isDeserialized());
    }

    @Test
    void shouldAddWithoutDeserializing() {
        final Headers original = new RecordHeaders()
            .add("existing", "value".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);
        lazy.add("new-key", "new-value".getBytes(StandardCharsets.UTF_8));

        assertFalse(lazy.isDeserialized());

        // Now access forces deserialization and merging
        final Header[] all = lazy.toArray();
        assertTrue(lazy.isDeserialized());
        assertEquals(2, all.length);
        assertEquals("existing", all[0].key());
        assertEquals("new-key", all[1].key());
    }

    @Test
    void shouldAddMultipleBeforeDeserialization() {
        final Headers original = new RecordHeaders()
            .add("h1", "v1".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);
        lazy.add("h2", "v2".getBytes(StandardCharsets.UTF_8));
        lazy.add("h3", "v3".getBytes(StandardCharsets.UTF_8));
        assertFalse(lazy.isDeserialized());

        final Header[] all = lazy.toArray();
        assertEquals(3, all.length);
        assertEquals("h1", all[0].key());
        assertEquals("h2", all[1].key());
        assertEquals("h3", all[2].key());
    }

    @Test
    void shouldAddAfterDeserialization() {
        final Headers original = new RecordHeaders()
            .add("existing", "value".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);

        // Force deserialization
        lazy.toArray();
        assertTrue(lazy.isDeserialized());

        // Add after deserialization
        lazy.add("post", "post-value".getBytes(StandardCharsets.UTF_8));

        final Header[] all = lazy.toArray();
        assertEquals(2, all.length);
        assertEquals("post", all[1].key());
    }

    @Test
    void shouldHandleRemove() {
        final Headers original = new RecordHeaders()
            .add("keep", "v1".getBytes(StandardCharsets.UTF_8))
            .add("remove", "v2".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);
        lazy.remove("remove");

        assertTrue(lazy.isDeserialized());
        assertEquals(1, lazy.toArray().length);
        assertEquals("keep", lazy.toArray()[0].key());
    }

    @Test
    void shouldHandleNullRawHeaders() {
        final LazyHeaders lazy = new LazyHeaders(null);
        assertFalse(lazy.isDeserialized());

        final Header[] all = lazy.toArray();
        assertTrue(lazy.isDeserialized());
        assertEquals(0, all.length);
    }

    @Test
    void shouldHandleEmptyRawHeaders() {
        final LazyHeaders lazy = new LazyHeaders(new byte[0]);
        assertFalse(lazy.isDeserialized());

        final Header[] all = lazy.toArray();
        assertTrue(lazy.isDeserialized());
        assertEquals(0, all.length);
    }

    @Test
    void shouldPreserveNullHeaderValues() {
        final Headers original = new RecordHeaders()
            .add("nullable", null);
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);
        final Header last = lazy.lastHeader("nullable");
        assertNotNull(last);
        assertNull(last.value());
    }

    @Test
    void shouldReturnCorrectToStringBeforeDeserialization() {
        final LazyHeaders lazy = new LazyHeaders(new byte[0]);
        assertEquals("LazyHeaders(not yet deserialized)", lazy.toString());
    }

    @Test
    void shouldReturnDelegateToStringAfterDeserialization() {
        final LazyHeaders lazy = new LazyHeaders(new byte[0]);
        lazy.toArray(); // force materialization
        assertNotNull(lazy.toString());
        assertFalse(lazy.toString().contains("not yet deserialized"));
    }

    @Test
    void shouldBeEqualToEquivalentRecordHeaders() {
        final Headers original = new RecordHeaders()
            .add("k1", "v1".getBytes(StandardCharsets.UTF_8));
        final byte[] raw = serializeHeaders(original);

        final LazyHeaders lazy = new LazyHeaders(raw);

        final RecordHeaders expected = new RecordHeaders();
        expected.add(new RecordHeader("k1", "v1".getBytes(StandardCharsets.UTF_8)));

        // LazyHeaders.equals(RecordHeaders) uses content-based comparison
        assertEquals(lazy, expected);
        // Also verify symmetric content equality via toArray
        assertArrayEquals(expected.toArray(), lazy.toArray());
    }

    @Test
    void shouldAddWithHeaderObject() {
        final LazyHeaders lazy = new LazyHeaders(null);

        lazy.add(new RecordHeader("key", "value".getBytes(StandardCharsets.UTF_8)));
        assertFalse(lazy.isDeserialized());

        final Header[] all = lazy.toArray();
        assertEquals(1, all.length);
        assertEquals("key", all[0].key());
    }
}
