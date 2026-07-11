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
import org.apache.kafka.common.utils.internals.ByteUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PreSerializedHeadersTest {

    /**
     * Serialize headers into the Kafka record wire format
     * ({@code [count(varint)][keyLen(varint)][key][valueLen(varint)|-1][value]...}) so the test
     * feeds PreSerializedHeaders bytes in exactly the layout it must parse.
     */
    private static byte[] serialize(final Header... headers) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream out = new DataOutputStream(baos);
            ByteUtils.writeVarint(headers.length, out);
            for (final Header header : headers) {
                final byte[] key = header.key().getBytes(StandardCharsets.UTF_8);
                ByteUtils.writeVarint(key.length, out);
                out.write(key);
                final byte[] value = header.value();
                if (value == null) {
                    ByteUtils.writeVarint(-1, out);
                } else {
                    ByteUtils.writeVarint(value.length, out);
                    out.write(value);
                }
            }
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void rawIfUnmodifiedReturnsOriginalBytesWhenUntouched() {
        final byte[] bytes = serialize(new RecordHeader("k1", "v1".getBytes()));
        final PreSerializedHeaders headers = new PreSerializedHeaders(bytes);

        // Same array reference is handed straight to the producer's raw write path.
        assertSame(bytes, headers.rawIfUnmodified());
        // Idempotent while still unread.
        assertSame(bytes, headers.rawIfUnmodified());
    }

    @Test
    public void nullBytesAreTreatedAsEmpty() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(null);
        assertNotNull(headers.rawIfUnmodified());
        assertEquals(0, headers.rawIfUnmodified().length);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void emptyBytesMaterializeToEmptyHeaders() {
        final byte[] bytes = serialize();
        final PreSerializedHeaders headers = new PreSerializedHeaders(bytes);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void toArrayInvalidatesRawFastPath() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        assertNotNull(headers.rawIfUnmodified());
        headers.toArray();
        assertNull(headers.rawIfUnmodified());
    }

    @Test
    public void iteratorInvalidatesRawFastPath() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        headers.iterator();
        assertNull(headers.rawIfUnmodified());
    }

    @Test
    public void lastHeaderInvalidatesRawFastPath() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        headers.lastHeader("k");
        assertNull(headers.rawIfUnmodified());
    }

    @Test
    public void headersLookupInvalidatesRawFastPath() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        headers.headers("k");
        assertNull(headers.rawIfUnmodified());
    }

    @Test
    public void addInvalidatesRawFastPath() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        headers.add("k2", "v2".getBytes());
        assertNull(headers.rawIfUnmodified());
    }

    @Test
    public void removeInvalidatesRawFastPath() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        headers.remove("k");
        assertNull(headers.rawIfUnmodified());
    }

    @Test
    public void materializesWireFormatBackToHeaders() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(
            new RecordHeader("k1", "v1".getBytes()),
            new RecordHeader("k2", "v2".getBytes())));

        final Header[] array = headers.toArray();
        assertEquals(2, array.length);
        assertEquals("k1", array[0].key());
        assertArrayEquals("v1".getBytes(), array[0].value());
        assertEquals("k2", array[1].key());
        assertArrayEquals("v2".getBytes(), array[1].value());
    }

    @Test
    public void materializesNullValuedHeader() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", (byte[]) null)));
        final Header[] array = headers.toArray();
        assertEquals(1, array.length);
        assertEquals("k", array[0].key());
        assertNull(array[0].value());
    }

    @Test
    public void preservesDuplicateKeysInOrder() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(
            new RecordHeader("k", "first".getBytes()),
            new RecordHeader("k", "second".getBytes())));

        final List<Header> matches = new ArrayList<>();
        headers.headers("k").forEach(matches::add);
        assertEquals(2, matches.size());
        assertArrayEquals("first".getBytes(), matches.get(0).value());
        assertArrayEquals("second".getBytes(), matches.get(1).value());
        // lastHeader returns the most recently added.
        assertArrayEquals("second".getBytes(), headers.lastHeader("k").value());
    }

    @Test
    public void addedHeaderIsVisibleAfterMaterialization() {
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k1", "v1".getBytes())));
        headers.add("k2", "v2".getBytes());

        final Header[] array = headers.toArray();
        assertEquals(2, array.length);
        assertEquals("k1", array[0].key());
        assertEquals("k2", array[1].key());
    }

    @Test
    public void materializedHeadersAreMutableAndThrowNothing() {
        // Ensure the materialized RecordHeaders is not read-only (add after materialize succeeds).
        final PreSerializedHeaders headers = new PreSerializedHeaders(serialize(new RecordHeader("k", "v".getBytes())));
        headers.toArray();
        // Should not throw; PreSerializedHeaders never marks itself read-only.
        headers.add("k2", "v2".getBytes());
        assertEquals(2, headers.toArray().length);
    }

    @Test
    public void malformedBytesThrowOnMaterialize() {
        // A header count that exceeds the available data should surface as an error rather than
        // silently producing wrong headers.
        final byte[] truncated = new byte[] {0x04}; // zigzag varint 4 -> count 2, but no header data follows
        final PreSerializedHeaders headers = new PreSerializedHeaders(truncated);
        assertThrows(RuntimeException.class, headers::toArray);
    }
}
