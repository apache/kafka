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
import org.apache.kafka.common.utils.internals.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * An internal {@link Headers} carrier that holds headers already serialized in the Kafka record
 * wire format ({@code [count(varint)][keyLen(varint)][key][valueLen(varint)|-1][value]...}) and
 * defers deserialization until a read or mutation actually needs the individual headers.
 *
 * <p>This lets a producer that already has headers in serialized form (e.g. Kafka Streams changelog
 * writing) hand the bytes straight to the record batch via {@link #rawIfUnmodified()} without paying
 * for a deserialize/re-serialize round trip. Because the class fully implements {@link Headers} by
 * lazily materializing to a {@link RecordHeaders} on the first read/mutation, callers that inspect
 * or edit headers (interceptors, serializers, {@code ProducerRecord#headers()}) still observe the
 * real headers. Once materialized (or mutated), {@link #rawIfUnmodified()} returns {@code null} and
 * the producer falls back to the normal {@link #toArray()} write path — so the fast path is only
 * taken when the bytes were never touched.
 *
 * <p>This type carries no wire-format guarantees to callers and is not part of the public API; it is
 * only recognized internally by {@code KafkaProducer}. Any code path that does not recognize it sees
 * a fully functional {@link Headers}.
 */
public class PreSerializedHeaders implements Headers {

    private final byte[] serializedBytes;

    // Lazily created on first read/mutation. Once non-null, it is the source of truth and the raw
    // bytes are considered stale (rawIfUnmodified() returns null).
    private RecordHeaders materialized;

    public PreSerializedHeaders(final byte[] serializedBytes) {
        this.serializedBytes = serializedBytes == null ? new byte[0] : serializedBytes;
    }

    /**
     * @return the pre-serialized header bytes if they have not been read or mutated, otherwise
     *         {@code null}. A non-null result is safe to write verbatim into the record batch.
     */
    public byte[] rawIfUnmodified() {
        return materialized == null ? serializedBytes : null;
    }

    @Override
    public Headers add(final Header header) throws IllegalStateException {
        return materialize().add(header);
    }

    @Override
    public Headers add(final String key, final byte[] value) throws IllegalStateException {
        return materialize().add(key, value);
    }

    @Override
    public Headers remove(final String key) throws IllegalStateException {
        return materialize().remove(key);
    }

    @Override
    public Header lastHeader(final String key) {
        return materialize().lastHeader(key);
    }

    @Override
    public Iterable<Header> headers(final String key) {
        return materialize().headers(key);
    }

    @Override
    public Header[] toArray() {
        return materialize().toArray();
    }

    @Override
    public Iterator<Header> iterator() {
        return materialize().iterator();
    }

    private RecordHeaders materialize() {
        if (materialized == null) {
            materialized = deserialize(serializedBytes);
        }
        return materialized;
    }

    /**
     * Deserialize header bytes in the Kafka record wire format
     * ({@code [count(varint)][keyLen(varint)][key][valueLen(varint)|-1][value]...}) into a
     * {@link RecordHeaders}. Empty input (length 0) yields empty headers.
     */
    private static RecordHeaders deserialize(final byte[] data) {
        if (data.length == 0) {
            return new RecordHeaders();
        }

        final ByteBuffer buffer = ByteBuffer.wrap(data);
        final int count = ByteUtils.readVarint(buffer);
        final Header[] headers = new Header[count];
        for (int i = 0; i < count; i++) {
            final int keyLength = ByteUtils.readVarint(buffer);
            final byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            final int valueLength = ByteUtils.readVarint(buffer);
            final byte[] value;
            if (valueLength == -1) {
                value = null;
            } else {
                value = new byte[valueLength];
                buffer.get(value);
            }
            headers[i] = new RecordHeader(key, value);
        }
        return new RecordHeaders(headers);
    }

    @Override
    public String toString() {
        return "PreSerializedHeaders(materialized=" + (materialized != null) + ")";
    }
}
