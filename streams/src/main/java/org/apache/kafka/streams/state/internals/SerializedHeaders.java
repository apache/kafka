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
import org.apache.kafka.common.record.internal.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Lazy {@link Headers} that wraps pre-serialized header bytes and defers
 * deserialization until a read ({@link #toArray()}, {@link #iterator()}, etc.).
 * Used during changelog writing so that {@link #add(Header)} (e.g. for the
 * vector clock) does not force parsing of the existing serialized headers.
 */
class SerializedHeaders implements Headers {

    private final byte[] serializedBytes;
    private List<Header> pendingHeaders;
    private RecordHeadersList materialized;

    SerializedHeaders(final byte[] serializedBytes) {
        this.serializedBytes = serializedBytes;
    }

    @Override
    public Headers add(final Header header) throws IllegalStateException {
        Objects.requireNonNull(header, "Header cannot be null.");
        if (pendingHeaders == null) {
            pendingHeaders = new ArrayList<>();
        }
        pendingHeaders.add(header);
        materialized = null;
        return this;
    }

    @Override
    public Headers add(final String key, final byte[] value) throws IllegalStateException {
        return add(new RecordHeader(key, value));
    }

    @Override
    public Headers remove(final String key) throws IllegalStateException {
        Objects.requireNonNull(key, "key cannot be null.");
        materialize();
        materialized.removeAll(key);
        return this;
    }

    @Override
    public Header lastHeader(final String key) {
        Objects.requireNonNull(key, "key cannot be null.");
        final RecordHeadersList all = materialize();
        for (int i = all.size() - 1; i >= 0; i--) {
            if (all.get(i).key().equals(key)) {
                return all.get(i);
            }
        }
        return null;
    }

    @Override
    public Iterable<Header> headers(final String key) {
        Objects.requireNonNull(key, "key cannot be null.");
        final RecordHeadersList all = materialize();
        final List<Header> result = new ArrayList<>();
        for (final Iterator<Header> it = all.iterator(); it.hasNext(); ) {
            final Header header = it.next();
            if (header.key().equals(key)) {
                result.add(header);
            }
        }
        return result;
    }

    @Override
    public Header[] toArray() {
        final RecordHeadersList all = materialize();
        return all.isEmpty() ? Record.EMPTY_HEADERS : all.toArray(new Header[0]);
    }

    @Override
    public Iterator<Header> iterator() {
        return materialize().iterator();
    }

    private RecordHeadersList materialize() {
        if (materialized == null) {
            final Headers deserialized = HeadersDeserializer.deserialize(serializedBytes);
            materialized = new RecordHeadersList(deserialized);
            if (pendingHeaders != null) {
                materialized.addAll(pendingHeaders);
                pendingHeaders = null;
            }
        }
        return materialized;
    }

    @Override
    public String toString() {
        return "SerializedHeaders(" +
            "materialized=" + (materialized != null) +
            ", pendingCount=" + (pendingHeaders != null ? pendingHeaders.size() : 0) +
            ')';
    }

    private static final class RecordHeadersList {
        private final List<Header> headers;

        RecordHeadersList(final Headers source) {
            this.headers = new ArrayList<>(Arrays.asList(source.toArray()));
        }

        void addAll(final List<Header> additional) {
            headers.addAll(additional);
        }

        void removeAll(final String key) {
            headers.removeIf(h -> h.key().equals(key));
        }

        int size() {
            return headers.size();
        }

        boolean isEmpty() {
            return headers.isEmpty();
        }

        Header get(final int index) {
            return headers.get(index);
        }

        Header[] toArray(final Header[] array) {
            return headers.toArray(array);
        }

        Iterator<Header> iterator() {
            return headers.iterator();
        }
    }
}
