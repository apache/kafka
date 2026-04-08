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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A lazy implementation of {@link Headers} that defers deserialization of header bytes
 * until first read access. This avoids unnecessary parsing when the downstream
 * deserializer does not inspect headers.
 *
 * <p>Headers added via {@link #add(Header)} or {@link #add(String, byte[])} before
 * materialization are accumulated in a side list and merged on first read access.
 *
 * <p>Instances are confined to a single {@code StreamThread} and are not shared
 * across threads, so no synchronization is needed.
 */
class LazyHeaders implements Headers {

    private final byte[] rawHeaders;
    private RecordHeaders materialized;
    private List<Header> pendingAdds;

    /**
     * Creates a new LazyHeaders wrapping the given raw header bytes.
     *
     * @param rawHeaders the serialized header bytes (without the varint size prefix),
     *                   as expected by {@link HeadersDeserializer#deserialize(byte[])}.
     *                   May be null or empty for empty headers.
     */
    LazyHeaders(final byte[] rawHeaders) {
        this.rawHeaders = rawHeaders;
    }

    private RecordHeaders materialize() {
        if (materialized == null) {
            final Headers deserialized = HeadersDeserializer.deserialize(rawHeaders);
            materialized = (deserialized instanceof RecordHeaders)
                ? (RecordHeaders) deserialized
                : new RecordHeaders(deserialized);
            if (pendingAdds != null) {
                for (final Header h : pendingAdds) {
                    materialized.add(h);
                }
                pendingAdds = null;
            }
        }
        return materialized;
    }

    /**
     * Returns true if the headers have been deserialized.
     * Visible for testing.
     */
    boolean isDeserialized() {
        return materialized != null;
    }

    @Override
    public Headers add(final Header header) throws IllegalStateException {
        Objects.requireNonNull(header, "header cannot be null");
        if (materialized != null) {
            materialized.add(header);
        } else {
            if (pendingAdds == null) {
                pendingAdds = new ArrayList<>();
            }
            pendingAdds.add(header);
        }
        return this;
    }

    @Override
    public Headers add(final String key, final byte[] value) throws IllegalStateException {
        return add(new RecordHeader(key, value));
    }

    @Override
    public Headers remove(final String key) throws IllegalStateException {
        materialize().remove(key);
        return this;
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Headers)) return false;
        final Headers other = (o instanceof LazyHeaders)
            ? ((LazyHeaders) o).materialize()
            : (Headers) o;
        return Arrays.equals(materialize().toArray(), other.toArray());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(materialize().toArray());
    }

    @Override
    public String toString() {
        if (materialized != null) {
            return materialized.toString();
        }
        return "LazyHeaders(not yet deserialized)";
    }
}
