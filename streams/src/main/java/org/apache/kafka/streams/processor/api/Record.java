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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.Objects;

/**
 * A data class representing an incoming record for processing in a {@link Processor}
 * or a record to forward to downstream processors via {@link ProcessorContext}.
 *
 * This class encapsulates all the data attributes of a record: the key and value, but
 * also the timestamp of the record and any record headers.
 *
 * This class is immutable, though the objects referenced in the attributes of this class
 * may themselves be mutable.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class Record<K, V> {
    private final K key;
    private final V value;
    private final long timestamp;
    private final Headers headers;

    /**
     * The full constructor, specifying all the attributes of the record.
     *
     * Note: this constructor makes a copy of the headers argument.
     * See {@link ProcessorContext#forward(Record)} for
     * considerations around mutability of keys, values, and headers.
     *
     * @param key The key of the record. May be null.
     * @param value The value of the record. May be null.
     * @param timestamp The timestamp of the record. May not be negative.
     * @param headers The headers of the record. May be null, which will cause subsequent calls
     *                to {@link #headers()} to return a non-null, empty, {@link Headers} collection.
     * @throws IllegalArgumentException if the timestamp is negative.
     * @see ProcessorContext#forward(Record)
     */
    public Record(final K key, final V value, final long timestamp, final Headers headers) {
        this.key = key;
        this.value = value;
        if (timestamp < 0) {
            throw new StreamsException(
                "Malformed Record",
                new IllegalArgumentException("Timestamp may not be negative. Got: " + timestamp)
            );
        }
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Convenience constructor in case you do not wish to specify any headers.
     * Subsequent calls to {@link #headers()} will return a non-null, empty, {@link Headers} collection.
     *
     * @param key The key of the record. May be null.
     * @param value The value of the record. May be null.
     * @param timestamp The timestamp of the record. May not be negative.
     *
     * @throws IllegalArgumentException if the timestamp is negative.
     */
    public Record(final K key, final V value, final long timestamp) {
        this(key, value, timestamp, null);
    }

    /**
     * The key of the record. May be null.
     */
    public K key() {
        return key;
    }

    /**
     * The value of the record. May be null.
     */
    public V value() {
        return value;
    }

    /**
     * The timestamp of the record. Will never be negative.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The headers of the record. Never null.
     */
    public Headers headers() {
        return headers;
    }

    /**
     * A convenient way to produce a new record if you only need to change the key.
     *
     * Copies the attributes of this record with the key replaced.
     *
     * @param key The key of the result record. May be null.
     * @param <NewK> The type of the new record's key.
     * @return A new Record instance with all the same attributes (except that the key is replaced).
     */
    public <NewK> Record<NewK, V> withKey(final NewK key) {
        return new Record<>(key, value, timestamp, headers);
    }

    /**
     * A convenient way to produce a new record if you only need to change the value.
     *
     * Copies the attributes of this record with the value replaced.
     *
     * @param value The value of the result record.
     * @param <NewV> The type of the new record's value.
     * @return A new Record instance with all the same attributes (except that the value is replaced).
     */
    public <NewV> Record<K, NewV> withValue(final NewV value) {
        return new Record<>(key, value, timestamp, headers);
    }

    /**
     * A convenient way to produce a new record if you only need to change the timestamp.
     *
     * Copies the attributes of this record with the timestamp replaced.
     *
     * @param timestamp The timestamp of the result record.
     * @return A new Record instance with all the same attributes (except that the timestamp is replaced).
     */
    public Record<K, V> withTimestamp(final long timestamp) {
        return new Record<>(key, value, timestamp, headers);
    }

    /**
     * A convenient way to produce a new record if you only need to change the headers.
     *
     * Copies the attributes of this record with the headers replaced.
     * Also makes a copy of the provided headers.
     *
     * See {@link ProcessorContext#forward(Record)} for
     * considerations around mutability of keys, values, and headers.
     *
     * @param headers The headers of the result record.
     * @return A new Record instance with all the same attributes (except that the headers are replaced).
     */
    public Record<K, V> withHeaders(final Headers headers) {
        return new Record<>(key, value, timestamp, headers);
    }

    @Override
    public String toString() {
        return "Record{" +
            "key=" + key +
            ", value=" + value +
            ", timestamp=" + timestamp +
            ", headers=" + headers +
            '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Record<?, ?> record = (Record<?, ?>) o;
        return timestamp == record.timestamp &&
            Objects.equals(key, record.key) &&
            Objects.equals(value, record.value) &&
            Objects.equals(headers, record.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, timestamp, headers);
    }
}
