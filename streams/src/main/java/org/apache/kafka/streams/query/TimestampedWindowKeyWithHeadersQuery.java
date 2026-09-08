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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Interactive query for retrieving records of a single key across a window-start range, including
 * their record headers, from a {@link TimestampedWindowStoreWithHeaders}.
 *
 * <p>This is the headers-aware parallel of {@link WindowKeyQuery}: it returns a
 * {@link ReadOnlyRecordIterator} of {@link ReadOnlyRecord} elements, each carrying the windowed key,
 * value, timestamp, and headers, whereas {@link WindowKeyQuery} returns a
 * {@link org.apache.kafka.streams.state.WindowStoreIterator} of values keyed by window-start
 * timestamp (value only, no headers). Each element's key is a {@link Windowed} whose window
 * describes the record's window; the record's stored event-time is exposed via
 * {@link ReadOnlyRecord#timestamp()}.
 *
 * <p>As with {@link WindowKeyQuery}, a closed window-start range must be supplied (both
 * {@code timeFrom} and {@code timeTo}); open-ended ranges are not supported.
 *
 * <p>Headers are persisted and returned only when the store is backed by a native headers store,
 * i.e. built with a KIP-1271 {@code WithHeaders} byte-store supplier (e.g.
 * {@code Stores.persistentTimestampedWindowStoreWithHeaders}). A {@code WithHeaders} store built
 * over a legacy (non-headers) supplier cannot persist headers, so the store-served reads come back
 * with empty {@code headers()}.
 *
 * <p>Against a plain store not built with the {@code WithHeaders} builder at all, this query type is
 * unsupported and fails with {@link FailureReason#UNKNOWN_QUERY_TYPE}.
 *
 * <p>Each element is a {@link ReadOnlyRecord}, whose timestamp is non-negative. If the backing store
 * does not persist timestamps -- for example a {@code WithHeaders} store built over a plain
 * window-store supplier that surfaces entries with {@code NO_TIMESTAMP} ({@code -1}) -- that entry
 * cannot be represented, so advancing the returned {@link ReadOnlyRecordIterator} throws
 * {@link org.apache.kafka.streams.errors.StreamsException} at that entry. Back the store with
 * {@code Stores.persistentTimestampedWindowStoreWithHeaders(...)} to persist timestamps and headers,
 * or use {@link WindowKeyQuery} if headers are not needed.
 *
 * <p>Because iteration can throw mid-stream, always close the returned {@link ReadOnlyRecordIterator}
 * (for example with try-with-resources), even when a call to {@code next()} throws; otherwise the
 * underlying store iterator leaks and the {@code num-open-iterators} metric stays incremented.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
@InterfaceAudience.Public
public final class TimestampedWindowKeyWithHeadersQuery<K, V> implements Query<ReadOnlyRecordIterator<Windowed<K>, V>> {

    private final K key;
    private final Optional<Instant> timeFrom;
    private final Optional<Instant> timeTo;

    private TimestampedWindowKeyWithHeadersQuery(final K key,
                                                 final Optional<Instant> timeFrom,
                                                 final Optional<Instant> timeTo) {
        this.key = key;
        this.timeFrom = timeFrom;
        this.timeTo = timeTo;
    }

    /**
     * Creates a query that will retrieve the records (value, timestamp, and headers) identified by
     * {@code key} whose window start falls within the closed range {@code [timeFrom, timeTo]}.
     * @param key      The key to retrieve
     * @param timeFrom The inclusive lower bound of the window-start range
     * @param timeTo   The inclusive upper bound of the window-start range
     * @param <K>      The type of the key
     * @param <V>      The type of the value that will be retrieved
     */
    public static <K, V> TimestampedWindowKeyWithHeadersQuery<K, V> withKeyAndWindowStartRange(final K key,
                                                                                               final Instant timeFrom,
                                                                                               final Instant timeTo) {
        Objects.requireNonNull(key, "the key should not be null");
        return new TimestampedWindowKeyWithHeadersQuery<>(key, Optional.of(timeFrom), Optional.of(timeTo));
    }

    /**
     * The key that was specified for this query.
     */
    public K key() {
        return key;
    }

    /**
     * The inclusive lower bound of the window-start range, if specified.
     */
    public Optional<Instant> timeFrom() {
        return timeFrom;
    }

    /**
     * The inclusive upper bound of the window-start range, if specified.
     */
    public Optional<Instant> timeTo() {
        return timeTo;
    }

    @Override
    public String toString() {
        return "TimestampedWindowKeyWithHeadersQuery{" +
            "key=" + key +
            ", timeFrom=" + timeFrom +
            ", timeTo=" + timeTo +
            '}';
    }
}
