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
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;

import java.time.Instant;
import java.util.Optional;

/**
 * Interactive query for retrieving records across a window-start range or for a single key's
 * sessions, including their record headers, from a {@link TimestampedWindowStoreWithHeaders} or a
 * {@link SessionStoreWithHeaders}.
 *
 * <p>This is the headers-aware parallel of {@link WindowRangeQuery}: it returns a
 * {@link ReadOnlyRecordIterator} of {@link ReadOnlyRecord} elements, each carrying the windowed key,
 * value, timestamp, and headers, whereas {@link WindowRangeQuery} returns a plain
 * {@link org.apache.kafka.streams.state.KeyValueIterator} of values (no headers). Like
 * {@link WindowRangeQuery}, this query has two mutually exclusive forms:
 *
 * <ul>
 *     <li>{@link #withWindowStartRange(Instant, Instant)} is handled by window stores: it retrieves
 *     every key whose window start falls within the closed range {@code [timeFrom, timeTo]}. Each
 *     element's key is a {@link Windowed} describing the record's window, and
 *     {@link ReadOnlyRecord#timestamp()} is the stored record's event-time. A stored event-time is
 *     contractually non-negative; if the backing store does not persist timestamps -- for example a
 *     {@code WithHeaders} store built over a plain window-store supplier that surfaces entries with
 *     {@code NO_TIMESTAMP} ({@code -1}) -- that entry cannot be represented, so advancing the
 *     returned {@link ReadOnlyRecordIterator} throws
 *     {@link org.apache.kafka.streams.errors.StreamsException} at that entry. Because iteration can
 *     therefore throw mid-stream, always close the returned iterator (for example with
 *     try-with-resources), even when a call to {@code next()} throws; otherwise the underlying store
 *     iterator leaks and the {@code num-open-iterators} metric stays incremented.</li>
 *
 *     <li>{@link #withKey(Object)} is handled by session stores: it retrieves every session for the
 *     given key. Each element's key is a {@link Windowed} whose window is the session's window.
 *     Session aggregations carry no per-record event-time of their own, so
 *     {@link ReadOnlyRecord#timestamp()} is filled from the session window's (inclusive) end
 *     timestamp. That value is validated non-negative when the window is constructed, so -- unlike
 *     the window-store form above -- this form can never throw while iterating.</li>
 * </ul>
 *
 * <p>Submitting the {@code withWindowStartRange} form to a session store, or the {@code withKey} form
 * to a window store, fails with {@link FailureReason#UNKNOWN_QUERY_TYPE}.
 *
 * <p>Headers are persisted and returned only when the store is backed by a native headers store,
 * i.e. built with a KIP-1271 {@code WithHeaders} byte-store supplier (e.g.
 * {@code Stores.persistentTimestampedWindowStoreWithHeaders} or
 * {@code Stores.persistentSessionStoreWithHeaders}). A {@code WithHeaders} store built over a legacy
 * (non-headers) supplier cannot persist headers, so the store-served reads come back with empty
 * {@code headers()}.
 *
 * <p>Against a plain store not built with the {@code WithHeaders} builder at all, this query type is
 * unsupported and fails with {@link FailureReason#UNKNOWN_QUERY_TYPE}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
@InterfaceAudience.Public
public final class TimestampedWindowRangeWithHeadersQuery<K, V> implements Query<ReadOnlyRecordIterator<Windowed<K>, V>> {

    private final Optional<K> key;
    private final Optional<Instant> timeFrom;
    private final Optional<Instant> timeTo;

    private TimestampedWindowRangeWithHeadersQuery(final Optional<K> key,
                                                    final Optional<Instant> timeFrom,
                                                    final Optional<Instant> timeTo) {
        this.key = key;
        this.timeFrom = timeFrom;
        this.timeTo = timeTo;
    }

    /**
     * Creates a query that will retrieve the records (value, timestamp, and headers) of every key
     * whose window start falls within the closed range {@code [timeFrom, timeTo]}. Handled by window
     * stores.
     * @param timeFrom The inclusive lower bound of the window-start range
     * @param timeTo   The inclusive upper bound of the window-start range
     * @param <K>      The type of the key
     * @param <V>      The type of the value that will be retrieved
     */
    public static <K, V> TimestampedWindowRangeWithHeadersQuery<K, V> withWindowStartRange(final Instant timeFrom,
                                                                                            final Instant timeTo) {
        return new TimestampedWindowRangeWithHeadersQuery<>(Optional.empty(), Optional.of(timeFrom), Optional.of(timeTo));
    }

    /**
     * Creates a query that will retrieve the sessions (aggregation, timestamp, and headers) for the
     * given {@code key}. Handled by session stores.
     * @param key The key to retrieve
     * @param <K> The type of the key
     * @param <V> The type of the value that will be retrieved
     */
    public static <K, V> TimestampedWindowRangeWithHeadersQuery<K, V> withKey(final K key) {
        return new TimestampedWindowRangeWithHeadersQuery<>(Optional.of(key), Optional.empty(), Optional.empty());
    }

    /**
     * The key that was specified for this query, if this is a {@link #withKey(Object)} query.
     */
    public Optional<K> key() {
        return key;
    }

    /**
     * The inclusive lower bound of the window-start range, if this is a
     * {@link #withWindowStartRange(Instant, Instant)} query.
     */
    public Optional<Instant> timeFrom() {
        return timeFrom;
    }

    /**
     * The inclusive upper bound of the window-start range, if this is a
     * {@link #withWindowStartRange(Instant, Instant)} query.
     */
    public Optional<Instant> timeTo() {
        return timeTo;
    }

    @Override
    public String toString() {
        return "TimestampedWindowRangeWithHeadersQuery{" +
            "key=" + key +
            ", timeFrom=" + timeFrom +
            ", timeTo=" + timeTo +
            '}';
    }
}
