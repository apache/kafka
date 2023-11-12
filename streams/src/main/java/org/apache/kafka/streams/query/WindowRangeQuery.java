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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.time.Instant;
import java.util.Optional;

public class WindowRangeQuery<K, V> implements Query<KeyValueIterator<Windowed<K>, V>> {

    private final Optional<K> lower;
    private final Optional<K> upper;
    private final Optional<K> key;
    private final Optional<Instant> oldTimeFrom;
    private final Optional<Instant> oldTimeTo;
    private final Optional<Instant> timeFrom;
    private final Optional<Instant> timeTo;

    private WindowRangeQuery(final Optional<K> lower,
                             final Optional<K> upper,
                             final Optional<Instant> oldTimeFrom,
                             final Optional<Instant> oldTimeTo,
                             final Optional<K> key,
                             final Optional<Instant> timeFrom,
                             final Optional<Instant> timeTo) {
        this.lower = lower;
        this.upper = upper;
        this.oldTimeFrom = oldTimeFrom;
        this.oldTimeTo = oldTimeTo;
        this.key = key;
        this.timeFrom = timeFrom;
        this.timeTo = timeTo;
    }

    public static <K, V> WindowRangeQuery<K, V> withKey(final K key) {
        return new WindowRangeQuery<>(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(key), Optional.empty(), Optional.empty());
    }

    public static <K, V> WindowRangeQuery<K, V> withWindowStartRange(final Instant timeFrom,
                                                                     final Instant timeTo) {
        return new WindowRangeQuery<>(Optional.empty(), Optional.empty(), Optional.of(timeFrom), Optional.of(timeTo), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static <K, V> WindowRangeQuery<K, V> withAllKey() {
        return new WindowRangeQuery<>(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }


    public WindowRangeQuery<K, V> fromTime(final Instant timeFrom) {
        return new WindowRangeQuery<>(lower, upper, Optional.empty(), Optional.empty(), Optional.empty(), Optional.ofNullable(timeFrom), timeTo);
    }


    public WindowRangeQuery<K, V> toTime(final Instant timeTo) {
        return new WindowRangeQuery<>(lower, upper, Optional.empty(), Optional.empty(), Optional.empty(), timeFrom, Optional.ofNullable(timeTo));
    }

    public static <K, V> WindowRangeQuery<K, V> withKeyRange(final K lower, final K upper) {
        return new WindowRangeQuery<>(Optional.ofNullable(lower), Optional.ofNullable(upper), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public Optional<K> lowerKeyBound() {
        return lower;
    }

    public Optional<K> upperKeyBound() {
        return upper;
    }

    //@Deprecated
    public Optional<Instant> getTimeFrom() {
        return oldTimeFrom;
    }

    //@Deprecated
    public Optional<Instant> getTimeTo() {
        return oldTimeTo;
    }

    public Optional<Instant> timeFrom() {
        return timeFrom;
    }

    public Optional<Instant> timeTo() {
        return timeTo;
    }

    public Optional<K> key() {
        return key;
    }

    @Override
    public String toString() {
        return "WindowRangeQuery{" +
            "lower=" + lower +
            ", upper=" + upper +
            ", oldTimeFrom=" + oldTimeFrom +
            ", oldTimeTo=" + oldTimeTo +
            ", key=" + key +
            ", timeFrom=" + timeFrom +
            ", timeTo=" + timeTo +
            '}';
    }
}
