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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.Optional;

@Evolving
public class WindowKeyQuery<K, V> implements Query<WindowStoreIterator<V>> {

    private final K key;
    private final Optional<Instant> timeFrom;
    private final Optional<Instant> timeTo;

    private WindowKeyQuery(final K key,
                           final Optional<Instant> timeTo,
                           final Optional<Instant> timeFrom) {
        this.key = key;
        this.timeFrom = timeFrom;
        this.timeTo = timeTo;
    }

    public static <K, V> WindowKeyQuery<K, V> withKeyAndWindowStartRange(final K key,
                                                                         final Instant timeFrom,
                                                                         final Instant timeTo) {
        return new WindowKeyQuery<>(key, Optional.of(timeFrom), Optional.of(timeTo));
    }

    public K getKey() {
        return key;
    }

    public Optional<Instant> getTimeFrom() {
        return timeFrom;
    }

    public Optional<Instant> getTimeTo() {
        return timeTo;
    }

    @Override
    public String toString() {
        return "WindowKeyQuery{" +
            "key=" + key +
            ", timeFrom=" + timeFrom +
            ", timeTo=" + timeTo +
            '}';
    }
}