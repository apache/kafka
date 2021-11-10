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

import org.apache.kafka.streams.KeyValue;

import java.util.Objects;

/**
 * Combines a timestamped key from a {@link KeyValue} with a boolean value referencing if the key is
 * part of the left join (true) or right join (false). This class is only useful when a state
 * store needs to be shared between left and right processors, and each processor needs to
 * access the key of the other processor.
 *
 * Note that it might be cleaner to have two layers for such usages: first a KeyAndJoinSide, where the Key
 * is in the form of a <timestamp, key>; but with the nested structure serdes would need extra byte array copies.
 * Since it is only used in a single place today we decided to combine them into a single type / serde.
 */
public class TimestampedKeyAndJoinSide<K> {
    private final K key;
    private final long timestamp;
    private final boolean leftSide;

    private TimestampedKeyAndJoinSide(final boolean leftSide, final K key, final long timestamp) {
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.leftSide = leftSide;
        this.timestamp = timestamp;
    }

    /**
     * Create a new {@link TimestampedKeyAndJoinSide} instance if the provide {@code key} is not {@code null}.
     *
     * @param leftSide True if the key is part of the left join side; False if it is from the right join side
     * @param key      the key
     * @param <K>      the type of the key
     * @return a new {@link TimestampedKeyAndJoinSide} instance if the provide {@code key} is not {@code null}
     */
    public static <K> TimestampedKeyAndJoinSide<K> make(final boolean leftSide, final K key, final long timestamp) {
        return new TimestampedKeyAndJoinSide<>(leftSide, key, timestamp);
    }

    public boolean isLeftSide() {
        return leftSide;
    }

    public K getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        final String joinSide = leftSide ? "left" : "right";
        return "<" + joinSide + "," + key + ":" + timestamp + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TimestampedKeyAndJoinSide<?> that = (TimestampedKeyAndJoinSide<?>) o;
        return leftSide == that.leftSide &&
            Objects.equals(key, that.key) &&
            timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftSide, key, timestamp);
    }
}