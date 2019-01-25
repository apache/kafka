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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.KeyValue;

import java.util.Objects;

/**
 * Combines a value from a {@link KeyValue} with a timestamp.
 *
 * @param <V>
 */
public class ValueAndTimestamp<V> {
    private final V value;
    private final long timestamp;

    private ValueAndTimestamp(final V value,
                              final long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public static <V> ValueAndTimestamp<V> make(final V value,
                                                final long timestamp) {
        return value == null ? null : new ValueAndTimestamp<>(value, timestamp);
    }

    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "<" + value + "," + timestamp + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ValueAndTimestamp<?> that = (ValueAndTimestamp<?>) o;
        return timestamp == that.timestamp &&
            Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp);
    }
}