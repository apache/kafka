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
package org.apache.kafka.streams;

import java.util.Objects;

public class KeyValueTimestamp<K, V> {
    private final K key;
    private final V value;
    private final long timestamp;

    public KeyValueTimestamp(final K key, final V value, final long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "KeyValueTimestamp{key=" + key + ", value=" + value + ", timestamp=" + timestamp + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final KeyValueTimestamp<?, ?> that = (KeyValueTimestamp<?, ?>) o;
        return timestamp == that.timestamp &&
            Objects.equals(key, that.key) &&
            Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, timestamp);
    }
}