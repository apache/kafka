/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

/**
 * A key-value pair defined for a single Kafka Streams record.
 * If the record comes directly from a Kafka topic then its key/value are defined as the message key/value.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class KeyValue<K, V> {

    /** The key of the key-value pair. */
    public final K key;
    /** The value of the key-value pair. */
    public final V value;

    /**
     * Create a new key-value pair.
     *
     * @param key   the key
     * @param value the value
     */
    public KeyValue(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Create a new key-value pair.
     *
     * @param key   the key
     * @param value the value
     * @param <K>   the type of the key
     * @param <V>   the type of the value
     * @return a new key-value pair
     */
    public static <K, V> KeyValue<K, V> pair(final K key, final V value) {
        return new KeyValue<>(key, value);
    }

    @Override
    public String toString() {
        return "KeyValue(" + key + ", " + value + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof KeyValue)) {
            return false;
        }

        final KeyValue other = (KeyValue) obj;
        return (key == null ? other.key == null : key.equals(other.key))
                && (value == null ? other.value == null : value.equals(other.value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

}
