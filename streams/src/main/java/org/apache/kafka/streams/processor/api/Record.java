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

public class Record<K, V> {
    private final K key;
    private final V value;
    private final long timestamp;
    private final Headers headers;

    public Record(final K key, final V value, final long timestamp, final Headers headers) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    public Record(final K key, final V value, final long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders();
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

    public Headers headers() {
        return headers;
    }

    public <NewK> Record<NewK, V> withKey(final NewK key) {
        return new Record<>(key, value, timestamp, headers);
    }

    public <NewV> Record<K, NewV> withValue(final NewV value) {
        return new Record<>(key, value, timestamp, headers);
    }

    public Record<K, V> withTimestamp(final long timestamp) {
        return new Record<>(key, value, timestamp, headers);
    }

    public Record<K, V> withHeaders(final Headers headers) {
        return new Record<>(key, value, timestamp, headers);
    }
}
