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
package org.apache.kafka.streams.kstream;

import java.util.Objects;
import org.apache.kafka.streams.header.Headers;
import org.apache.kafka.streams.header.StreamHeaders;

/**
 * Record value plus metadata (read-only) representation.
 *
 * @param <V> value type.
 */
public class RecordValue<V> {

    final String topic;
    final int partition;
    final long offset;
    final V value;
    final long timestamp;
    final Headers headers;

    public RecordValue(
        final String topic,
        final int partition,
        final long offset,
        final V value,
        final long timestamp,
        final org.apache.kafka.common.header.Headers headers
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
        this.headers = StreamHeaders.wrap(headers);
        this.timestamp = timestamp;
    }

    public RecordValue(
        final String topic,
        final int partition,
        final long offset,
        final V value,
        final long timestamp,
        final org.apache.kafka.common.header.Header[] headers
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
        this.headers = StreamHeaders.wrap(headers);
        this.timestamp = timestamp;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Headers headers() {
        return headers;
    }

    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RecordValue<?> that = (RecordValue<?>) o;
        return partition == that.partition && offset == that.offset && timestamp == that.timestamp
            && Objects.equals(topic, that.topic) && Objects
            .equals(value, that.value) && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, value, timestamp, headers);
    }

    @Override
    public String toString() {
        return "RecordValue(" +
            "topic='" + topic + '\'' +
            ",partition=" + partition +
            ",offset=" + offset +
            ",value=" + value +
            ",timestamp=" + timestamp +
            ",headers=" + headers +
            ')';
    }
}
