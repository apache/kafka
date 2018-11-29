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
package org.apache.kafka.clients.producer;

import java.util.OptionalLong;

import org.apache.kafka.common.header.Header;

/**
 * A {@link ProducerRecord} that specifies an offset
 */
public class ProducerRecordWithOffset<K, V> extends ProducerRecord<K, V> {

    private final long offset;

    /**
     * Creates a record with a specified offset 
     * 
     * @param offset The offset that Kafka should use when appending this record to the log. For regular clients this is expected to be null.
     */
    public ProducerRecordWithOffset(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers, long offset) {
        super(topic, partition, timestamp, key, value, headers);
        if (offset < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid offset: %d. Offset should always be non-negative.", offset));

        this.offset = offset;
    }

    @Override
    public OptionalLong offset() {
        return OptionalLong.of(this.offset);
    }

    @Override
    public String toString() {
        String headers = this.headers() == null ? "null" : this.headers().toString();
        String key = this.key() == null ? "null" : this.key().toString();
        String value = this.value() == null ? "null" : this.value().toString();
        String timestamp = this.timestamp() == null ? "null" : this.timestamp().toString();
        return "ProducerRecordWithOffset(topic=" + topic() + ", partition=" + partition() + ", headers=" + headers + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ", offset=" + offset + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        ProducerRecordWithOffset<?, ?> other = (ProducerRecordWithOffset<?, ?>) obj;
        if (offset != other.offset)
            return false;
        return true;
    }

}
