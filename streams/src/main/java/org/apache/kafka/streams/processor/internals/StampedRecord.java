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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Optional;

public class StampedRecord extends Stamped<ConsumerRecord<?, ?>> {

    private final byte[] rawKey;
    private final byte[] rawValue;
    private final Headers rawHeaders;

    public StampedRecord(final ConsumerRecord<?, ?> record, final long timestamp) {
        this(record, timestamp, null, null, null);
    }

    public StampedRecord(final ConsumerRecord<?, ?> record,
                         final long timestamp,
                         final byte[] rawKey,
                         final byte[] rawValue) {
        this(record, timestamp, rawKey, rawValue, null);
    }

    public StampedRecord(final ConsumerRecord<?, ?> record,
                         final long timestamp,
                         final byte[] rawKey,
                         final byte[] rawValue,
                         final Headers rawHeaders) {
        super(record, timestamp);
        this.rawKey = rawKey;
        this.rawValue = rawValue;
        this.rawHeaders = rawHeaders;
    }

    public String topic() {
        return value.topic();
    }

    public int partition() {
        return value.partition();
    }

    public Object key() {
        return value.key();
    }

    public Object value() {
        return value.value();
    }

    public long offset() {
        return value.offset();
    }

    public Optional<Integer> leaderEpoch() {
        return value.leaderEpoch();
    }

    public Headers headers() {
        return value.headers();
    }

    public byte[] rawKey() {
        return rawKey;
    }

    public byte[] rawValue() {
        return rawValue;
    }

    public Headers rawHeaders() {
        return rawHeaders;
    }

    @Override
    public String toString() {
        return value.toString() + ", timestamp = " + timestamp;
    }

    @Override
    public boolean equals(final Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
