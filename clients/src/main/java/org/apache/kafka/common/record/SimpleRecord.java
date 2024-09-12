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
package org.apache.kafka.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * High-level representation of a kafka record. This is useful when building record sets to
 * avoid depending on a specific magic version.
 */
public class SimpleRecord {
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timestamp;
    private final Header[] headers;

    public SimpleRecord(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        Objects.requireNonNull(headers, "Headers must be non-null");
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    public SimpleRecord(long timestamp, byte[] key, byte[] value, Header[] headers) {
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    public SimpleRecord(long timestamp, ByteBuffer key, ByteBuffer value) {
        this(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public SimpleRecord(long timestamp, byte[] key, byte[] value) {
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value));
    }

    public SimpleRecord(long timestamp, byte[] value) {
        this(timestamp, null, value);
    }

    public SimpleRecord(byte[] value) {
        this(RecordBatch.NO_TIMESTAMP, null, value);
    }

    public SimpleRecord(ByteBuffer value) {
        this(RecordBatch.NO_TIMESTAMP, null, value);
    }

    public SimpleRecord(byte[] key, byte[] value) {
        this(RecordBatch.NO_TIMESTAMP, key, value);
    }

    public SimpleRecord(Record record) {
        this(record.timestamp(), record.key(), record.value(), record.headers());
    }

    public ByteBuffer key() {
        return key;
    }

    public ByteBuffer value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public Header[] headers() {
        return headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SimpleRecord that = (SimpleRecord) o;
        return timestamp == that.timestamp &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    @Override
    public String toString() {
        return String.format("SimpleRecord(timestamp=%d, key=%d bytes, value=%d bytes)",
                timestamp(),
                key == null ? 0 : key.limit(),
                value == null ? 0 : value.limit());
    }
}
