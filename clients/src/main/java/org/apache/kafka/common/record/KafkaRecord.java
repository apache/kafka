/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;

/**
 * High-level representation of a record from the client perspective which abstracts the low-level log representation.
 * This is mainly to facilitate generic testing (it's easier to verify the fields collectively rather than individually).
 */
public class KafkaRecord {

    private final byte[] key;
    private final byte[] value;
    private final long timestamp;

    public KafkaRecord(long timestamp,
                       byte[] key,
                       byte[] value) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public KafkaRecord(long timestamp,
                       byte[] value) {
        this(timestamp, null, value);
    }

    public KafkaRecord(byte[] value) {
        this(Record.NO_TIMESTAMP, null, value);
    }

    public KafkaRecord(byte[] key, byte[] value) {
        this(Record.NO_TIMESTAMP, key, value);
    }

    public KafkaRecord(LogRecord logRecord) {
        this(logRecord.timestamp(), Utils.toNullableArray(logRecord.key()), Utils.toNullableArray(logRecord.value()));
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KafkaRecord that = (KafkaRecord) o;

        if (timestamp != that.timestamp) return false;
        if (!Arrays.equals(key, that.key)) return false;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
