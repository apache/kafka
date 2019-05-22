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

import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;

/**
 *
 * @Flower.min
 */
public class SimplifiedDefaultRecord implements Record {

    private final int sizeInBytes;
    private final byte attributes;
    private final long offset;
    private final long timestamp;
    private final int sequence;
    private final boolean isHasKey;

    public SimplifiedDefaultRecord(int sizeInBytes,
                                   byte attributes,
                                   long offset,
                                   long timestamp,
                                   int sequence,
                                   boolean isHasKey) {
        this.sizeInBytes = sizeInBytes;
        this.attributes = attributes;
        this.offset = offset;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.isHasKey = isHasKey;
    }

    public long offset() {
        return offset;
    }

    public int sequence() {
        return sequence;
    }

    public int sizeInBytes() {
        return sizeInBytes;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public Long checksumOrNull() {
        return null;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public void ensureValid() {

    }

    @Override
    public int keySize() {
        return 0;
    }

    @Override
    public boolean hasKey() {
        return isHasKey;
    }

    @Override
    public ByteBuffer key() {
        return null;
    }

    @Override
    public int valueSize() {
        return 0;
    }

    @Override
    public boolean hasValue() {
        return false;
    }

    @Override
    public ByteBuffer value() {
        return null;
    }

    @Override
    public boolean hasMagic(byte magic) {
        return magic >= MAGIC_VALUE_V2;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return false;
    }

    @Override
    public Header[] headers() {
        return new Header[0];
    }

    public byte attributes() {
        return attributes;
    }

    public boolean isHasKey() {
        return isHasKey;
    }

}
