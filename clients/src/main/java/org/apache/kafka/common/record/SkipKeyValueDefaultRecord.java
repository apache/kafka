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

public class SkipKeyValueDefaultRecord extends DefaultRecord {

    private final boolean hasKey;
    private final int keySize;

    SkipKeyValueDefaultRecord(int sizeInBytes,
                              byte attributes,
                              long offset,
                              long timestamp,
                              int sequence,
                              int keySize,
                              boolean hasKey) {
        super(sizeInBytes, attributes, offset, timestamp, sequence, null, null, null);

        this.hasKey = hasKey;
        this.keySize = keySize;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) &&
            this.keySize == ((SkipKeyValueDefaultRecord) o).keySize &&
            this.hasKey == ((SkipKeyValueDefaultRecord) o).hasKey;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + keySize;
        result = 31 * result + (hasKey ? 0 : 1);
        return result;
    }

    @Override
    public int keySize() {
        return keySize;
    }

    @Override
    public boolean hasKey() {
        return hasKey;
    }

    @Override
    public ByteBuffer key() {
        throw new UnsupportedOperationException("key is skipped in SkipKeyValueDefaultRecord");
    }

    @Override
    public int valueSize() {
        throw new UnsupportedOperationException("valueSize is skipped in SkipKeyValueDefaultRecord");
    }

    @Override
    public boolean hasValue() {
        throw new UnsupportedOperationException("hasValue is skipped in SkipKeyValueDefaultRecord");
    }

    @Override
    public ByteBuffer value() {
        throw new UnsupportedOperationException("value is skipped in SkipKeyValueDefaultRecord");
    }

    @Override
    public Header[] headers() {
        throw new UnsupportedOperationException("headers is skipped in SkipKeyValueDefaultRecord");
    }
}
