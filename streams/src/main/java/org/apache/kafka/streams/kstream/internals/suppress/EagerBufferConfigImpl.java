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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;

import java.util.Objects;

public class EagerBufferConfigImpl extends BufferConfigInternal<Suppressed.EagerBufferConfig> implements Suppressed.EagerBufferConfig {

    private final long maxRecords;
    private final long maxBytes;

    public EagerBufferConfigImpl(final long maxRecords, final long maxBytes) {
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
    }

    @Override
    public Suppressed.EagerBufferConfig withMaxRecords(final long recordLimit) {
        return new EagerBufferConfigImpl(recordLimit, maxBytes);
    }

    @Override
    public Suppressed.EagerBufferConfig withMaxBytes(final long byteLimit) {
        return new EagerBufferConfigImpl(maxRecords, byteLimit);
    }

    @Override
    public long maxRecords() {
        return maxRecords;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
    public BufferFullStrategy bufferFullStrategy() {
        return BufferFullStrategy.EMIT;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EagerBufferConfigImpl that = (EagerBufferConfigImpl) o;
        return maxRecords == that.maxRecords &&
            maxBytes == that.maxBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxRecords, maxBytes);
    }

    @Override
    public String toString() {
        return "EagerBufferConfigImpl{maxRecords=" + maxRecords + ", maxBytes=" + maxBytes + '}';
    }
}
