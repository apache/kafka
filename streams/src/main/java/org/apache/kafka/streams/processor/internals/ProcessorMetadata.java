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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ProcessorMetadata to be access and populated by processor node. This will be committed along with
 * offset
 */
public class ProcessorMetadata {

    private final Map<String, Long> metadata;
    private boolean committed;

    public static ProcessorMetadata emptyMetadata() {
        return new ProcessorMetadata();
    }

    public static ProcessorMetadata with(final Map<String, Long> metadata) {
        return new ProcessorMetadata(metadata);
    }

    private ProcessorMetadata() {
        this(new HashMap<>());
    }

    private ProcessorMetadata(final Map<String, Long> metadata) {
        this.metadata = metadata;
        committed = false;
    }

    public static ProcessorMetadata deserialize(final byte[] metaData) {
        if (metaData == null || metaData.length == 0) {
            return new ProcessorMetadata();
        }

        final ByteBuffer buffer = ByteBuffer.wrap(metaData);
        final int entrySize = buffer.getInt();
        final Map<String, Long> metadata = new HashMap<>(entrySize);
        for (int i = 0; i < entrySize; i++) {
            final int keySize = buffer.getInt();
            final byte[] keyBytes = new byte[keySize];
            buffer.get(keyBytes);
            final Long value = buffer.getLong();
            metadata.put(new String(keyBytes, StandardCharsets.UTF_8), value);
        }
        return new ProcessorMetadata(metadata);
    }

    public byte[] serialize() {
        if (metadata.isEmpty()) {
            return new byte[0];
        }

        int kvSize = 0;
        for (final Map.Entry<String, Long> entry : metadata.entrySet()) {
            kvSize += Integer.BYTES;
            kvSize += entry.getKey().getBytes().length;
            kvSize += Long.BYTES;
        }

        final int capacity = Integer.BYTES + kvSize;
        final ByteBuffer buffer = ByteBuffer.allocate(capacity).putInt(metadata.size());
        for (final Map.Entry<String, Long> entry : metadata.entrySet()) {
            final byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            final int keyLen = keyBytes.length;
            buffer.putInt(keyLen)
                .put(keyBytes)
                .putLong(entry.getValue());
        }
        return buffer.array();
    }

    public void addMetadata(final String key, final long value) {
        metadata.put(key, value);
        committed = false;
    }

    public Long getMetadata(final String key) {
        return metadata.get(key);
    }

    public void commit() {
        committed = true;
    }

    public boolean needCommit() {
        return !committed;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(metadata);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        if (this == obj) {
            return true;
        }

        return metadata.equals(((ProcessorMetadata) obj).metadata);
    }
}
