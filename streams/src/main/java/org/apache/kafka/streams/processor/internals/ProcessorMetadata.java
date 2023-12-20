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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ProcessorMetadata to be access and populated by processor node. This will be committed along with
 * offset. This metadata is mainly for windowed aggregation processor to store last emitted timestamp
 * for now. Therefore, the supported metadata value type is only Long which is timestamp type.
 */
public class ProcessorMetadata {

    private final Map<String, Long> metadata;

    // Whether metadata should be committed. We only need to commit if metadata is updated via
    // put() or set explicitly
    private boolean needsCommit;

    public ProcessorMetadata() {
        this(new HashMap<>());
    }

    public ProcessorMetadata(final Map<String, Long> metadata) {
        this.metadata = metadata;
        needsCommit = false;
    }

    public static ProcessorMetadata deserialize(final byte[] metaDataBytes) {
        if (metaDataBytes == null || metaDataBytes.length == 0) {
            return new ProcessorMetadata();
        }

        final ByteBuffer buffer = ByteBuffer.wrap(metaDataBytes);
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

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final byte[] mapSizeBytes = ByteBuffer.allocate(Integer.BYTES).putInt(metadata.size()).array();
        outputStream.write(mapSizeBytes, 0, mapSizeBytes.length);

        for (final Map.Entry<String, Long> entry : metadata.entrySet()) {
            final byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            final int keyLen = keyBytes.length;
            final byte[] buffer = ByteBuffer.allocate(Integer.BYTES + keyBytes.length + Long.BYTES)
                .putInt(keyLen)
                .put(keyBytes)
                .putLong(entry.getValue())
                .array();
            outputStream.write(buffer, 0, buffer.length);
        }
        return outputStream.toByteArray();
    }

    public void put(final String key, final long value) {
        metadata.put(key, value);
        needsCommit = true;
    }

    public Long get(final String key) {
        return metadata.get(key);
    }

    /**
     * Merge with other metadata. Missing keys will be added. Existing key's value will be updated to
     * max
     * @param other Other metadata to be merged
     */
    public void update(final ProcessorMetadata other) {
        if (other == null) {
            return;
        }
        for (final Map.Entry<String, Long> kv : other.metadata.entrySet()) {
            final Long value = metadata.get(kv.getKey());
            if (value == null || value < kv.getValue()) {
                metadata.put(kv.getKey(), kv.getValue());
            }
        }
    }

    public void setNeedsCommit(final boolean needsCommit) {
        this.needsCommit = needsCommit;
    }

    /**
     * Whether metadata needs to be committed. It should be committed only if put is or
     * {@link #setNeedsCommit} is called explicitly
     *
     * @return If metadata needs to be committed.
     */
    public boolean needsCommit() {
        return needsCommit;
    }

    @Override
    public int hashCode() {
        // needsCommit is not considered in hashCode or equals
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
