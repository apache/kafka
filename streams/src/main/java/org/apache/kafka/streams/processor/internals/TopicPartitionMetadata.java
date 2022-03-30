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
import java.util.Base64;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata to be committed together with TopicPartition offset
 */
public class TopicPartitionMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(TopicPartitionMetadata.class);

    // visible for testing
    static final byte LATEST_MAGIC_BYTE = 2;

    private final long partitionTime;
    private final ProcessorMetadata processorMetadata;

    public TopicPartitionMetadata(final long partitionTime, final ProcessorMetadata processorMetadata) {
        Objects.requireNonNull(processorMetadata);
        this.partitionTime = partitionTime;
        this.processorMetadata = processorMetadata;
    }

    public long partitionTime() {
        return partitionTime;
    }

    public ProcessorMetadata processorMetadata() {
        return processorMetadata;
    }

    public String encode() {
        final byte[] serializedMeta = processorMetadata.serialize();
        // Format: MAGIC_BYTE(1) + PartitionTime(8) + processMeta
        final ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Long.BYTES + serializedMeta.length);
        buffer.put(LATEST_MAGIC_BYTE);
        buffer.putLong(partitionTime);
        buffer.put(serializedMeta);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    public static TopicPartitionMetadata decode(final String encryptedString) {
        long timestamp = RecordQueue.UNKNOWN;
        ProcessorMetadata metadata = new ProcessorMetadata();

        if (encryptedString.isEmpty()) {
            return new TopicPartitionMetadata(timestamp, metadata);
        }
        try {
            final ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedString));
            final byte version = buffer.get();
            switch (version) {
                case (byte) 1:
                    timestamp = buffer.getLong();
                    break;
                case LATEST_MAGIC_BYTE:
                    timestamp = buffer.getLong();
                    if (buffer.remaining() > 0) {
                        final byte[] metaBytes = new byte[buffer.remaining()];
                        buffer.get(metaBytes);
                        metadata = ProcessorMetadata.deserialize(metaBytes);
                    }
                    break;
                default:
                    LOG.warn(
                        "Unsupported offset metadata version found. Supported version <= {}. Found version {}.",
                        LATEST_MAGIC_BYTE, version);
            }
        } catch (final Exception exception) {
            LOG.warn("Unsupported offset metadata found");
        }
        return new TopicPartitionMetadata(timestamp, metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionTime, processorMetadata);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        return partitionTime == ((TopicPartitionMetadata) obj).partitionTime
            && Objects.equals(processorMetadata, ((TopicPartitionMetadata) obj).processorMetadata);
    }
}
