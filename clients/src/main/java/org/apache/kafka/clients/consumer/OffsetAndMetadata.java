/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import java.io.Serializable;

/**
 * The Kafka offset commit API allows users to provide additional metadata (in the form of a string)
 * when an offset is committed. This can be useful (for example) to store information about which
 * node made the commit, what time the commit was made, etc.
 */
public class OffsetAndMetadata implements Serializable {
    private final long offset;
    private final String metadata;

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link KafkaConsumer}.
     * @param offset The offset to be committed
     * @param metadata Non-null metadata
     */
    public OffsetAndMetadata(long offset, String metadata) {
        if (metadata == null)
            throw new IllegalArgumentException("Metadata cannot be null");

        this.offset = offset;
        this.metadata = metadata;
    }

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link KafkaConsumer}. The metadata
     * associated with the commit will be empty.
     * @param offset The offset to be committed
     */
    public OffsetAndMetadata(long offset) {
        this(offset, "");
    }

    public long offset() {
        return offset;
    }

    public String metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetAndMetadata that = (OffsetAndMetadata) o;

        if (offset != that.offset) return false;
        return metadata == null ? that.metadata == null : metadata.equals(that.metadata);

    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "OffsetAndMetadata{" +
                "offset=" + offset +
                ", metadata='" + metadata + '\'' +
                '}';
    }
}
