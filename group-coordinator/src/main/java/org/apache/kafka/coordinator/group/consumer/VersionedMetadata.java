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
package org.apache.kafka.coordinator.group.consumer;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Immutable versioned metadata. It contains a bunch of bytes tagged with a version. The
 * format of the bytes is unspecified. This is mainly used by client side assignors to
 * exchange arbitrary metadata between the members and the assignor and vice versa.
 */
public class VersionedMetadata {
    public static final VersionedMetadata EMPTY = new VersionedMetadata((short) 0, ByteBuffer.allocate(0));

    /**
     * The version of the metadata encoded in {{@link VersionedMetadata#metadata}}.
     */
    private final short version;

    /**
     * The metadata bytes.
     */
    private final ByteBuffer metadata;

    public VersionedMetadata(
        short version,
        ByteBuffer metadata
    ) {
        this.version = version;
        this.metadata = Objects.requireNonNull(metadata);
    }

    /**
     * @return The version of the metadata.
     */
    public short version() {
        return this.version;
    }

    /**
     * @return The ByteBuffer holding the metadata.
     */
    public ByteBuffer metadata() {
        return this.metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VersionedMetadata that = (VersionedMetadata) o;

        if (version != that.version) return false;
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = version;
        result = 31 * result + metadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "VersionedMetadata(" +
            "version=" + version +
            ", metadata=" + metadata +
            ')';
    }
}
