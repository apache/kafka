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

import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * An immutable representation of a client side assignor within a consumer group member.
 */
public class ClientAssignor {
    /**
     * The name of the assignor.
     */
    private final String name;

    /**
     * The reason reported by the assignor.
     */
    private final byte reason;

    /**
     * The minimum metadata version supported by the assignor.
     */
    private final short minimumVersion;

    /**
     * The maximum metadata version supported by the assignor.
     */
    private final short maximumVersion;

    /**
     * The versioned metadata.
     */
    private final VersionedMetadata metadata;

    public ClientAssignor(
        String name,
        byte reason,
        short minimumVersion,
        short maximumVersion,
        VersionedMetadata metadata
    ) {
        this.name = Objects.requireNonNull(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Assignor name cannot be empty.");
        }
        this.reason = reason;
        this.minimumVersion = minimumVersion;
        if (minimumVersion < -1) {
            // -1 is supported as part of the upgrade from the old protocol to the new protocol. It
            // basically means that the assignor supports metadata from the old client assignor.
            throw new IllegalArgumentException("Assignor minimum version must be greater than -1.");
        }
        this.maximumVersion = maximumVersion;
        if (maximumVersion < 0) {
            throw new IllegalArgumentException("Assignor maximum version must be greater than or equals to 0.");
        } else if (maximumVersion < minimumVersion) {
            throw new IllegalArgumentException("Assignor maximum version must be greater than or equals to "
                + "the minimum version.");
        }
        this.metadata = Objects.requireNonNull(metadata);
    }

    /**
     * @return The client side assignor name.
     */
    public String name() {
        return this.name;
    }

    /**
     * @return The current reason reported by the assignor.
     */
    public byte reason() {
        return this.reason;
    }

    /**
     * @return The minimum version supported by the assignor.
     */
    public short minimumVersion() {
        return this.minimumVersion;
    }

    /**
     * @return The maximum version supported by the assignor.
     */
    public short maximumVersion() {
        return this.maximumVersion;
    }

    /**
     * @return The versioned metadata.
     */
    public VersionedMetadata metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientAssignor that = (ClientAssignor) o;

        if (reason != that.reason) return false;
        if (minimumVersion != that.minimumVersion) return false;
        if (maximumVersion != that.maximumVersion) return false;
        if (!name.equals(that.name)) return false;
        return metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) reason;
        result = 31 * result + (int) minimumVersion;
        result = 31 * result + (int) maximumVersion;
        result = 31 * result + metadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClientAssignor(name=" + name +
            ", reason=" + reason +
            ", minimumVersion=" + minimumVersion +
            ", maximumVersion=" + maximumVersion +
            ", metadata=" + metadata +
            ')';
    }

    public static ClientAssignor fromRecord(
        ConsumerGroupMemberMetadataValue.Assignor record
    ) {
        return new ClientAssignor(
            record.name(),
            record.reason(),
            record.minimumVersion(),
            record.maximumVersion(),
            new VersionedMetadata(
                record.version(),
                ByteBuffer.wrap(record.metadata())
            )
        );
    }
}
