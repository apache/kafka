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
 * An immutable representation of an assignor within a consumer group member.
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
     * The metadata version reported by the assignor.
     */
    private final short metadataVersion;

    /**
     * The metadata raw bytes reported by the assignor.
     */
    private final ByteBuffer metadataBytes;

    public ClientAssignor(
        String name,
        byte reason,
        short minimumVersion,
        short maximumVersion,
        short metadataVersion,
        ByteBuffer metadataBytes
    ) {
        this.name = Objects.requireNonNull(name);
        this.reason = reason;
        this.minimumVersion = minimumVersion;
        this.maximumVersion = maximumVersion;
        this.metadataVersion = metadataVersion;
        this.metadataBytes = Objects.requireNonNull(metadataBytes);
    }

    public String name() {
        return this.name;
    }

    public byte reason() {
        return this.reason;
    }

    public short minimumVersion() {
        return this.minimumVersion;
    }

    public short maximumVersion() {
        return this.maximumVersion;
    }

    public short metadataVersion() {
        return this.metadataVersion;
    }

    public ByteBuffer metadataBytes() {
        return this.metadataBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientAssignor that = (ClientAssignor) o;

        if (reason != that.reason) return false;
        if (minimumVersion != that.minimumVersion) return false;
        if (maximumVersion != that.maximumVersion) return false;
        if (metadataVersion != that.metadataVersion) return false;
        if (!name.equals(that.name)) return false;
        return metadataBytes.equals(that.metadataBytes);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) reason;
        result = 31 * result + (int) minimumVersion;
        result = 31 * result + (int) maximumVersion;
        result = 31 * result + (int) metadataVersion;
        result = 31 * result + metadataBytes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClientAssignor(name=" + name +
            ", reason=" + reason +
            ", minimumVersion=" + minimumVersion +
            ", maximumVersion=" + maximumVersion +
            ", metadataVersion=" + metadataVersion +
            ", metadataBytes=" + metadataBytes +
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
            record.version(),
            ByteBuffer.wrap(record.metadata())
        );
    }
}
