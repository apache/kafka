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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.requests.OffsetFetchResponse;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * The Kafka offset commit API allows users to provide additional metadata (in the form of a string)
 * when an offset is committed. This can be useful (for example) to store information about which
 * node made the commit, what time the commit was made, etc.
 */
public class OffsetAndMetadata implements Serializable {
    private static final long serialVersionUID = 2019555404968089681L;

    private final long offset;
    private final String metadata;

    // We use null to represent the absence of a leader epoch to simplify serialization.
    // I.e., older serializations of this class which do not have this field will automatically
    // initialize its value to null.
    private final Integer leaderEpoch;

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link KafkaConsumer}.
     *
     * @param offset The offset to be committed
     * @param leaderEpoch Optional leader epoch of the last consumed record
     * @param metadata Non-null metadata
     */
    public OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch, String metadata) {
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");

        this.offset = offset;
        this.leaderEpoch = leaderEpoch.orElse(null);

        // The server converts null metadata to an empty string. So we store it as an empty string as well on the client
        // to be consistent.
        if (metadata == null)
            this.metadata = OffsetFetchResponse.NO_METADATA;
        else
            this.metadata = metadata;
    }

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link KafkaConsumer}.
     * @param offset The offset to be committed
     * @param metadata Non-null metadata
     */
    public OffsetAndMetadata(long offset, String metadata) {
        this(offset, Optional.empty(), metadata);
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

    /**
     * Get the leader epoch of the previously consumed record (if one is known). Log truncation is detected
     * if there exists a leader epoch which is larger than this epoch and begins at an offset earlier than
     * the committed offset.
     *
     * @return the leader epoch or empty if not known
     */
    public Optional<Integer> leaderEpoch() {
        if (leaderEpoch == null || leaderEpoch < 0)
            return Optional.empty();
        return Optional.of(leaderEpoch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetAndMetadata that = (OffsetAndMetadata) o;
        return offset == that.offset &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, metadata, leaderEpoch);
    }

    @Override
    public String toString() {
        return "OffsetAndMetadata{" +
                "offset=" + offset +
                ", leaderEpoch=" + leaderEpoch +
                ", metadata='" + metadata + '\'' +
                '}';
    }

}
