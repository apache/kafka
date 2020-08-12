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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * Represents an interaction between a broker and a second-tier storage. This type of event is generated
 * by the {@link LocalTieredStorage} which is an implementation of the {@link RemoteStorageManager}
 * operating in Kafka's runtime as the interface between Kafka and external storage systems, through
 * which all such interactions go through.
 */
public final class LocalTieredStorageEvent implements Comparable<LocalTieredStorageEvent> {

    /**
     * The nature of the interaction.
     */
    public enum EventType {
        OFFLOAD_SEGMENT,
        FETCH_SEGMENT,
        FETCH_OFFSET_INDEX,
        FETCH_TIME_INDEX,
        DELETE_SEGMENT
    }

    private final EventType type;
    private final RemoteLogSegmentId segmentId;
    private final int timestamp;
    private final Optional<RemoteLogSegmentFileset> fileset;
    private final Optional<RemoteLogSegmentMetadata> metadata;
    private final Optional<Long> startPosition;
    private final Optional<Long> endPosition;
    private final Optional<Exception> exception;

    /**
     * Assess whether this event matches the characteristics of an event specified by the {@code condition}.
     *
     * @param condition The condition which contains the characteristics to match.
     * @return true if this event matches the condition's characteristics, false otherwise.
     */
    public boolean matches(final LocalTieredStorageCondition condition) {
        if (condition.eventType != type) {
            return false;
        }
        if (!segmentId.topicPartition().equals(condition.topicPartition)) {
            return false;
        }
        if (!exception.map(e -> condition.failed).orElseGet(() -> !condition.failed)) {
            return false;
        }
        return true;
    }

    /**
     * Returns whether the provided {@code event} was created after the present event.
     * This assumes a chronological ordering of events.
     * Both events need to be generated from the same broker.
     *
     * @param event The event to compare
     * @return true if the current instance was generated after the given {@code event},
     *         false if events are equal or the current instance was generated before the
     *         given {@code event}.
     */
    public boolean isAfter(final LocalTieredStorageEvent event) {
        return event.timestamp < timestamp;
    }

    public EventType getType() {
        return type;
    }

    public TopicPartition getTopicPartition() {
        return segmentId.topicPartition();
    }

    @Override
    public int compareTo(LocalTieredStorageEvent other) {
        requireNonNull(other);

        if (other.timestamp > timestamp) {
            return -1;
        }
        if (other.timestamp < timestamp) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return format("LocalTieredStorageEvent[type=%s brokerId=%s segmentId=%s timestamp=%s",
                type, segmentId, timestamp);
    }

    private LocalTieredStorageEvent(final Builder builder) {
        this.type = builder.eventType;
        this.segmentId = builder.segmentId;
        this.timestamp = builder.timestamp;
        this.fileset = ofNullable(builder.fileset);
        this.metadata = ofNullable(builder.metadata);
        this.startPosition = ofNullable(builder.startPosition);
        this.endPosition = ofNullable(builder.endPosition);
        this.exception = ofNullable(builder.exception);
    }

    public static Builder newBuilder(
            final EventType type, final int time, final RemoteLogSegmentId segmentId) {
        return new Builder(type, time, segmentId);
    }

    public static class Builder {
        private EventType eventType;
        private RemoteLogSegmentId segmentId;
        private int timestamp;
        private RemoteLogSegmentFileset fileset;
        private RemoteLogSegmentMetadata metadata;
        private Long startPosition;
        private Long endPosition;
        private Exception exception;

        public Builder withFileset(final RemoteLogSegmentFileset fileset) {
            this.fileset = fileset;
            return this;
        }

        public Builder withMetadata(final RemoteLogSegmentMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder withStartPosition(final Long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public Builder withEndPosition(final Long endPosition) {
            this.endPosition = endPosition;
            return this;
        }

        public Builder withException(final Exception exception) {
            this.exception = exception;
            return this;
        }

        public LocalTieredStorageEvent build() {
            return new LocalTieredStorageEvent(this);
        }

        private Builder(final EventType type, final int time, final RemoteLogSegmentId segId) {
            this.eventType = requireNonNull(type);
            this.timestamp = time;
            this.segmentId = requireNonNull(segId);
        }
    }
}
