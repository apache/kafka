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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ShareConsumerImpl;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * This is the abstract definition of the events created by the {@link AsyncKafkaConsumer} and
 * {@link ShareConsumerImpl} on the user's application thread.
 */
public abstract class ApplicationEvent {

    public enum Type {
        COMMIT_ASYNC, COMMIT_SYNC, POLL, FETCH_COMMITTED_OFFSETS, NEW_TOPICS_METADATA_UPDATE, ASSIGNMENT_CHANGE,
        LIST_OFFSETS, CHECK_AND_UPDATE_POSITIONS, RESET_OFFSET, TOPIC_METADATA, ALL_TOPICS_METADATA,
        TOPIC_SUBSCRIPTION_CHANGE, TOPIC_PATTERN_SUBSCRIPTION_CHANGE, TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE,
        UPDATE_SUBSCRIPTION_METADATA, UNSUBSCRIBE,
        CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED,
        COMMIT_ON_CLOSE, CREATE_FETCH_REQUESTS, LEAVE_GROUP_ON_CLOSE,
        PAUSE_PARTITIONS, RESUME_PARTITIONS, CURRENT_LAG,
        SHARE_FETCH, SHARE_ACKNOWLEDGE_ASYNC, SHARE_ACKNOWLEDGE_SYNC,
        SHARE_SUBSCRIPTION_CHANGE, SHARE_UNSUBSCRIBE,
        SHARE_ACKNOWLEDGE_ON_CLOSE,
        SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION,
        SEEK_UNVALIDATED,
    }

    private final Type type;

    /**
     * This identifies a particular event. It is used to disambiguate events via {@link #hashCode()} and
     * {@link #equals(Object)} and can be used in log messages when debugging.
     */
    private final Uuid id;

    /**
     * The time in milliseconds when this event was enqueued.
     * This field can be changed after the event is created, so it should not be used in hashCode or equals.
     */
    private long enqueuedMs;

    protected ApplicationEvent(Type type) {
        this.type = Objects.requireNonNull(type);
        this.id = Uuid.randomUuid();
    }

    public Type type() {
        return type;
    }

    public Uuid id() {
        return id;
    }

    public void setEnqueuedMs(long enqueuedMs) {
        this.enqueuedMs = enqueuedMs;
    }

    public long enqueuedMs() {
        return enqueuedMs;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplicationEvent that = (ApplicationEvent) o;
        return type == that.type && id.equals(that.id);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, id);
    }

    protected String toStringBase() {
        return "type=" + type + ", id=" + id + ", enqueuedMs=" + enqueuedMs;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "{" + toStringBase() + "}";
    }
}
