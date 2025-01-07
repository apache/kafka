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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * This is the abstract definition of the events created by the {@link ConsumerNetworkThread network thread}.
 */
public abstract class BackgroundEvent {

    public enum Type {
        ERROR, CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED, SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK
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

    protected BackgroundEvent(Type type) {
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
        BackgroundEvent that = (BackgroundEvent) o;
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
