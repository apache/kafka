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

import java.util.Objects;

/**
 * This is the abstract definition of the events created by the {@link ConsumerNetworkThread network thread}.
 */
public abstract class BackgroundEvent {

    public enum Type {
        ERROR,
        CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED,
        STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED,
        STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED,
        STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED
    }

    private final Type type;

    /**
     * The time in milliseconds when this event was enqueued.
     * This field can be changed after the event is created, so it should not be used in hashCode or equals.
     */
    private long enqueuedMs;

    protected BackgroundEvent(Type type) {
        this.type = Objects.requireNonNull(type);
    }

    public Type type() {
        return type;
    }

    public void setEnqueuedMs(long enqueuedMs) {
        this.enqueuedMs = enqueuedMs;
    }

    public long enqueuedMs() {
        return enqueuedMs;
    }

    protected String toStringBase() {
        return "type=" + type + ", enqueuedMs=" + enqueuedMs;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "{" + toStringBase() + "}";
    }
}
