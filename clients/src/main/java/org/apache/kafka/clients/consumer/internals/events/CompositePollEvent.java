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

import org.apache.kafka.clients.consumer.internals.Blocker;

public class CompositePollEvent extends ApplicationEvent {

    public enum State {

        OFFSET_COMMIT_CALLBACKS_REQUIRED,
        BACKGROUND_EVENT_PROCESSING_REQUIRED,
        COMPLETE
    }

    private final long deadlineMs;
    private final long pollTimeMs;
    private final Type nextEventType;
    private final Blocker<State> blocker;

    public CompositePollEvent(long deadlineMs, long pollTimeMs, Type nextEventType) {
        super(Type.COMPOSITE_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;
        this.nextEventType = nextEventType;
        this.blocker = new Blocker<>();
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public Type nextEventType() {
        return nextEventType;
    }

    public Blocker<State> blocker() {
        return blocker;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", deadlineMs=" + deadlineMs + ", pollTimeMs=" + pollTimeMs + ", nextEventType=" + nextEventType + ", blocker=" + blocker;
    }
}
