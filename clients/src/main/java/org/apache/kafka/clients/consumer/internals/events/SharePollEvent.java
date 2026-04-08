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

import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;

public class SharePollEvent extends ApplicationEvent {

    private final long deadlineMs;
    private final long pollTimeMs;
    private volatile boolean isComplete;

    /**
     * @param deadlineMs        Time, in milliseconds, at which point the event must be completed; based on the
     *                          {@link Duration} passed to {@link ShareConsumer#poll(Duration)}
     * @param pollTimeMs        Time, in milliseconds, at which point the event was created
     */
    public SharePollEvent(final long deadlineMs, final long pollTimeMs) {
        super(Type.SHARE_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public boolean isExpired(final Time time) {
        return time.milliseconds() >= deadlineMs();
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void completeSuccessfully() {
        isComplete = true;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() +
            ", deadlineMs=" + deadlineMs +
            ", pollTimeMs=" + pollTimeMs +
            ", isComplete=" + isComplete;
    }
}