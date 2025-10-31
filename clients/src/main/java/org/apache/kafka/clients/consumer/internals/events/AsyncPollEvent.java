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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Optional;

/**
 * This class represents the non-blocking event that executes logic functionally equivalent to the following:
 *
 * <ul>
 *     <li>Polling</li>
 *     <li>{@link CheckAndUpdatePositionsEvent}</li>
 *     <li>{@link CreateFetchRequestsEvent}</li>
 * </ul>
 *
 * {@link AsyncKafkaConsumer#poll(Duration)} is implemented using a non-blocking design to ensure performance is
 * at the same level as {@link ClassicKafkaConsumer#poll(Duration)}. The event is submitted in {@code poll()}, but
 * there are no blocking waits for the "result" of the event. Checks are made for the result at certain points, but
 * they do not block. The logic for the previously-mentioned events is executed sequentially on the background thread.
 */
public class AsyncPollEvent extends ApplicationEvent implements MetadataErrorNotifiableEvent {

    private final long deadlineMs;
    private final long pollTimeMs;
    private volatile KafkaException error;
    private volatile boolean isComplete;
    private volatile boolean isValidatePositionsComplete;

    /**
     * Creates a new event to signify a multi-stage processing of {@link Consumer#poll(Duration)} logic.
     *
     * @param deadlineMs        Time, in milliseconds, at which point the event must be completed; based on the
     *                          {@link Duration} passed to {@link Consumer#poll(Duration)}
     * @param pollTimeMs        Time, in milliseconds, at which point the event was created
     */
    public AsyncPollEvent(long deadlineMs, long pollTimeMs) {
        super(Type.ASYNC_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public Optional<KafkaException> error() {
        return Optional.ofNullable(error);
    }

    public boolean isExpired(Time time) {
        return time.milliseconds() >= deadlineMs();
    }

    public boolean isValidatePositionsComplete() {
        return isValidatePositionsComplete;
    }

    public void markValidatePositionsComplete() {
        this.isValidatePositionsComplete = true;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void completeSuccessfully() {
        isComplete = true;
    }

    public void completeExceptionally(KafkaException e) {
        error = e;
        isComplete = true;
    }

    @Override
    public void onMetadataError(Exception metadataError) {
        completeExceptionally(ConsumerUtils.maybeWrapAsKafkaException(metadataError));
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
            ", deadlineMs=" + deadlineMs +
            ", pollTimeMs=" + pollTimeMs +
            ", error=" + error +
            ", isComplete=" + isComplete +
            ", isValidatePositionsComplete=" + isValidatePositionsComplete;
    }
}
