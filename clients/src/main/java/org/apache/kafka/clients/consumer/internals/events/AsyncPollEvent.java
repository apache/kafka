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
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class represents the non-blocking event that executes logic functionally equivalent to the following:
 *
 * <ul>
 *     <li>{@link SharePollEvent}</li>
 *     <li>{@link UpdatePatternSubscriptionEvent}</li>
 *     <li>{@link CheckAndUpdatePositionsEvent}</li>
 *     <li>{@link CreateFetchRequestsEvent}</li>
 * </ul>
 *
 * {@link AsyncKafkaConsumer#poll(Duration)} is implemented using a non-blocking design to ensure performance is
 * at the same level as {@link ClassicKafkaConsumer#poll(Duration)}. The event is submitted in {@code poll()}, but
 * there are no blocking waits for the "result" of the event. Checks are made for the result at certain points, but
 * they do not block. The logic for the previously-mentioned events is executed sequentially on the background thread.
 *
 * <p/>
 *
 * When the {@code AsyncPollEvent} is created, it exists in the {@link State#STARTED} state. The background
 * thread will execute the {@code AsyncPollEvent} until it completes successfully ({@link State#SUCCEEDED}),
 * hits an error ({@link State#FAILED}), or detects that the application thread needs to execute callbacks
 * ({@link State#CALLBACKS_REQUIRED}).
 *
 * <p/>
 *
 * It's possible that the background processing of the polling will need to be "paused" in order to execute a
 * {@link ConsumerInterceptor}, {@link ConsumerRebalanceListener}, and/or {@link OffsetCommitCallback} in the
 * application thread. The background thread is able to detect when it needs to complete processing so that the
 * application thread can execute the awaiting callbacks.
 */
public class AsyncPollEvent extends ApplicationEvent implements MetadataErrorNotifiable {

    public enum State {

        STARTED,
        SUCCEEDED,
        FAILED,
        CALLBACKS_REQUIRED
    }

    public static class Result {

        /**
         * This string value is used when the {@code Result} represents a completed event. This is used so that
         * {@code null} isn't used for {@link #value}.
         */
        private static final Object COMPLETED_SENTINEL = "COMPLETED";

        /**
         * Used as the initial state/result until the terminal state is achieved.
         */
        private static final Result STARTED = new Result(State.STARTED, null);
        private final State state;
        private final Object value;

        public Result(State state, Object value) {
            this.state = state;
            this.value = value;
        }

        public State state() {
            return state;
        }

        public Type asNextEventType() {
            if (state != State.CALLBACKS_REQUIRED)
                throw new KafkaException("The usage of asNextEventType is unexpected for state: " + state);

            if (!(value instanceof ApplicationEvent.Type))
                throw new KafkaException("The result value for the poll was unexpected: " + value);

            return (ApplicationEvent.Type) value;
        }

        public KafkaException asKafkaException() {
            if (state != State.FAILED)
                throw new KafkaException("The usage of asKafkaException is unexpected for state: " + state);

            if (!(value instanceof KafkaException))
                throw new KafkaException("The result value for the poll was unexpected: " + value);

            return (KafkaException) value;
        }

        @Override
        public String toString() {
            return "Result{" + "state=" + state + ", value=" + value + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return state == result.state && Objects.equals(value, result.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, value);
        }
    }

    private static final List<Type> ALLOWED_STARTING_EVENT_TYPES = List.of(
        Type.ASYNC_POLL,
        Type.CHECK_AND_UPDATE_POSITIONS
    );
    private final long deadlineMs;
    private final long pollTimeMs;
    private final Type startingEventType;
    private final AtomicReference<Result> result;

    /**
     * Creates a new event to signify a multi-stage processing of {@link Consumer#poll(Duration)} logic.
     *
     * @param deadlineMs        Time, in milliseconds, at which point the event must be completed; based on the
     *                          {@link Duration} passed to {@link Consumer#poll(Duration)}
     * @param pollTimeMs        Time, in milliseconds, at which point the event was created
     */
    public AsyncPollEvent(long deadlineMs, long pollTimeMs) {
        this(deadlineMs, pollTimeMs, Type.ASYNC_POLL);
    }

    /**
     * Creates a new event to signify a multi-stage processing of {@link Consumer#poll(Duration)} logic.
     * 
     * @param deadlineMs        Time, in milliseconds, at which point the event must be completed; based on the
     *                          {@link Duration} passed to {@link Consumer#poll(Duration)}
     * @param pollTimeMs        Time, in milliseconds, at which point the event was created
     * @param startingEventType {@link ApplicationEvent.Type} that serves as the starting point for the event processing
     */
    public AsyncPollEvent(long deadlineMs, long pollTimeMs, Type startingEventType) {
        super(Type.ASYNC_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;

        if (!ALLOWED_STARTING_EVENT_TYPES.contains(startingEventType))
            throw new KafkaException("The starting event type " + startingEventType + " is not valid. Should be one of " + ALLOWED_STARTING_EVENT_TYPES);

        this.startingEventType = startingEventType;
        this.result = new AtomicReference<>(Result.STARTED);
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public Type startingEventType() {
        return startingEventType;
    }

    public Result result() {
        return result.get();
    }

    public void completeSuccessfully() {
        Result r = new Result(State.SUCCEEDED, Result.COMPLETED_SENTINEL);
        result.compareAndSet(Result.STARTED, r);
    }

    public void completeExceptionally(KafkaException e) {
        Result r = new Result(State.FAILED, Objects.requireNonNull(e));
        result.compareAndSet(Result.STARTED, r);
    }

    public void completeWithCallbackRequired(Type nextEventType) {
        Result r = new Result(State.CALLBACKS_REQUIRED, Objects.requireNonNull(nextEventType));
        result.compareAndSet(Result.STARTED, r);
    }

    @Override
    public void metadataError(Exception metadataException) {
        completeExceptionally(ConsumerUtils.maybeWrapAsKafkaException(metadataException));
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
            ", deadlineMs=" + deadlineMs +
            ", pollTimeMs=" + pollTimeMs +
            ", startingEventType=" + startingEventType +
            ", result=" + result.get();
    }
}
