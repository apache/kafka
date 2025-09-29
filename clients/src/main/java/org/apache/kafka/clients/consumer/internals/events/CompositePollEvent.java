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

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class represents the non-blocking event that executes logic functionally equivalent to the following:
 *
 * <ul>
 *     <li>{@link PollEvent}</li>
 *     <li>{@link CheckAndUpdatePositionsEvent}</li>
 *     <li>{@link CreateFetchRequestsEvent}</li>
 * </ul>
 *
 * {@link AsyncKafkaConsumer#poll(Duration)} is implemented using a non-blocking design to ensure performance is
 * at the same level as {@link ClassicKafkaConsumer#poll(Duration)}. The event is submitted in {@code poll()}, but
 * there are no blocking waits for the "result" of the event. Checks are made for the result at certain points, but
 * they do not block. The logic for the three previously-mentioned events is executed one after the other on the
 * background thread.
 *
 * <p/>
 *
 * When the {@code CompositePollEvent} is created, it exists in the {@link State#STARTED} state. The background
 * thread will execute the {@code CompositePollEvent} until it completes successfully ({@link State#SUCCEEDED}),
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
public class CompositePollEvent extends ApplicationEvent {

    public enum State {

        STARTED,
        SUCCEEDED,
        FAILED,
        CALLBACKS_REQUIRED
    }

    public static class Result {

        private static final Object COMPLETED = new Object();
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
            if (!(value instanceof ApplicationEvent.Type))
                throw new KafkaException("The result value for the poll was unexpected: " + value);

            return (ApplicationEvent.Type) value;
        }

        public KafkaException asKafkaException() {
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

    private final long deadlineMs;
    private final long pollTimeMs;
    private final Type nextEventType;
    private final AtomicReference<Result> result;

    public CompositePollEvent(long deadlineMs, long pollTimeMs, Type nextEventType) {
        super(Type.COMPOSITE_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;
        this.nextEventType = nextEventType;
        this.result = new AtomicReference<>(Result.STARTED);
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

    public Result result() {
        return result.get();
    }

    public void completeSuccessfully() {
        Result r = new Result(State.SUCCEEDED, Result.COMPLETED);
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
    protected String toStringBase() {
        return super.toStringBase() + ", deadlineMs=" + deadlineMs + ", pollTimeMs=" + pollTimeMs + ", nextEventType=" + nextEventType + ", result=" + result;
    }
}
