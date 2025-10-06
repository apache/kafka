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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

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
 *
 * <p/>
 *
 * When the {@code AsyncPollEvent} is created, it exists in the {@link State#STARTED} state. The background
 * thread will execute the {@code AsyncPollEvent} until it completes successfully ({@link State#SUCCEEDED})
 * or hits an error ({@link State#FAILED}).
 */
public class AsyncPollEvent extends ApplicationEvent implements MetadataErrorNotifiable {

    public enum State {

        STARTED,
        SUCCEEDED,
        FAILED
    }

    public static class Result {

        /**
         * Used as the initial state/result until the terminal state is achieved.
         */
        private static final Result STARTED = new Result(State.STARTED, null);
        private final State state;
        private final KafkaException error;

        public Result(State state, KafkaException error) {
            this.state = state;
            this.error = error;
        }

        public State state() {
            return state;
        }

        public KafkaException error() {
            return error;
        }

        @Override
        public String toString() {
            return "Result{" + "state=" + state + ", error=" + error + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return state == result.state && Objects.equals(error, result.error);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, error);
        }
    }

    private final long deadlineMs;
    private final long pollTimeMs;
    private final AtomicReference<Result> result;

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
        this.result = new AtomicReference<>(Result.STARTED);
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    public Result result() {
        return result.get();
    }

    public void completeSuccessfully() {
        Result r = new Result(State.SUCCEEDED, null);
        result.compareAndSet(Result.STARTED, r);
    }

    public void completeExceptionally(KafkaException e) {
        Result r = new Result(State.FAILED, Objects.requireNonNull(e));
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
            ", result=" + result.get();
    }
}
