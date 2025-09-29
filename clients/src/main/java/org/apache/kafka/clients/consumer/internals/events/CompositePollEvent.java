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

import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class CompositePollEvent extends ApplicationEvent {

    public enum State {

        CALLBACKS_REQUIRED,
        IN_PROGRESS,
        COMPLETE
    }

    public static class Result {

        private static final Result IN_PROGRESS = new Result(State.IN_PROGRESS, Optional.empty());
        private final State state;

        private final Optional<Type> nextEventType;

        public Result(State state, Optional<Type> nextEventType) {
            this.state = state;
            this.nextEventType = nextEventType;
        }

        public State state() {
            return state;
        }

        public Optional<Type> nextEventType() {
            return nextEventType;
        }

        @Override
        public String toString() {
            return "Result{" + "state=" + state + ", nextEventType=" + nextEventType + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return state == result.state && Objects.equals(nextEventType, result.nextEventType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, nextEventType);
        }
    }

    private final long deadlineMs;
    private final long pollTimeMs;
    private final Type nextEventType;
    private final AtomicReference<Object> resultOrError;

    public CompositePollEvent(long deadlineMs, long pollTimeMs, Type nextEventType) {
        super(Type.COMPOSITE_POLL);
        this.deadlineMs = deadlineMs;
        this.pollTimeMs = pollTimeMs;
        this.nextEventType = nextEventType;
        this.resultOrError = new AtomicReference<>(Result.IN_PROGRESS);
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

    public Result resultOrError() {
        Object o = resultOrError.get();

        if (o instanceof KafkaException)
            throw (KafkaException) o;
        else
            return (Result) o;
    }

    public void complete(State state, Optional<Type> nextEventType) {
        Result result = new Result(
            Objects.requireNonNull(state),
            Objects.requireNonNull(nextEventType)
        );

        resultOrError.compareAndSet(Result.IN_PROGRESS, result);
    }

    public void completeExceptionally(KafkaException e) {
        resultOrError.compareAndSet(Result.IN_PROGRESS, Objects.requireNonNull(e));
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", deadlineMs=" + deadlineMs + ", pollTimeMs=" + pollTimeMs + ", nextEventType=" + nextEventType + ", resultOrError=" + resultOrError;
    }
}
