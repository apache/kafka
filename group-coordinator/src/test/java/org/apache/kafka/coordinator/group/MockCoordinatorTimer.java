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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * A simple mock for the {@link CoordinatorTimer}. The mock does not automatically
 * expire timeouts. They are only expired when {@link MockCoordinatorTimer#poll()}
 * is called.
 */
public class MockCoordinatorTimer<T, U> implements CoordinatorTimer<T, U> {
    /**
     * Represents a scheduled timeout.
     */
    public static class ScheduledTimeout<T, U> {
        public final String key;
        public final long deadlineMs;
        public final TimeoutOperation<T, U> operation;

        ScheduledTimeout(
            String key,
            long deadlineMs,
            TimeoutOperation<T, U> operation
        ) {
            this.key = key;
            this.deadlineMs = deadlineMs;
            this.operation = operation;
        }
    }

    /**
     * Represents an expired timeout.
     */
    public static class ExpiredTimeout<T, U> {
        public final String key;
        public final CoordinatorResult<T, U> result;

        ExpiredTimeout(
            String key,
            CoordinatorResult<T, U> result
        ) {
            this.key = key;
            this.result = result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ExpiredTimeout<?, ?> that = (ExpiredTimeout<?, ?>) o;

            if (!Objects.equals(key, that.key)) return false;
            return Objects.equals(result, that.result);
        }

        @Override
        public int hashCode() {
            int result1 = key != null ? key.hashCode() : 0;
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            return result1;
        }
    }

    private final Time time;

    private final Map<String, ScheduledTimeout<T, U>> timeoutMap = new HashMap<>();
    private final PriorityQueue<ScheduledTimeout<T, U>> timeoutQueue = new PriorityQueue<>(
        Comparator.comparingLong(entry -> entry.deadlineMs)
    );

    public MockCoordinatorTimer(Time time) {
        this.time = time;
    }

    /**
     * Schedules a timeout.
     */
    @Override
    public void schedule(
        String key,
        long delay,
        TimeUnit unit,
        boolean retry,
        TimeoutOperation<T, U> operation
    ) {
        cancel(key);

        long deadlineMs = time.milliseconds() + unit.toMillis(delay);
        ScheduledTimeout<T, U> timeout = new ScheduledTimeout<>(key, deadlineMs, operation);
        timeoutQueue.add(timeout);
        timeoutMap.put(key, timeout);
    }

    /**
     * Cancels a timeout.
     */
    @Override
    public void cancel(String key) {
        ScheduledTimeout<T, U> timeout = timeoutMap.remove(key);
        if (timeout != null) {
            timeoutQueue.remove(timeout);
        }
    }

    /**
     * @return True if a timeout with the key exists; false otherwise.
     */
    public boolean contains(String key) {
        return timeoutMap.containsKey(key);
    }

    /**
     * @return The scheduled timeout for the key; null otherwise.
     */
    public ScheduledTimeout<T, U> timeout(String key) {
        return timeoutMap.get(key);
    }

    /**
     * @return The number of scheduled timeouts.
     */
    public int size() {
        return timeoutMap.size();
    }

    /**
     * @return A list of expired timeouts based on the current time.
     */
    public List<ExpiredTimeout<T, U>> poll() {
        List<ExpiredTimeout<T, U>> results = new ArrayList<>();

        ScheduledTimeout<T, U> timeout = timeoutQueue.peek();
        while (timeout != null && timeout.deadlineMs <= time.milliseconds()) {
            timeoutQueue.poll();
            timeoutMap.remove(timeout.key, timeout);

            results.add(new ExpiredTimeout<>(
                timeout.key,
                timeout.operation.generateRecords()
            ));

            timeout = timeoutQueue.peek();
        }

        return results;
    }
}
