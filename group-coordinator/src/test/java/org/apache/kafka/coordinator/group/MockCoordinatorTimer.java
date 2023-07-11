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
public class MockCoordinatorTimer<T> implements CoordinatorTimer<T> {
    /**
     * Represents a scheduled timeout.
     */
    public static class ScheduledTimeout<T> {
        public final String key;
        public final long deadlineMs;
        public final TimeoutOperation<T> operation;

        ScheduledTimeout(
            String key,
            long deadlineMs,
            TimeoutOperation<T> operation
        ) {
            this.key = key;
            this.deadlineMs = deadlineMs;
            this.operation = operation;
        }
    }

    /**
     * Represents an expired timeout.
     */
    public static class ExpiredTimeout<T> {
        public final String key;
        public final List<T> records;

        ExpiredTimeout(
            String key,
            List<T> records
        ) {
            this.key = key;
            this.records = records;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ExpiredTimeout<?> that = (ExpiredTimeout<?>) o;

            if (!Objects.equals(key, that.key)) return false;
            return Objects.equals(records, that.records);
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (records != null ? records.hashCode() : 0);
            return result;
        }
    }

    private final Time time;

    private final Map<String, ScheduledTimeout<T>> timeoutMap = new HashMap<>();
    private final PriorityQueue<ScheduledTimeout<T>> timeoutQueue = new PriorityQueue<>(
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
        TimeoutOperation<T> operation
    ) {
        cancel(key);

        long deadlineMs = time.milliseconds() + unit.toMillis(delay);
        ScheduledTimeout<T> timeout = new ScheduledTimeout<>(key, deadlineMs, operation);
        timeoutQueue.add(timeout);
        timeoutMap.put(key, timeout);
    }

    /**
     * Cancels a timeout.
     */
    @Override
    public void cancel(String key) {
        ScheduledTimeout<T> timeout = timeoutMap.remove(key);
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
    public ScheduledTimeout<T> timeout(String key) {
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
    public List<ExpiredTimeout<T>> poll() {
        List<ExpiredTimeout<T>> results = new ArrayList<>();

        ScheduledTimeout<T> timeout = timeoutQueue.peek();
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
