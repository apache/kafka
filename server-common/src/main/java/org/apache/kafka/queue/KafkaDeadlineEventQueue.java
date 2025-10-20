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
package org.apache.kafka.queue;

import org.apache.kafka.common.utils.Timer;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

public class KafkaDeadlineEventQueue<T> {
    private final Queue<Event<T>> eventQueue;
    private final Consumer<T> timeoutOperation;

    public KafkaDeadlineEventQueue() {
        this(null);
    }

    public KafkaDeadlineEventQueue(Consumer<T> timeoutOperation) {
        this.eventQueue = new PriorityBlockingQueue<>();
        this.timeoutOperation = timeoutOperation;
    }

    public Queue<Event<T>> eventQueue() {
        return eventQueue;
    }

    public void enqueue(Event<T> element) {
        eventQueue.add(Objects.requireNonNull(element));
    }

    public Optional<T> dequeue(long currentTimeMs) {
        checkTimeout(currentTimeMs);
        Event<T> event = eventQueue.poll();
        return Optional.ofNullable(event == null ? null : event.get());
    }

    public boolean isEmpty() {
        return eventQueue.isEmpty();
    }

    public void checkTimeout(long currentTimeMs) {
        while (true) {
            Event<T> event = eventQueue.peek();

            if (event != null) {
                event.timer.update(currentTimeMs);
                if (event.timer.isExpired()) {
                    eventQueue.poll();
                    if (timeoutOperation != null) {
                        timeoutOperation.accept(event.get());
                    }
                    continue;
                }
            }
            break;
        }
    }

    public int size() {
        return eventQueue.size();
    }

    // Visible for test
    Optional<Event<T>> dequeueContext() {
        Event<T> event = eventQueue.poll();
        return Optional.ofNullable(event);
    }

    public static class Event<T> implements Comparable<Event<T>> {
        private final Timer timer;
        private final String tag;
        private final T payload;

        public Event(Timer timer, String tag, T payload) {
            this.timer = timer;
            this.tag = tag;
            this.payload = Objects.requireNonNull(payload);
        }

        public T get() {
            return payload;
        }

        public String getTag() {
            return tag;
        }

        @Override
        public int compareTo(Event<T> that) {
            return Long.compare(this.timer.deadlineMs(), that.timer.deadlineMs());
        }

        @Override
        public String toString() {
            return "Event={" +
                    "timer=" + timer +
                    ", tag=" + tag +
                    ", playload=" + payload +
                    "}";
        }

    }
}