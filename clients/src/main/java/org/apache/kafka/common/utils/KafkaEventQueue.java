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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

public final class KafkaEventQueue implements EventQueue {
    /**
     * A context object that wraps events.
     */
    private static class EventContext {
        /**
         * The caller-supplied event.
         */
        private final Event event;

        /**
         * How this event was inserted.
         */
        private final EventInsertionType insertionType;

        /**
         * The previous pointer of our circular doubly-linked list.
         */
        private EventContext prev = this;

        /**
         * The next pointer in our circular doubly-linked list.
         */
        private EventContext next = this;

        /**
         * If this event is in the delay map, this is the key it is there under.
         * If it is not in the map, this is null.
         */
        private Long deadlineNs = null;

        /**
         * The tag associated with this event.
         */
        private String tag;

        EventContext(Event event, EventInsertionType insertionType, String tag) {
            this.event = event;
            this.insertionType = insertionType;
            this.tag = tag;
        }

        /**
         * Insert a new node in the circularly linked list after this node.
         */
        void insertAfter(EventContext other) {
            this.next.prev = other;
            other.next = this.next;
            other.prev = this;
            this.next = other;
        }

        /**
         * Insert a new node in the circularly linked list before this node.
         */
        void insertBefore(EventContext other) {
            this.prev.next = other;
            other.prev = this.prev;
            other.next = this;
            this.prev = other;
        }

        /**
         * Remove this node from the circularly linked list.
         */
        void remove() {
            this.prev.next = this.next;
            this.next.prev = this.prev;
            this.prev = this;
            this.next = this;
        }

        /**
         * Returns true if this node is the only element in its list.
         */
        boolean isSingleton() {
            return prev == this && next == this;
        }

        /**
         * Run the event associated with this EventContext.
         */
        void run() throws InterruptedException {
            try {
                event.run();
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                event.handleException(e);
            }
        }

        /**
         * Complete the event associated with this EventContext with a timeout exception.
         */
        void completeWithTimeout() {
            completeWithException(new TimeoutException());
        }

        /**
         * Complete the event associated with this EventContext with a cancellation exception.
         */
        void cancel() {
            event.handleException(new CancellationException());
        }

        /**
         * Complete the event associated with this EventContext with the specified
         * exception.
         */
        void completeWithException(Throwable t) {
            event.handleException(t);
        }
    }

    private class EventHandler implements Runnable {
        /**
         * Event contexts indexed by tag.  Events without a tag are not included here.
         */
        private final Map<String, EventContext> tagToEventContext = new HashMap<>();

        /**
         * The head of the event queue.
         */
        private final EventContext head = new EventContext(null, null, null);

        /**
         * An ordered map of times in monotonic nanoseconds to events to time out.
         */
        private final TreeMap<Long, EventContext> delayMap = new TreeMap<>();

        /**
         * A condition variable for waking up the event handler thread.
         */
        private final Condition cond = lock.newCondition();

        @Override
        public void run() {
            try {
                handleEvents();
                cleanupEvent.run();
            } catch (Throwable e) {
                log.warn("event handler thread exiting with exception", e);
            }
        }

        private void remove(EventContext eventContext) {
            eventContext.remove();
            if (eventContext.deadlineNs != null) {
                delayMap.remove(eventContext.deadlineNs);
                eventContext.deadlineNs = null;
            }
            if (eventContext.tag != null) {
                tagToEventContext.remove(eventContext.tag, eventContext);
                eventContext.tag = null;
            }
        }

        private void handleEvents() throws InterruptedException {
            EventContext toTimeout = null;
            EventContext toRun = null;
            while (true) {
                if (toTimeout != null) {
                    toTimeout.completeWithTimeout();
                    toTimeout = null;
                } else if (toRun != null) {
                    toRun.run();
                    toRun = null;
                }
                lock.lock();
                try {
                    long awaitNs = Long.MAX_VALUE;
                    Map.Entry<Long, EventContext> entry = delayMap.firstEntry();
                    if (entry != null) {
                        // Search for timed-out events or deferred events that are ready
                        // to run.
                        long now = time.nanoseconds();
                        long timeoutNs = entry.getKey();
                        EventContext eventContext = entry.getValue();
                        if (timeoutNs <= now) {
                            if (eventContext.insertionType == EventInsertionType.DEFERRED) {
                                // The deferred event is ready to run.  Prepend it to the
                                // queue.  (The value for deferred events is a schedule time
                                // rather than a timeout.)
                                remove(eventContext);
                                head.insertAfter(eventContext);
                            } else {
                                // not a deferred event, so it is a deadline, and it is timed out.
                                remove(eventContext);
                                toTimeout = eventContext;
                            }
                            continue;
                        } else if (closingTimeNs <= now) {
                            remove(eventContext);
                            toTimeout = eventContext;
                            continue;
                        }
                        awaitNs = timeoutNs - now;
                    }
                    if (head.next == head) {
                        if ((closingTimeNs != Long.MAX_VALUE) && delayMap.isEmpty()) {
                            // If there are no more entries to process, and the queue is
                            // closing, exit the thread.
                            return;
                        }
                    } else {
                        toRun = head.next;
                        remove(toRun);
                        continue;
                    }
                    if (closingTimeNs != Long.MAX_VALUE) {
                        long now = time.nanoseconds();
                        if (awaitNs > closingTimeNs - now) {
                            awaitNs = closingTimeNs - now;
                        }
                    }
                    if (awaitNs == Long.MAX_VALUE) {
                        cond.await();
                    } else {
                        cond.awaitNanos(awaitNs);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        private void enqueue(EventContext eventContext,
                             Function<Long, Long> deadlineNsCalculator) {
            lock.lock();
            try {
                Long existingDeadlineNs = null;
                if (eventContext.tag != null) {
                    EventContext toRemove =
                        tagToEventContext.put(eventContext.tag, eventContext);
                    if (toRemove != null) {
                        existingDeadlineNs = toRemove.deadlineNs;
                        remove(toRemove);
                        toRemove.cancel();
                    }
                }
                Long deadlineNs = deadlineNsCalculator.apply(existingDeadlineNs);
                boolean queueWasEmpty = head.isSingleton();
                boolean shouldSignal = false;
                switch (eventContext.insertionType) {
                    case APPEND:
                        head.insertBefore(eventContext);
                        if (queueWasEmpty) {
                            shouldSignal = true;
                        }
                        break;
                    case PREPEND:
                        head.insertAfter(eventContext);
                        if (queueWasEmpty) {
                            shouldSignal = true;
                        }
                        break;
                    case DEFERRED:
                        if (deadlineNs == null) {
                            eventContext.completeWithException(new RuntimeException(
                                "You must specify a deadline for deferred events."));
                            return;
                        }
                        break;
                }
                if (deadlineNs != null) {
                    long insertNs =  deadlineNs;
                    long prevStartNs = delayMap.isEmpty() ? Long.MAX_VALUE : delayMap.firstKey();
                    // If the time in nanoseconds is already taken, take the next one.
                    while (delayMap.putIfAbsent(insertNs, eventContext) != null) {
                        insertNs++;
                    }
                    eventContext.deadlineNs = insertNs;
                    // If the new timeout is before all the existing ones, wake up the
                    // timeout thread.
                    if (insertNs <= prevStartNs) {
                        shouldSignal = true;
                    }
                }
                if (shouldSignal) {
                    cond.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private final Time time;
    private final ReentrantLock lock;
    private final Logger log;
    private final Supplier<Throwable> closedExceptionSupplier;
    private final EventHandler eventHandler;
    private final Thread eventHandlerThread;

    /**
     * The time in monotonic nanoseconds when the queue is closing, or Long.MAX_VALUE if
     * the queue is not currently closing.
     */
    private long closingTimeNs;

    private Event cleanupEvent;

    public KafkaEventQueue(Time time,
                           LogContext logContext,
                           String threadNamePrefix) {
        this(time, logContext, threadNamePrefix, TimeoutException::new);
    }

    public KafkaEventQueue(Time time,
                           LogContext logContext,
                           String threadNamePrefix,
                           Supplier<Throwable> closedExceptionSupplier) {
        this.time = time;
        this.lock = new ReentrantLock();
        this.log = logContext.logger(KafkaEventQueue.class);
        this.closedExceptionSupplier = closedExceptionSupplier;
        this.eventHandler = new EventHandler();
        this.eventHandlerThread = new KafkaThread(threadNamePrefix + "EventHandler",
            this.eventHandler, false);
        this.closingTimeNs = Long.MAX_VALUE;
        this.cleanupEvent = null;
        this.eventHandlerThread.start();
    }

    @Override
    public void enqueue(EventInsertionType insertionType,
                        String tag,
                        Function<Long, Long> deadlineNsCalculator,
                        Event event) {
        lock.lock();
        try {
            EventContext eventContext = new EventContext(event, insertionType, tag);
            if (closingTimeNs != Long.MAX_VALUE) {
                eventContext.completeWithException(closedExceptionSupplier.get());
            } else {
                eventHandler.enqueue(eventContext,
                    deadlineNsCalculator == null ? __ -> null : deadlineNsCalculator);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void beginShutdown(String source, Event newCleanupEvent,
                              TimeUnit timeUnit, long timeSpan) {
        if (timeSpan < 0) {
            throw new IllegalArgumentException("beginShutdown must be called with a " +
                "non-negative timeout.");
        }
        Objects.requireNonNull(newCleanupEvent);
        lock.lock();
        try {
            if (cleanupEvent != null) {
                log.debug("{}: Event queue is already shut down.", source);
                return;
            }
            log.info("{}: shutting down event queue.", source);
            cleanupEvent = newCleanupEvent;
            long newClosingTimeNs = time.nanoseconds() + timeUnit.toNanos(timeSpan);
            if (closingTimeNs >= newClosingTimeNs)
                closingTimeNs = newClosingTimeNs;
            eventHandler.cond.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        beginShutdown("KafkaEventQueue#close");
        eventHandlerThread.join();
        log.info("closed event queue.");
    }
}
