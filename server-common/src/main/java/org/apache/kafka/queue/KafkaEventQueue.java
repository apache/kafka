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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;


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
        private OptionalLong deadlineNs = OptionalLong.empty();

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
         * Insert the event context in the circularly linked list after this node.
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
         *
         * @param log                  The logger to use.
         * @param exceptionToDeliver   If non-null, the exception to deliver to the event.
         *
         * @return                     True if the thread was interrupted; false otherwise.
         */
        boolean run(Logger log, Throwable exceptionToDeliver) {
            if (exceptionToDeliver == null) {
                try {
                    event.run();
                } catch (InterruptedException e) {
                    log.warn("Interrupted while running event. Shutting down event queue");
                    return true;
                } catch (Throwable e) {
                    log.debug("Got exception while running {}. Invoking handleException.", event, e);
                    exceptionToDeliver = e;
                }
            }
            if (exceptionToDeliver != null) {
                completeWithException(log, exceptionToDeliver);
            }
            return Thread.currentThread().isInterrupted();
        }

        /**
         * Complete the event associated with this EventContext with the specified
         * exception.
         */
        void completeWithException(Logger log, Throwable t) {
            try {
                event.handleException(t);
            } catch (Exception e) {
                log.error("Unexpected exception in handleException", e);
            }
        }
    }

    private class EventHandler implements Runnable {
        private int size = 0;

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
        private final TreeMap<Long, EventContext> deadlineMap = new TreeMap<>();

        /**
         * A condition variable for waking up the event handler thread.
         */
        private final Condition cond = lock.newCondition();

        @Override
        public void run() {
            try {
                handleEvents();
            } catch (Throwable e) {
                log.warn("event handler thread exiting with exception", e);
            }
            try {
                cleanupEvent.run();
            } catch (Throwable e) {
                log.warn("cleanup event threw exception", e);
            }
        }

        private void remove(EventContext eventContext) {
            eventContext.remove();
            if (eventContext.deadlineNs.isPresent()) {
                deadlineMap.remove(eventContext.deadlineNs.getAsLong());
                eventContext.deadlineNs = OptionalLong.empty();
            }
            if (eventContext.tag != null) {
                tagToEventContext.remove(eventContext.tag, eventContext);
                eventContext.tag = null;
            }
        }

        private void handleEvents() {
            Throwable toDeliver = null;
            EventContext toRun = null;
            boolean wasInterrupted = false;
            while (true) {
                if (toRun != null) {
                    wasInterrupted = toRun.run(log, toDeliver);
                }
                lock.lock();
                try {
                    if (toRun != null) {
                        size--;
                        if (wasInterrupted) {
                            interrupted = wasInterrupted;
                        }
                        toDeliver = null;
                        toRun = null;
                        wasInterrupted = false;
                    }
                    long awaitNs = Long.MAX_VALUE;
                    Map.Entry<Long, EventContext> entry = deadlineMap.firstEntry();
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
                                toDeliver = null;
                                toRun = eventContext;
                            } else {
                                // not a deferred event, so it is a deadline, and it is timed out.
                                remove(eventContext);
                                toDeliver = new TimeoutException();
                                toRun = eventContext;
                            }
                            continue;
                        } else if (interrupted) {
                            remove(eventContext);
                            toDeliver = new InterruptedException("The event handler thread is interrupted");
                            toRun = eventContext;
                            continue;
                        } else if (shuttingDown) {
                            remove(eventContext);
                            toDeliver = new RejectedExecutionException("The event queue is shutting down");
                            toRun = eventContext;
                            continue;
                        }
                        awaitNs = timeoutNs - now;
                    }
                    if (head.next == head) {
                        if (deadlineMap.isEmpty() && (shuttingDown || interrupted)) {
                            // If there are no more entries to process, and the queue is
                            // closing, exit the thread.
                            return;
                        }
                    } else {
                        if (interrupted) {
                            toDeliver = new InterruptedException("The event handler thread is interrupted");
                        } else {
                            toDeliver = null;
                        }
                        toRun = head.next;
                        remove(toRun);
                        continue;
                    }
                    if (awaitNs == Long.MAX_VALUE) {
                        try {
                            cond.await();
                        } catch (InterruptedException e) {
                            log.warn("Interrupted while waiting for a new event. " +
                                "Shutting down event queue");
                            interrupted = true;
                        }
                    } else {
                        try {
                            cond.awaitNanos(awaitNs);
                        } catch (InterruptedException e) {
                            log.warn("Interrupted while waiting for a deferred event. " +
                                "Shutting down event queue");
                            interrupted = true;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        Exception enqueue(EventContext eventContext,
                          Function<OptionalLong, OptionalLong> deadlineNsCalculator) {
            lock.lock();
            try {
                if (shuttingDown) {
                    return new RejectedExecutionException("The event queue is shutting down");
                }
                if (interrupted) {
                    return new InterruptedException("The event handler thread is interrupted");
                }
                OptionalLong existingDeadlineNs = OptionalLong.empty();
                if (eventContext.tag != null) {
                    EventContext toRemove =
                        tagToEventContext.put(eventContext.tag, eventContext);
                    if (toRemove != null) {
                        existingDeadlineNs = toRemove.deadlineNs;
                        remove(toRemove);
                        size--;
                    }
                }
                OptionalLong deadlineNs = deadlineNsCalculator.apply(existingDeadlineNs);
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
                        if (!deadlineNs.isPresent()) {
                            return new RuntimeException(
                                "You must specify a deadline for deferred events.");
                        }
                        break;
                }
                if (deadlineNs.isPresent()) {
                    long insertNs = deadlineNs.getAsLong();
                    long prevStartNs = deadlineMap.isEmpty() ? Long.MAX_VALUE : deadlineMap.firstKey();
                    // If the time in nanoseconds is already taken, take the next one.
                    while (deadlineMap.putIfAbsent(insertNs, eventContext) != null) {
                        insertNs++;
                    }
                    eventContext.deadlineNs = OptionalLong.of(insertNs);
                    // If the new timeout is before all the existing ones, wake up the
                    // timeout thread.
                    if (insertNs <= prevStartNs) {
                        shouldSignal = true;
                    }
                }
                size++;
                if (shouldSignal) {
                    cond.signal();
                }
            } finally {
                lock.unlock();
            }
            return null;
        }

        void cancelDeferred(String tag) {
            lock.lock();
            try {
                EventContext eventContext = tagToEventContext.get(tag);
                if (eventContext != null) {
                    remove(eventContext);
                    size--;
                }
            } finally {
                lock.unlock();
            }
        }

        void wakeUp() {
            lock.lock();
            try {
                eventHandler.cond.signal();
            } finally {
                lock.unlock();
            }
        }

        int size() {
            lock.lock();
            try {
                return size;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * The event to run when the queue is closing.
     */
    private final Event cleanupEvent;

    /**
     * The lock which protects private data.
     */
    private final ReentrantLock lock;

    /**
     * The log4j logger to use.
     */
    private final Logger log;

    /**
     * The runnable that our thread executes.
     */
    private final EventHandler eventHandler;

    /**
     * The queue thread.
     */
    private final Thread eventHandlerThread;

    /**
     * True if the event queue is shutting down. Protected by the lock.
     */
    private boolean shuttingDown;

    /**
     * True if the event handler thread was interrupted. Protected by the lock.
     */
    private boolean interrupted;

    public KafkaEventQueue(
        Time time,
        LogContext logContext,
        String threadNamePrefix
    ) {
        this(time, logContext, threadNamePrefix, VoidEvent::new);
    }

    public KafkaEventQueue(
        Time time,
        LogContext logContext,
        String threadNamePrefix,
        Event cleanupEvent
    ) {
        this.time = time;
        this.cleanupEvent = Objects.requireNonNull(cleanupEvent);
        this.lock = new ReentrantLock();
        this.log = logContext.logger(KafkaEventQueue.class);
        this.eventHandler = new EventHandler();
        this.eventHandlerThread = new KafkaThread(threadNamePrefix + "event-handler",
            this.eventHandler, false);
        this.shuttingDown = false;
        this.interrupted = false;
        this.eventHandlerThread.start();
    }

    public Time time() {
        return time;
    }

    @Override
    public void enqueue(EventInsertionType insertionType,
                        String tag,
                        Function<OptionalLong, OptionalLong> deadlineNsCalculator,
                        Event event) {
        EventContext eventContext = new EventContext(event, insertionType, tag);
        Exception e = eventHandler.enqueue(eventContext, deadlineNsCalculator);
        if (e != null) {
            eventContext.completeWithException(log, e);
        }
    }

    @Override
    public void cancelDeferred(String tag) {
        eventHandler.cancelDeferred(tag);
    }

    @Override
    public void beginShutdown(String source) {
        lock.lock();
        try {
            if (shuttingDown) {
                log.debug("{}: Event queue is already shutting down.", source);
                return;
            }
            log.info("{}: shutting down event queue.", source);
            shuttingDown = true;
            eventHandler.cond.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        return eventHandler.size();
    }

    @Override
    public void wakeup() {
        eventHandler.wakeUp();
    }

    @Override
    public void close() throws InterruptedException {
        beginShutdown("KafkaEventQueue#close");
        eventHandlerThread.join();
        log.info("closed event queue.");
    }
}
