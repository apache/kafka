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

import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * {@code CompletableEventReaper} is responsible for tracking {@link CompletableEvent time-bound events} and removing
 * any that exceed their {@link CompletableEvent#deadlineMs() deadline} (unless they've already completed). This
 * mechanism is used by the {@link AsyncKafkaConsumer} to enforce the timeout provided by the user in its API
 * calls (e.g. {@link AsyncKafkaConsumer#commitSync(Duration)}).
 */
public class CompletableEventReaper {

    private enum State {
        ACTIVE,
        PAUSED
    }

    private enum Modification {

        ADD(State.ACTIVE, "added"),
        PAUSE(State.PAUSED, "paused"),
        RESUME(State.ACTIVE, "resumed");

        private final State state;
        private final String verb;

        Modification(State state, String verb) {
            this.state = state;
            this.verb = verb;
        }
    }

    private final Logger log;

    /**
     * Tracked events that are candidates for expiration.
     */
    private final Map<CompletableEvent<?>, State> tracked;

    public CompletableEventReaper(LogContext logContext) {
        this.log = logContext.logger(CompletableEventReaper.class);
        this.tracked = new HashMap<>();
    }

    /**
     * Adds a new {@link CompletableEvent event} to track for later completion/expiration.
     *
     * @param event Event to track
     */
    public void add(CompletableEvent<?> event) {
        put(event, Modification.ADD);
    }

    /**
     * Pauses an event to exclude it as a candidate for reaps.
     *
     * @param event Event to pause
     */
    public void pause(CompletableEvent<?> event) {
        put(event, Modification.PAUSE);
    }

    /**
     * Resumes tracking an event for reaping.
     *
     * @param event Event to resume
     */
    public void resume(CompletableEvent<?> event) {
        put(event, Modification.RESUME);
    }

    private void put(CompletableEvent<?> event, Modification modification) {
        Objects.requireNonNull(event, "Event must be non-null");

        if (modification == Modification.ADD) {
            if (tracked.containsKey(event))
                throw new IllegalArgumentException("The event " + event + " was previously added, so it cannot be " + modification.verb + " again");
        } else if (!tracked.containsKey(event)) {
            throw new IllegalArgumentException("The event " + event + " was not previously added, so it cannot be " + modification.verb);
        }

        tracked.put(event, modification.state);
        log.trace("Event {} was {} and is now in state {}", event, modification.verb, modification.state);
    }

    /**
     * This method performs a two-step process to "complete" {@link CompletableEvent events} that have either expired
     * or completed normally:
     *
     * <ol>
     *     <li>
     *         For each tracked event which has exceeded its {@link CompletableEvent#deadlineMs() deadline}, an
     *         instance of {@link TimeoutException} is created and passed to
     *         {@link CompletableFuture#completeExceptionally(Throwable)}.
     *     </li>
     *     <li>
     *         For each tracked event of which its {@link CompletableEvent#future() future} is already in the
     *         {@link CompletableFuture#isDone() done} state, it will be removed from the list of tracked events.
     *     </li>
     * </ol>
     *
     * <p/>
     *
     * This method should be called at regular intervals, based upon the needs of the resource that owns the reaper.
     *
     * @param currentTimeMs <em>Current</em> time with which to compare against the
     *                      <em>{@link CompletableEvent#deadlineMs() expiration time}</em>
     * @return The number of events that were expired
     */
    public long reap(long currentTimeMs) {
        int count = 0;
        List<CompletableEvent<?>> unpausedEvents = new ArrayList<>(tracked.size());

        for (Map.Entry<CompletableEvent<?>, State> entry : tracked.entrySet()) {
            State state = entry.getValue();

            if (state == State.PAUSED) {
                // Don't reap "paused" events
                continue;
            }

            unpausedEvents.add(entry.getKey());
        }

        for (CompletableEvent<?> event : unpausedEvents) {
            if (event.future().isDone()) {
                // Remove any events that are already complete.
                tracked.remove(event);
                continue;
            }

            long deadlineMs = event.deadlineMs();
            long pastDueMs = currentTimeMs - deadlineMs;

            if (pastDueMs < 0)
                continue;

            TimeoutException error = new TimeoutException(String.format("%s was %s ms past its expiration of %s", event.getClass().getSimpleName(), pastDueMs, deadlineMs));

            // Complete (exceptionally) any events that have passed their deadline AND aren't already complete.
            if (event.future().completeExceptionally(error)) {
                log.debug("Event {} completed exceptionally since its expiration of {} passed {} ms ago", event, deadlineMs, pastDueMs);
            } else {
                log.trace("Event {} not completed exceptionally since it was previously completed", event);
            }

            count++;

            // Remove the events so that we don't hold a reference to it.
            tracked.remove(event);
        }

        return count;
    }

    /**
     * It is possible for the {@link AsyncKafkaConsumer#close() consumer to close} before completing the processing of
     * all the events in the queue. In this case, we need to
     * {@link CompletableFuture#completeExceptionally(Throwable) expire} any remaining events.
     *
     * <p/>
     *
     * Check each of the {@link #add(CompletableEvent) previously-added} {@link CompletableEvent completable events},
     * and for any that are incomplete, expire them. Also check the core event queue for any incomplete events and
     * likewise expire them.
     *
     * <p/>
     *
     * <em>Note</em>: because this is called in the context of {@link AsyncKafkaConsumer#close() closing consumer},
     * don't take the deadline into consideration, just close it regardless.
     *
     * @param events Events from a queue that have not yet been tracked that also need to be reviewed
     * @return The number of events that were expired
     */
    public long reap(Collection<?> events) {
        Objects.requireNonNull(events, "Event queue to reap must be non-null");

        long trackedExpiredCount = completeEventsExceptionallyOnClose(tracked.keySet());
        tracked.clear();

        long eventExpiredCount = completeEventsExceptionallyOnClose(events);
        events.clear();

        return trackedExpiredCount + eventExpiredCount;
    }

    public int size() {
        return tracked.size();
    }

    public boolean contains(CompletableEvent<?> event) {
        return event != null && tracked.containsKey(event);
    }

    public List<CompletableEvent<?>> uncompletedEvents() {
        // The following code does not use the Java Collections Streams API to reduce overhead in the critical
        // path of the ConsumerNetworkThread loop.
        List<CompletableEvent<?>> events = new ArrayList<>();

        for (Map.Entry<CompletableEvent<?>, State> entry : tracked.entrySet()) {
            CompletableEvent<?> event = entry.getKey();

            if (!event.future().isDone())
                events.add(event);
        }

        return events;
    }

    /**
     * For all the {@link CompletableEvent}s in the collection, if they're not already complete, invoke
     * {@link CompletableFuture#completeExceptionally(Throwable)}.
     *
     * @param events Collection of objects, assumed to be subclasses of {@link ApplicationEvent} or
     *               {@link BackgroundEvent}, but will only perform completion for any
     *               unfinished {@link CompletableEvent}s
     *
     * @return Number of events closed
     */
    private long completeEventsExceptionallyOnClose(Collection<?> events) {
        long count = 0;

        for (Object o : events) {
            if (!(o instanceof CompletableEvent))
                continue;

            CompletableEvent<?> event = (CompletableEvent<?>) o;

            if (event.future().isDone())
                continue;

            count++;

            TimeoutException error = new TimeoutException(String.format("%s could not be completed before the consumer closed", event.getClass().getSimpleName()));

            if (event.future().completeExceptionally(error)) {
                log.debug("Event {} completed exceptionally since the consumer is closing", event);
            } else {
                log.trace("Event {} not completed exceptionally since it was completed prior to the consumer closing", event);
            }
        }

        return count;
    }
}
