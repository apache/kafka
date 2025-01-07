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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * {@code CompletableEventReaper} is responsible for tracking {@link CompletableEvent time-bound events} and removing
 * any that exceed their {@link CompletableEvent#deadlineMs() deadline} (unless they've already completed). This
 * mechanism is used by the {@link AsyncKafkaConsumer} to enforce the timeout provided by the user in its API
 * calls (e.g. {@link AsyncKafkaConsumer#commitSync(Duration)}).
 */
public class CompletableEventReaper {

    private final Logger log;

    /**
     * List of tracked events that are candidates for expiration.
     */
    private final List<CompletableEvent<?>> tracked;

    public CompletableEventReaper(LogContext logContext) {
        this.log = logContext.logger(CompletableEventReaper.class);
        this.tracked = new ArrayList<>();
    }

    /**
     * Adds a new {@link CompletableEvent event} to track for later completion/expiration.
     *
     * @param event Event to track
     */
    public void add(CompletableEvent<?> event) {
        tracked.add(Objects.requireNonNull(event, "Event to track must be non-null"));
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
        Consumer<CompletableEvent<?>> expireEvent = event -> {
            long pastDueMs = currentTimeMs - event.deadlineMs();
            TimeoutException error = new TimeoutException(String.format("%s was %s ms past its expiration of %s", event.getClass().getSimpleName(), pastDueMs, event.deadlineMs()));

            if (event.future().completeExceptionally(error)) {
                log.debug("Event {} completed exceptionally since its expiration of {} passed {} ms ago", event, event.deadlineMs(), pastDueMs);
            } else {
                log.trace("Event {} not completed exceptionally since it was previously completed", event);
            }
        };

        // First, complete (exceptionally) any events that have passed their deadline AND aren't already complete.
        long count = tracked.stream()
            .filter(e -> !e.future().isDone())
            .filter(e -> currentTimeMs >= e.deadlineMs())
            .peek(expireEvent)
            .count();
        // Second, remove any events that are already complete, just to make sure we don't hold references. This will
        // include any events that finished successfully as well as any events we just completed exceptionally above.
        tracked.removeIf(e -> e.future().isDone());

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

        Consumer<CompletableEvent<?>> expireEvent = event -> {
            TimeoutException error = new TimeoutException(String.format("%s could not be completed before the consumer closed", event.getClass().getSimpleName()));

            if (event.future().completeExceptionally(error)) {
                log.debug("Event {} completed exceptionally since the consumer is closing", event);
            } else {
                log.trace("Event {} not completed exceptionally since it was completed prior to the consumer closing", event);
            }
        };

        long trackedExpiredCount = tracked.stream()
            .filter(e -> !e.future().isDone())
            .peek(expireEvent)
            .count();
        tracked.clear();

        long eventExpiredCount = events.stream()
            .filter(e -> e instanceof CompletableEvent<?>)
            .map(e -> (CompletableEvent<?>) e)
            .filter(e -> !e.future().isDone())
            .peek(expireEvent)
            .count();
        events.clear();
        return trackedExpiredCount + eventExpiredCount;
    }

    public int size() {
        return tracked.size();
    }

    public boolean contains(CompletableEvent<?> event) {
        return event != null && tracked.contains(event);
    }

    public List<CompletableEvent<?>> uncompletedEvents() {
        return tracked.stream()
                .filter(e -> !e.future().isDone())
                .collect(Collectors.toList());
    }
    
}
