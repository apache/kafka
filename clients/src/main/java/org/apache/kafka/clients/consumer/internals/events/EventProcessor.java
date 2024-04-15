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

import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

/**
 * An {@link EventProcessor} is the means by which events <em>produced</em> by thread <em>A</em> are
 * <em>processed</em> by thread <em>B</em>. By definition, threads <em>A</em> and <em>B</em> run in parallel to
 * each other, so a mechanism is needed with which to receive and process the events from the other thread. That
 * communication channel is formed around {@link BlockingQueue a shared queue} into which thread <em>A</em>
 * enqueues events and thread <em>B</em> reads and processes those events.
 */
public abstract class EventProcessor<T> implements Closeable {

    private final Logger log;
    private final BlockingQueue<T> eventQueue;
    private final IdempotentCloser closer;

    protected EventProcessor(final LogContext logContext, final BlockingQueue<T> eventQueue) {
        this.log = logContext.logger(EventProcessor.class);
        this.eventQueue = eventQueue;
        this.closer = new IdempotentCloser();
    }

    public abstract boolean process();

    protected abstract void process(T event);

    @Override
    public void close() {
        closer.close(this::closeInternal, () -> log.warn("The event processor was already closed"));
    }

    protected interface ProcessHandler<T> {

        void onProcess(T event, Optional<KafkaException> error);
    }

    /**
     * Drains all available events from the queue, and then processes them in order. If any errors are thrown while
     * processing the individual events, these are submitted to the given {@link ProcessHandler}.
     */
    protected boolean process(ProcessHandler<T> processHandler) {
        closer.assertOpen("The processor was previously closed, so no further processing can occur");

        List<T> events = drain();

        if (events.isEmpty()) {
            log.trace("No events to process");
            return false;
        }

        try {
            log.trace("Starting processing of {} event{}", events.size(), events.size() == 1 ? "" : "s");

            for (T event : events) {
                try {
                    Objects.requireNonNull(event, "Attempted to process a null event");
                    log.trace("Processing event: {}", event);
                    process(event);
                    processHandler.onProcess(event, Optional.empty());
                } catch (Throwable t) {
                    KafkaException error = ConsumerUtils.maybeWrapAsKafkaException(t);
                    processHandler.onProcess(event, Optional.of(error));
                }
            }
        } finally {
            log.trace("Completed processing");
        }

        return true;
    }

    /**
     * It is possible for the consumer to close before complete processing all the events in the queue. In
     * this case, we need to throw an exception to notify the user the consumer is closed.
     */
    private void closeInternal() {
        log.trace("Closing event processor");
        List<T> incompleteEvents = drain();

        if (incompleteEvents.isEmpty())
            return;

        KafkaException exception = new KafkaException("The consumer is closed");

        // Check each of the events and if it has a Future that is incomplete, complete it exceptionally.
        incompleteEvents
                .stream()
                .filter(e -> e instanceof CompletableEvent)
                .map(e -> ((CompletableEvent<?>) e).future())
                .filter(f -> !f.isDone())
                .forEach(f -> {
                    log.debug("Completing {} with exception {}", f, exception.getMessage());
                    f.completeExceptionally(exception);
                });

        log.debug("Discarding {} events because the consumer is closing", incompleteEvents.size());
    }

    /**
     * Moves all the events from the queue to the returned list.
     */
    private List<T> drain() {
        LinkedList<T> events = new LinkedList<>();
        eventQueue.drainTo(events);
        return events;
    }
}
