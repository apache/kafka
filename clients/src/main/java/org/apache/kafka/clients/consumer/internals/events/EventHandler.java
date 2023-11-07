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

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;

/**
 * An event handler is used to publish events from one thread which are then consumed from another thread.
 */
public class EventHandler<T> implements Closeable {

    private final Logger log;
    private final BlockingQueue<T> queue;
    private final Watcher watcher;

    public EventHandler(final LogContext logContext, final BlockingQueue<T> queue, Watcher watcher) {
        this.log = logContext.logger(EventHandler.class);
        this.queue = queue;
        this.watcher = watcher;
    }

    /**
     * Add an event to the underlying queue and internally invoke {@link #notifyWatcher} to alert the watcher that
     * it has an event to process.
     *
     * @param event An event to enqueue for later processing
     */
    public void add(final T event) {
        Objects.requireNonNull(event, "Event must be non-null");
        log.trace("Enqueued event: {}", event);
        queue.add(event);
        watcher.updated();
    }

    public void notifyWatcher() {
        watcher.updated();
    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    public void close(Duration timeout) {
        // Do nothing. Available for subclasses.
    }

    public interface Watcher {

        void updated();
    }
}
