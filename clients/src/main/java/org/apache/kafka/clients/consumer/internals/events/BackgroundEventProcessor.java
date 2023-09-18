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

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

public class BackgroundEventProcessor {

    private final Logger log;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    public BackgroundEventProcessor(final LogContext logContext,
                                    final BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.log = logContext.logger(BackgroundEventProcessor.class);
        this.backgroundEventQueue = backgroundEventQueue;
    }

    /**
     * Drains all available {@link BackgroundEvent}s, and then processes them in order. If any
     * errors are thrown as a result of a {@link ErrorBackgroundEvent} or an error occurs while processing
     * another type of {@link BackgroundEvent}, only the <em>first</em> exception will be thrown, all
     * subsequent errors will simply be logged at <code>WARN</code> level.
     *
     * @throws RuntimeException or subclass
     */
    public void process() {
        LinkedList<BackgroundEvent> events = new LinkedList<>();
        backgroundEventQueue.drainTo(events);

        RuntimeException first = null;
        int errorCount = 0;

        for (BackgroundEvent event : events) {
            log.debug("Consuming background event: {}", event);

            try {
                process(event);
            } catch (RuntimeException e) {
                errorCount++;

                if (first == null) {
                    first = e;
                    log.warn("Error #{} from background thread (will be logged and thrown): {}", errorCount, e.getMessage(), e);
                } else {
                    log.warn("Error #{} from background thread (will be logged only): {}", errorCount, e.getMessage(), e);
                }
            }
        }

        if (first != null) {
            throw first;
        }
    }

    private void process(final BackgroundEvent event) {
        Objects.requireNonNull(event, "Attempt to process null BackgroundEvent");
        Objects.requireNonNull(event.type(), "Attempt to process BackgroundEvent with null type: " + event);

        log.debug("Processing event {}", event);

        // Make sure to use the event's type() method, not the type variable directly. This causes problems when
        // unit tests mock the EventType.
        switch (event.type()) {
            case NOOP:
                process((NoopBackgroundEvent) event);
                return;

            case ERROR:
                process((ErrorBackgroundEvent) event);
                return;

            default:
                throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
        }
    }

    private void process(final NoopBackgroundEvent event) {
        log.debug("Received no-op background event with message: {}", event.message());
    }

    private void process(final ErrorBackgroundEvent event) {
        throw event.error();
    }
}
