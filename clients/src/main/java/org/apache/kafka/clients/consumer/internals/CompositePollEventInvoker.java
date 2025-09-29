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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompositePollEvent;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;

public class CompositePollEventInvoker {

    private final Logger log;
    private final Time time;
    private final ApplicationEventHandler applicationEventHandler;
    private final Runnable applicationThreadCallbacks;
    private CompositePollEvent inflight;

    public CompositePollEventInvoker(LogContext logContext,
                                     Time time,
                                     ApplicationEventHandler applicationEventHandler,
                                     Runnable applicationThreadCallbacks) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.applicationEventHandler = applicationEventHandler;
        this.applicationThreadCallbacks = applicationThreadCallbacks;
    }

    public void poll(Timer timer) {
        if (inflight == null) {
            log.debug("No existing inflight event, submitting");
            submitEvent(ApplicationEvent.Type.POLL, timer);
        }

        try {
            if (log.isTraceEnabled()) {
                log.trace(
                    "Attempting to retrieve result from previously submitted {} with {} remaining on timer",
                    inflight,
                    timer.remainingMs()
                );
            }

            CompositePollEvent.Result result = inflight.resultOrError();
            CompositePollEvent.State state = result.state();

            if (state == CompositePollEvent.State.COMPLETE) {
                // Make sure to clear out the inflight request since it's complete.
                log.debug("Event {} completed, clearing inflight", inflight);
                inflight = null;
            } else if (state == CompositePollEvent.State.CALLBACKS_REQUIRED) {
                log.debug("About to process callbacks");
                applicationThreadCallbacks.run();
                log.debug("Done processing callbacks");
                result.nextEventType().ifPresent(t -> submitEvent(t, timer));
            }
        } catch (Throwable t) {
            // If an exception is hit, bubble it up to the user but make sure to clear out the inflight request
            // because the error effectively renders it complete.
            log.debug("Event {} \"completed\" via error ({}), clearing inflight", inflight, String.valueOf(t));
            inflight = null;
            throw ConsumerUtils.maybeWrapAsKafkaException(t);
        }
    }

    private void submitEvent(ApplicationEvent.Type type, Timer timer) {
        long deadlineMs = calculateDeadlineMs(timer);
        inflight = new CompositePollEvent(deadlineMs, time.milliseconds(), type);
        applicationEventHandler.add(inflight);
        log.debug("Submitted new {} with {} remaining on timer", inflight, timer.remainingMs());
    }
}