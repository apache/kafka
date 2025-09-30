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
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.time.Duration;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;

/**
 * {@code CompositePollEventInvoker} is executed on the application thread in the
 * {@link AsyncKafkaConsumer#poll(Duration)}.
 */
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

    /**
     * {@code poll()} manages the lifetime of the {@link CompositePollEvent} processing. If it is called when
     * no event is currently processing, it will start a new event processing asynchronously. A check is made
     * during each invocation to see if the <em>inflight</em> event has reached a
     * {@link CompositePollEvent.State terminal state}. If it has, the result will be processed accordingly.
     */
    public void poll(Timer timer) {
        if (inflight == null) {
            log.trace("No existing inflight event, submitting a new event");
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

            // Result should be non-null and starts off as State.STARTED.
            CompositePollEvent.Result result = inflight.result();
            CompositePollEvent.State state = result.state();

            if (state == CompositePollEvent.State.SUCCEEDED) {
                // The composite event has completed all the requisite stages, though it does not imply that
                // there is data in the FetchBuffer yet. Make sure to clear out the inflight request.
                log.trace("Event {} completed, clearing inflight", inflight);
                inflight = null;
            } else if (state == CompositePollEvent.State.FAILED) {
                // The composite event failed at one of the stages. Make sure to clear out the inflight request
                // before the underlying error is surfaced to the user.
                log.trace("Event {} failed, clearing inflight", inflight);
                inflight = null;

                throw result.asKafkaException();
            } else if (state == CompositePollEvent.State.CALLBACKS_REQUIRED) {
                // The background thread detected that it needed to yield to the application thread to invoke
                // callbacks. Even though the inflight reference _should_ be overwritten when the next stage of
                // the event is submitted, go ahead and clear out the inflight request just to be sure.
                log.trace("Event {} paused for callbacks, clearing inflight", inflight);
                inflight = null;

                // Note: this is calling user-supplied code, so make sure to handle possible errors.
                applicationThreadCallbacks.run();

                // The application thread callbacks are complete. Create another event to resume the polling at
                // the next stage.
                submitEvent(result.asNextEventType(), timer);
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
        long pollTimeMs = time.milliseconds();
        inflight = new CompositePollEvent(deadlineMs, pollTimeMs, type);
        applicationEventHandler.add(inflight);

        if (log.isTraceEnabled())
            log.trace("Submitted new {} with {} remaining on timer", inflight, timer.remainingMs());
    }
}