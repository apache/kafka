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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;

public class CompositePollEventInvoker {

    private final Logger log;
    private final Time time;
    private final ApplicationEventHandler applicationEventHandler;
    private final Runnable backgroundEventProcessor;
    private final Runnable offsetCommitProcessor;
    private CompositePollEvent latest;

    public CompositePollEventInvoker(LogContext logContext,
                                     Time time,
                                     ApplicationEventHandler applicationEventHandler,
                                     Runnable backgroundEventProcessor,
                                     Runnable offsetCommitProcessor) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.applicationEventHandler = applicationEventHandler;
        this.backgroundEventProcessor = backgroundEventProcessor;
        this.offsetCommitProcessor = offsetCommitProcessor;
    }

    public void poll(Timer timer) {
        if (latest == null) {
            log.debug("latest was null, so submitting new event...");
            submitEvent(ApplicationEvent.Type.POLL, timer);
        }

        try {
            log.debug("Attempting to retrieve result from previously submitted {} with {} remaining on timer", latest, timer.remainingMs());

            CompositePollEvent.Result result = latest.resultOrError();
            CompositePollEvent.State state = result.state();
            log.debug("Retrieved result: {}, with state: {}", result, state);

            if (state == CompositePollEvent.State.COMPLETE) {
                // Make sure to clear out the latest request since it's complete.
                log.debug("We're supposedly complete with event {}, so clearing...", latest);
                latest = null;
            } else if (state == CompositePollEvent.State.BACKGROUND_EVENT_PROCESSING_REQUIRED) {
                log.debug("About to process background events");
                backgroundEventProcessor.run();
                log.debug("Done processing background events");
                result.nextEventType().ifPresent(t -> submitEvent(t, timer));
            } else if (state == CompositePollEvent.State.OFFSET_COMMIT_CALLBACKS_REQUIRED) {
                log.debug("About to process offset commits");
                offsetCommitProcessor.run();
                log.debug("Done processing offset commits");
                result.nextEventType().ifPresent(t -> submitEvent(t, timer));
            } else if (state == CompositePollEvent.State.UNKNOWN) {
                throw new KafkaException("Unexpected poll result received");
            }
        } catch (Throwable t) {
            log.debug("Caught error, rethrowing...", t);
            // If an exception is hit, bubble it up to the user but make sure to clear out the latest request
            // to signify this one is complete.
            latest = null;
            throw ConsumerUtils.maybeWrapAsKafkaException(t);
        }
    }

    private void submitEvent(ApplicationEvent.Type type, Timer timer) {
        long deadlineMs = calculateDeadlineMs(timer);
        latest = new CompositePollEvent(deadlineMs, time.milliseconds(), type);
        applicationEventHandler.add(latest);
        log.debug("Submitted new {} submitted with {} remaining on timer", latest, timer.remainingMs());
    }
}