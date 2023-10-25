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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An event handler that receives {@link ApplicationEvent application events} from the application thread which
 * are then readable from the {@link ApplicationEventProcessor} in the {@link ConsumerNetworkThread network thread}.
 */
public class ApplicationEventHandler {

    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;

    public ApplicationEventHandler(final LogContext logContext,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue) {
        this.log = logContext.logger(ApplicationEventHandler.class);
        this.applicationEventQueue = applicationEventQueue;
    }

    /**
     * Add an {@link ApplicationEvent} to the handler.
     *
     * @param event An {@link ApplicationEvent} created by the application thread
     */
    public void add(final ApplicationEvent event) {
        Objects.requireNonNull(event, "ApplicationEvent provided to add must be non-null");
        log.trace("Enqueued event: {}", event);
        applicationEventQueue.add(event);
    }

    /**
     * Add a {@link CompletableApplicationEvent} to the handler. The method blocks waiting for the result, and will
     * return the result value upon successful completion; otherwise throws an error.
     *
     * <p/>
     *
     * See {@link CompletableApplicationEvent#get(Timer)} and {@link Future#get(long, TimeUnit)} for more details.
     *
     * @param event A {@link CompletableApplicationEvent} created by the polling thread
     * @param timer Timer for which to wait for the event to complete
     * @return      Value that is the result of the event
     * @param <T>   Type of return value of the event
     */
    public <T> T addAndGet(final CompletableApplicationEvent<T> event, final Timer timer) {
        Objects.requireNonNull(event, "CompletableApplicationEvent provided to addAndGet must be non-null");
        Objects.requireNonNull(timer, "Timer provided to addAndGet must be non-null");
        add(event);
        return event.get(timer);
    }
}
