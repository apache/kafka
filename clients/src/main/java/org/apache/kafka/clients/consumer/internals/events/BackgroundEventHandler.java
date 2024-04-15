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
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Queue;

/**
 * An event handler that receives {@link BackgroundEvent background events} from the
 * {@link ConsumerNetworkThread network thread} which are then made available to the application thread
 * via an {@link EventProcessor}.
 */

public class BackgroundEventHandler {

    private final Logger log;
    private final Queue<BackgroundEvent> backgroundEventQueue;

    public BackgroundEventHandler(final LogContext logContext, final Queue<BackgroundEvent> backgroundEventQueue) {
        this.log = logContext.logger(BackgroundEventHandler.class);
        this.backgroundEventQueue = backgroundEventQueue;
    }

    /**
     * Add a {@link BackgroundEvent} to the handler.
     *
     * @param event A {@link BackgroundEvent} created by the {@link ConsumerNetworkThread network thread}
     */
    public void add(BackgroundEvent event) {
        Objects.requireNonNull(event, "BackgroundEvent provided to add must be non-null");
        backgroundEventQueue.add(event);
        log.trace("Enqueued event: {}", event);
    }
}
