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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * An event handler that receives {@link BackgroundEvent background events} from the
 * {@link ConsumerNetworkThread network thread} which are then made available to the application thread
 * via an {@link EventProcessor}.
 */

public class ShareAcknowledgementEventHandler {

    private final BlockingQueue<ShareAcknowledgementEvent> eventQueue;

    public ShareAcknowledgementEventHandler(final BlockingQueue<ShareAcknowledgementEvent> eventQueue) {
        this.eventQueue = eventQueue;
    }

    /**
     * Add a {@link ShareAcknowledgementEvent} to the handler.
     *
     * @param event A {@link ShareAcknowledgementEvent} created by the {@link ConsumerNetworkThread network thread}
     */
    public void add(ShareAcknowledgementEvent event) {
        Objects.requireNonNull(event, "ShareAcknowledgementCompleteEvent provided to add must be non-null");
        eventQueue.add(event);
    }

    /**
     * Drain all the {@link ShareAcknowledgementEvent events} from the handler.
     *
     * @return A list of {@link ShareAcknowledgementEvent events} that were drained
     */
    public List<ShareAcknowledgementEvent> drainEvents() {
        List<ShareAcknowledgementEvent> events = new ArrayList<>();
        eventQueue.drainTo(events);
        return events;
    }
}
