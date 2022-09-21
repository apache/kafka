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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class interfaces the KafkaConsumer and the background thread.  It allows the caller to enqueue {@link ApplicationEvent}
 * to be consumed by the background thread and poll {@linkBackgroundEvent} produced by the background thread.
 */
public class DefaultEventHandler implements EventHandler {
    private final BlockingQueue<ApplicationEvent> applicationEvents;
    private final BlockingQueue<BackgroundEvent> backgroundEvents;

    public DefaultEventHandler() {
        this.applicationEvents = new LinkedBlockingQueue<>();
        this.backgroundEvents = new LinkedBlockingQueue<>();
        // TODO: a concreted implementation of how requests are being consumed, and how responses are being produced.
    }

    @Override
    public Optional<BackgroundEvent> poll() {
        return Optional.ofNullable(backgroundEvents.poll());
    }

    @Override
    public boolean isEmpty() {
        return backgroundEvents.isEmpty();
    }

    @Override
    public boolean add(ApplicationEvent event) {
        try {
            return applicationEvents.add(event);
        } catch (IllegalStateException e) {
            // swallow the capacity restriction exception
            return false;
        }
    }
}
