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

import org.apache.kafka.clients.consumer.internals.events.ConsumerRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerResponseEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultEventHandler implements EventHandler<ConsumerRequestEvent, ConsumerResponseEvent> {
    private BlockingQueue<ConsumerRequestEvent> consumerRequestEvents;
    private BlockingQueue<ConsumerResponseEvent> consumerResponseEvents;

    public DefaultEventHandler() {
        this.consumerRequestEvents = new LinkedBlockingQueue<>();
        this.consumerResponseEvents = new LinkedBlockingQueue<>();
        // TODO: a concreted implementation of how requests are being consumed, and how responses are being produced.
    }

    @Override
    public ConsumerResponseEvent poll() {
        return consumerResponseEvents.poll();
    }

    @Override
    public boolean add(ConsumerRequestEvent event) {
        return consumerRequestEvents.add(event);
    }
}
