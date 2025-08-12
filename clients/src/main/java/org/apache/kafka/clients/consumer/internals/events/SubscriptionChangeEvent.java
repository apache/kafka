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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.util.Optional;

/**
 * Application event indicating that the subscription state has changed, triggered when a user
 * calls the subscribe API. This will make the consumer join a consumer group if not part of it
 * yet, or just send the updated subscription to the broker if it's already a member of the group.
 */
public abstract class SubscriptionChangeEvent extends CompletableApplicationEvent<Void> {

    private final Optional<ConsumerRebalanceListener> listener;

    public SubscriptionChangeEvent(final Type type, final Optional<ConsumerRebalanceListener> listener, final long deadlineMs) {
        super(type, deadlineMs);
        this.listener = listener;
    }

    public Optional<ConsumerRebalanceListener> listener() {
        return listener;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", listener=" + listener;
    }
}
