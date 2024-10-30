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
import java.util.Set;

/**
 * Application event indicating that a user calls the subscribe API for new topics.
 * This will make the consumer join a consumer group if not part of it yet,
 * or just send the updated subscription to the broker on the next poll.
 */
public class TopicSubscriptionChangeEvent extends SubscriptionChangeEvent {
    private final Set<String> topics;
    public TopicSubscriptionChangeEvent(final Set<String> topics, final Optional<ConsumerRebalanceListener> listener, final long deadlineMs) {
        super(Type.TOPIC_SUBSCRIPTION_CHANGE, listener, deadlineMs);
        this.topics = topics;
    }

    public Set<String> topics() {
        return topics;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", topics=" + topics;
    }
}
