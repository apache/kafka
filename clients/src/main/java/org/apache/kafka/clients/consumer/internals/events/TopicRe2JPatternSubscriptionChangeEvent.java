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
import org.apache.kafka.clients.consumer.SubscriptionPattern;

import java.util.Optional;

/**
 * Application event indicating triggered by a call to the subscribe API
 * providing a {@link SubscriptionPattern} (RE2J-compatible pattern).
 * This will make the consumer send the updated subscription to the
 * broker on the next poll, joining the group if it is not already part of it.
 */
public class TopicRe2JPatternSubscriptionChangeEvent extends SubscriptionChangeEvent {
    private final SubscriptionPattern pattern;

    public TopicRe2JPatternSubscriptionChangeEvent(final SubscriptionPattern pattern,
                                                   final Optional<ConsumerRebalanceListener> listener,
                                                   final long deadlineMs) {
        super(Type.TOPIC_RE2J_PATTERN_SUBSCRIPTION_CHANGE, listener, deadlineMs);
        this.pattern = pattern;
    }

    public SubscriptionPattern pattern() {
        return pattern;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", subscriptionPattern=" + pattern;
    }
}
