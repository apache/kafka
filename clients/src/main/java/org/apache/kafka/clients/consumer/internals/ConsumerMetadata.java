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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.utils.LogContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConsumerMetadata extends Metadata {
    private final boolean includeInternalTopics;
    private final boolean allowAutoTopicCreation;
    private final SubscriptionState subscription;
    private final Set<String> transientTopics;

    public ConsumerMetadata(long refreshBackoffMs,
                            long refreshBackoffMaxMs,
                            long metadataExpireMs,
                            boolean includeInternalTopics,
                            boolean allowAutoTopicCreation,
                            SubscriptionState subscription,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners) {
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.includeInternalTopics = includeInternalTopics;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.subscription = subscription;
        this.transientTopics = new HashSet<>();
    }

    public ConsumerMetadata(ConsumerConfig config,
                            SubscriptionState subscriptions,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners) {
        this(config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                subscriptions,
                logContext,
                clusterResourceListeners);
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        if (subscription.hasPatternSubscription() || subscription.hasRe2JPatternSubscription())
            return MetadataRequest.Builder.allTopics();
        List<String> topics = new ArrayList<>();
        topics.addAll(subscription.metadataTopics());
        topics.addAll(transientTopics);
        return new MetadataRequest.Builder(topics, allowAutoTopicCreation);
    }

    synchronized void addTransientTopics(Set<String> topics) {
        this.transientTopics.addAll(topics);
        if (!fetch().topics().containsAll(topics))
            requestUpdateForNewTopics();
    }

    synchronized void clearTransientTopics() {
        this.transientTopics.clear();
    }

    @Override
    protected synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        if (transientTopics.contains(topic) || subscription.needsMetadata(topic))
            return true;

        if (isInternal && !includeInternalTopics)
            return false;

        return subscription.matchesSubscribedPattern(topic);
    }
}
