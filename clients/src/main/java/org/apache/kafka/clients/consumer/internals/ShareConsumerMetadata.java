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
import java.util.List;

public class ShareConsumerMetadata extends Metadata {
    private final boolean allowAutoTopicCreation;
    private final SubscriptionState subscription;

    public ShareConsumerMetadata(long refreshBackoffMs,
                                 long refreshBackoffMaxMs,
                                 long metadataExpireMs,
                                 boolean allowAutoTopicCreation,
                                 SubscriptionState subscription,
                                 LogContext logContext,
                                 ClusterResourceListeners clusterResourceListeners) {
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.subscription = subscription;
    }

    public ShareConsumerMetadata(ConsumerConfig config,
                                 SubscriptionState subscriptions,
                                 LogContext logContext,
                                 ClusterResourceListeners clusterResourceListeners) {
        this(config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
            config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
            config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
            config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
            subscriptions,
            logContext,
            clusterResourceListeners);
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    /**
     * Constructs a metadata request builder for fetching cluster metadata for the topics the share consumer needs.
     */
    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        List<String> topics = new ArrayList<>();
        topics.addAll(subscription.metadataTopics());
        return MetadataRequest.Builder.forTopicNames(topics, allowAutoTopicCreation);
    }

    /**
     * Check if the metadata for the topic should be retained, based on the topic name.
     */
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return subscription.needsMetadata(topic);
    }
}
