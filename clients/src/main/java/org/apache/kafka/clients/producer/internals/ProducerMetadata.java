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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;
    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;

    private final boolean allowAutoTopicCreation;
    /* Topics with expiry time */
    private final Map<String, Long> topics = new HashMap<>();
    private final Logger log;
    private final Time time;
    private final long topicExpiryMs;

    // LI-HOTFIX: this constructor should only be used for unit tests
    // after the following hotfix changes:
    // 1) add metadata.topic.expiry.ms config to KafkaProducer
    // 2) Make client-side auto.topic.creation configurable and default to be false
    public ProducerMetadata(long refreshBackoffMs,
        long metadataExpireMs,
        LogContext logContext,
        ClusterResourceListeners clusterResourceListeners,
        Time time) {
        this(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners, time, TOPIC_EXPIRY_MS, true);
    }

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time,
                            long topicExpiryMs,
                            boolean allowAutoTopicCreation) {
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
        this.topicExpiryMs = topicExpiryMs;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), allowAutoTopicCreation);
    }

    public synchronized void add(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            requestUpdateForNewTopics();
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        Long expireMs = topics.get(topic);
        if (expireMs == null) {
            return false;
        } else if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE) {
            topics.put(topic, Long.MAX_VALUE - nowMs > topicExpiryMs ? nowMs + topicExpiryMs : Long.MAX_VALUE);
            return true;
        } else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            topics.remove(topic);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, long now) {
        super.update(requestVersion, response, now);
        notifyAll();
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
