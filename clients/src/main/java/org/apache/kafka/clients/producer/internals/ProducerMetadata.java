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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    private final long metadataIdleMs;

    private final boolean allowAutoTopicCreation;
    /* Topics with expiry time */
    private final Map<String, Long> topics = new HashMap<>();
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;
    private final long topicExpiryMs;
    private final Sensor metadataRequestRateSensor;
    private final Metrics metrics;

    // LI-HOTFIX: this constructor should only be used for unit tests
    // after the following hotfix changes:
    // 1) add metadata.topic.expiry.ms config to KafkaProducer
    // 2) Make client-side auto.topic.creation configurable and default to be false
    public ProducerMetadata(long refreshBackoffMs,
        long metadataExpireMs,
        long metadataIdleMs,
        LogContext logContext,
        ClusterResourceListeners clusterResourceListeners,
        Time time) {
        this(refreshBackoffMs, metadataExpireMs, metadataIdleMs, logContext, clusterResourceListeners, time, TOPIC_EXPIRY_MS, true, new Metrics());
    }


    public ProducerMetadata(long refreshBackoffMs,
        long metadataExpireMs,
        long metadataIdleMs,
        LogContext logContext,
        ClusterResourceListeners clusterResourceListeners,
        Time time,
        long topicExpiryMs,
        boolean allowAutoTopicCreation,
        Metrics metrics) {
        this(refreshBackoffMs, metadataExpireMs, metadataIdleMs, logContext, clusterResourceListeners, time,
            topicExpiryMs, allowAutoTopicCreation, metrics, Long.MAX_VALUE);
    }

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time,
                            long topicExpiryMs,
                            boolean allowAutoTopicCreation,
                            Metrics metrics,
                            long clusterMetadataExpireMs) {
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners, clusterMetadataExpireMs);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
        this.topicExpiryMs = topicExpiryMs;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.metrics = metrics;
        this.metadataRequestRateSensor = metrics.sensor("producer-metadata-request-rate");
        MetricName requestRate = metrics.metricName("producer-metadata-request-rate",
            "producer-metrics",
            "The average per-second number of metadata request sent by the producer");
        MetricName requestTotal = metrics.metricName("producer-metadata-request-total",
            "producer-metrics",
            "The total number of metadata request sent by the producer");

        this.metadataRequestRateSensor.add(new Meter(requestRate, requestTotal));
    }
    public void recordMetadataRequest() {
        this.metadataRequestRateSensor.record();
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), allowAutoTopicCreation);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), allowAutoTopicCreation);
    }

    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic);
            requestUpdateForNewTopics();
        }
    }

    public synchronized int requestUpdateForTopic(String topic) {
        if (newTopics.contains(topic)) {
            return requestUpdateForNewTopics();
        } else {
            return requestUpdate();
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        Long expireMs = topics.get(topic);
        if (expireMs == null) {
            return false;
        } else if (newTopics.contains(topic)) {
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
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        super.update(requestVersion, response, isPartialUpdate, nowMs);

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }

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
