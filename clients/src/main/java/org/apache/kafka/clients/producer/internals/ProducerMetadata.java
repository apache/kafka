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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    private final long metadataIdleMs;

    /* Topics with expiry time */
    private final Map<String, Long> topics = new ConcurrentHashMap<>();
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;
    private Map<String, Errors> errors = null;

    public ProducerMetadata(long refreshBackoffMs,
                            long refreshBackoffMaxMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    /**
     * Add a topic to the working set, refreshing its expiry. Already-known topics are updated
     * lock-free via {@link ConcurrentHashMap#replace}; only topics that are new (or were concurrently
     * evicted) fall back to a synchronized block so the map insert and {@code newTopics} bookkeeping
     * happen atomically with {@link #retainTopic}. Called on every {@code send()}.
     */
    public void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        // Fast path: topic already known - update expiry without acquiring the instance lock.
        if (topics.replace(topic, nowMs + metadataIdleMs) != null) {
            return;
        }
        // Slow path: topic is new (or was concurrently evicted). Enter the lock so the map
        // insert and newTopics bookkeeping happen atomically with retainTopic().
        synchronized (this) {
            if (topics.put(topic, nowMs + metadataIdleMs) == null) {
                newTopics.add(topic);
                requestUpdateForNewTopics();
            }
        }
    }

    /**
     * Add a batch of topics to the working set, refreshing the expiry for those already present.
     * Already-known topics are updated lock-free via {@link ConcurrentHashMap#replace}; only topics
     * that are new (or were concurrently evicted) fall back to a synchronized block so the map insert
     * and {@code newTopics} bookkeeping happen atomically with {@link #retainTopic}.
     * If any topic was newly added, a partial metadata refresh is requested and the current
     * updateVersion is returned so the caller can pass it to {@link #awaitUpdate(int, long)} to
     * wait for the next response. Returns an empty {@code OptionalInt} when no topic was newly added.
     */
    public OptionalInt add(Collection<String> topics, long nowMs) {
        // Fast path: refresh expiry for all already-known topics without acquiring the lock.
        List<String> newOrEvicted = null;
        for (String topic : topics) {
            if (this.topics.replace(topic, nowMs + metadataIdleMs) == null) {
                if (newOrEvicted == null) newOrEvicted = new ArrayList<>();
                newOrEvicted.add(topic);
            }
        }
        if (newOrEvicted == null) return OptionalInt.empty();

        // Slow path: enter the lock once for the whole batch of truly-new topics.
        synchronized (this) {
            boolean anyNew = false;
            for (String topic : newOrEvicted) {
                if (this.topics.put(topic, nowMs + metadataIdleMs) == null) {
                    newTopics.add(topic);
                    anyNew = true;
                }
            }
            return anyNew ? OptionalInt.of(requestUpdateForNewTopics()) : OptionalInt.empty();
        }
    }

    public synchronized int requestUpdateForTopic(String topic) {
        if (newTopics.contains(topic)) {
            return requestUpdateForNewTopics();
        } else {
            return requestUpdate(false);
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

    /**
     * Returns whether the given topic should be retained in the metadata cache. Idle topics
     * (whose expiry has passed) are evicted via a conditional {@link ConcurrentHashMap#remove(Object, Object)}
     * so that a concurrent {@link #add} that refreshed the expiry just before this call is not undone.
     */
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        Long expireMs = topics.get(topic);
        if (expireMs == null) {
            return false;
        } else if (newTopics.contains(topic)) {
            return true;
        } else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            // Use conditional remove so a concurrent add() that refreshed the expiry after
            // our get() above is not accidentally undone.
            topics.remove(topic, expireMs);
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
        errors = response.errors();

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }

        notifyAll();
    }

    public Errors getError(final String topic) {
        if (errors != null) {
            return errors.get(topic);
        }
        return null;
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
