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

import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 */
public class FetchState<K, V> {

    private final Logger log;
    private final LogContext logContext;
    private final BufferSupplier decompressionBufferSupplier;
    private final ConcurrentLinkedQueue<CompletedFetch<K, V>> completedFetches;
    private final Map<Integer, FetchSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingFetchRequests;
    private CompletedFetch<K, V> nextInLineFetch;

    public FetchState(final LogContext logContext) {
        this.log = logContext.logger(FetchState.class);
        this.logContext = logContext;
        this.decompressionBufferSupplier = BufferSupplier.create();
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingFetchRequests = new HashSet<>();
    }

    public LogContext logContext() {
        return logContext;
    }

    public BufferSupplier decompressionBufferSupplier() {
        return decompressionBufferSupplier;
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions
     *
     * @param assignedPartitions  newly assigned {@link TopicPartition}
     */
    public void clearBufferedDataForUnassignedPartitions(final Collection<TopicPartition> assignedPartitions) {
        final Iterator<CompletedFetch<K, V>> completedFetchesItr = completedFetches.iterator();

        while (completedFetchesItr.hasNext()) {
            final CompletedFetch<K, V> completedFetch = completedFetchesItr.next();
            final TopicPartition tp = completedFetch.partition();

            if (!assignedPartitions.contains(tp)) {
                log.debug("Removing {} from buffered data as it is no longer an assigned partition", tp);
                completedFetch.drain();
                completedFetchesItr.remove();
            }
        }

        if (nextInLineFetch != null && !assignedPartitions.contains(nextInLineFetch.partition())) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }
    }

    public Set<TopicPartition> bufferedDataForTopicPartitions() {
        final Set<TopicPartition> topicPartitions = new HashSet<>();

        if (nextInLineFetch != null && !nextInLineFetch.isConsumed())
            topicPartitions.add(nextInLineFetch.partition());

        topicPartitions.addAll(completedFetches.stream().map(CompletedFetch::partition).collect(Collectors.toSet()));
        return topicPartitions;
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return true if there are completed fetches, false otherwise
     */
    public boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    public boolean hasCompletedFetches(final Predicate<CompletedFetch<K, V>> predicate) {
        return completedFetches.stream().anyMatch(predicate);
    }

    public void addCompletedFetch(final CompletedFetch<K, V> fetch) {
        completedFetches.add(fetch);
    }

    public void addCompletedFetches(final Collection<CompletedFetch<K, V>> fetches) {
        completedFetches.addAll(fetches);
    }

    public CompletedFetch<K, V> firstCompletedFetch() {
        return completedFetches.peek();
    }

    public void dropFirstCompletedFetch() {
        completedFetches.poll();
    }

    public Collection<FetchSessionHandler> sessionHandlers() {
        return Collections.unmodifiableCollection(sessionHandlers.values());
    }

    public FetchSessionHandler sessionHandlerOrDefault(final LogContext logContext, final Node node) {
        return sessionHandlers.computeIfAbsent(node.id(), k -> new FetchSessionHandler(logContext, k));
    }

    public FetchSessionHandler sessionHandler(final Node node) {
        return sessionHandlers.get(node.id());
    }

    public CompletedFetch<K, V> nextInLineFetch() {
        return nextInLineFetch;
    }

    public void setNextInLineFetch(final CompletedFetch<K, V> nextInLineFetch) {
        this.nextInLineFetch = nextInLineFetch;
    }

    public void addPendingRequest(final Node node) {
        log.debug("Adding pending request for node {}", node);
        nodesWithPendingFetchRequests.add(node.id());
    }

    public void removePendingRequest(final Node node) {
        log.debug("Removing pending request for node {}", node);
        nodesWithPendingFetchRequests.remove(node.id());
    }

    public boolean hasPendingRequest(final Node node) {
        return nodesWithPendingFetchRequests.contains(node.id());
    }

    public void close() {
        if (nextInLineFetch != null) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }

        Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
        sessionHandlers.clear();
    }

}
