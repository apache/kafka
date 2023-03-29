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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

public class FetchBuffer<K, V> implements Closeable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedFetch<K, V>> completedFetches;

    private CompletedFetch<K, V> nextInLineFetch;

    public FetchBuffer(final LogContext logContext) {
        this.log = logContext.logger(FetchBuffer.class);
        this.completedFetches = new ConcurrentLinkedQueue<>();
    }

    /**
     * Return whether we have any completed fetches pending return to the user.
     *
     * @return {@code true} if there are completed fetches, {@code false} otherwise
     */
    public boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return {@code true} if there are completed fetches that match the {@link Predicate}, {@code false} otherwise
     */
    public boolean hasCompletedFetches(Predicate<CompletedFetch<K, V>> predicate) {
        return completedFetches.stream().anyMatch(predicate);
    }

    public void add(CompletedFetch<K, V> completedFetch) {
        completedFetches.add(completedFetch);
    }

    public void addAll(Collection<CompletedFetch<K, V>> completedFetches) {
        this.completedFetches.addAll(completedFetches);
    }

    public CompletedFetch<K, V> nextInLineFetch() {
        return nextInLineFetch;
    }

    public void setNextInLineFetch(CompletedFetch<K, V> completedFetch) {
        this.nextInLineFetch = completedFetch;
    }

    public CompletedFetch<K, V> peek() {
        return completedFetches.peek();
    }

    public CompletedFetch<K, V> poll() {
        return completedFetches.poll();
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions. Any previously
     * {@link CompletedFetch fetched data} is dropped if it is for a partition that is no longer in
     * {@code assignedPartitions}.
     *
     * @param assignedPartitions Newly-assigned {@link TopicPartition}
     */
    public void clearForUnassignedPartitions(final Set<TopicPartition> assignedPartitions) {
        final Iterator<CompletedFetch<K, V>> completedFetchesItr = completedFetches.iterator();

        while (completedFetchesItr.hasNext()) {
            final CompletedFetch<K, V> completedFetch = completedFetchesItr.next();
            final TopicPartition tp = completedFetch.partition;

            if (!assignedPartitions.contains(tp)) {
                log.debug("Removing {} from buffered data as it is no longer an assigned partition", tp);
                completedFetch.drain();
                completedFetchesItr.remove();
            }
        }

        if (nextInLineFetch != null && !assignedPartitions.contains(nextInLineFetch.partition)) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }
    }

    public Set<TopicPartition> partitions() {
        Set<TopicPartition> partitions = new HashSet<>();

        if (nextInLineFetch != null && !nextInLineFetch.isConsumed) {
            partitions.add(nextInLineFetch.partition);
        }

        for (CompletedFetch<K, V> completedFetch : completedFetches) {
            partitions.add(completedFetch.partition);
        }

        return partitions;
    }

    @Override
    public void close() {
        if (nextInLineFetch != null) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }
    }
}
