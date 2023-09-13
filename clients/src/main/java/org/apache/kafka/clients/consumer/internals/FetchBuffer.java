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
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

/**
 * {@code FetchBuffer} buffers up {@link CompletedFetch the results} from the broker responses as they are received.
 * It is essentially a wrapper around a {@link java.util.Queue} of {@link CompletedFetch}. There is at most one
 * {@link CompletedFetch} per partition in the queue.
 *
 * <p/>
 *
 * <em>Note</em>: this class is not thread-safe and is intended to only be used from a single thread.
 */
public class FetchBuffer implements Closeable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();

    private CompletedFetch nextInLineFetch;

    public FetchBuffer(final LogContext logContext) {
        this.log = logContext.logger(FetchBuffer.class);
        this.completedFetches = new ConcurrentLinkedQueue<>();
    }

    /**
     * Returns {@code true} if there are no completed fetches pending to return to the user.
     *
     * @return {@code true} if the buffer is empty, {@code false} otherwise
     */
    boolean isEmpty() {
        return completedFetches.isEmpty();
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return {@code true} if there are completed fetches that match the {@link Predicate}, {@code false} otherwise
     */
    boolean hasCompletedFetches(Predicate<CompletedFetch> predicate) {
        return completedFetches.stream().anyMatch(predicate);
    }

    void add(CompletedFetch completedFetch) {
        completedFetches.add(completedFetch);
    }

    void addAll(Collection<CompletedFetch> completedFetches) {
        this.completedFetches.addAll(completedFetches);
    }

    CompletedFetch nextInLineFetch() {
        return nextInLineFetch;
    }

    void setNextInLineFetch(CompletedFetch completedFetch) {
        this.nextInLineFetch = completedFetch;
    }

    CompletedFetch peek() {
        return completedFetches.peek();
    }

    CompletedFetch poll() {
        return completedFetches.poll();
    }

    /**
     * Updates the buffer to retain only the fetch data that corresponds to the given partitions. Any previously
     * {@link CompletedFetch fetched data} is removed if its partition is not in the given set of partitions.
     *
     * @param partitions {@link Set} of {@link TopicPartition}s for which any buffered data should be kept
     */
    void retainAll(final Set<TopicPartition> partitions) {
        completedFetches.removeIf(cf -> maybeDrain(partitions, cf));

        if (maybeDrain(partitions, nextInLineFetch))
            nextInLineFetch = null;
    }

    private boolean maybeDrain(final Set<TopicPartition> partitions, final CompletedFetch completedFetch) {
        if (completedFetch != null && !partitions.contains(completedFetch.partition)) {
            log.debug("Removing {} from buffered fetch data as it is not in the set of partitions to retain ({})", completedFetch.partition, partitions);
            completedFetch.drain();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Return the set of {@link TopicPartition partitions} for which we have data in the buffer.
     *
     * @return {@link TopicPartition Partition} set
     */
    Set<TopicPartition> bufferedPartitions() {
        final Set<TopicPartition> partitions = new HashSet<>();

        if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
            partitions.add(nextInLineFetch.partition);
        }

        completedFetches.forEach(cf -> partitions.add(cf.partition));
        return partitions;
    }

    @Override
    public void close() {
        idempotentCloser.close(() -> {
            log.debug("Closing the fetch buffer");

            if (nextInLineFetch != null) {
                nextInLineFetch.drain();
                nextInLineFetch = null;
            }

            completedFetches.forEach(CompletedFetch::drain);
            completedFetches.clear();
        }, () -> log.warn("The fetch buffer was previously closed"));
    }
}
