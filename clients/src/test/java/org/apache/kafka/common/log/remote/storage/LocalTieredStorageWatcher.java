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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.lang.String.*;
import static java.util.stream.Collectors.*;

/**
 * Provides support to wait on internal modifications from the local tiered storage.
 */
public final class LocalTieredStorageWatcher {
    private final Map<TopicPartition, AtomicInteger> remainingSegments;
    private final CountDownLatch latch;

    public static LocalTieredStorageWatcher.Builder newWatcherBuilder() {
        return new LocalTieredStorageWatcher.Builder();
    }

    /**
     * Wait for the provided number of segments to be available in the tiered storage. Only the number of
     * segments provided for the topic-partition registered with this waiter is accounted for.
     *
     * @param timeout the maximum time to wait.
     * @param unit the time unit of the {@code timeout} argument.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting.
     * @throws TimeoutException if the time specified by {@code timeout} elapsed before all segments were reported.
     */
    public void watch(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        LOGGER.debug("Waiting on segments from topic-partitions: {}", remainingSegments);

        if (!latch.await(timeout, unit)) {
            throw new TimeoutException(
                    format("Timed out before all segments were offloaded to the remote storage. " +
                            "Remaining segments: %s", remainingSegments));
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTieredStorageWatcher.class);

    private final class InternalListener implements LocalTieredStorageListener {
        @Override
        public void onTopicPartitionCreated(TopicPartition topicPartition) {
        }

        @Override
        public void onSegmentOffloaded(final RemoteLogSegmentFileset remoteFileset) {
            final RemoteLogSegmentId id = remoteFileset.getRemoteLogSegmentId();
            LOGGER.debug("Segment uploaded to remote storage: {}", id);

            final AtomicInteger remaining = remainingSegments.get(id.topicPartition());

            if (remaining != null && remaining.decrementAndGet() >= 0) {
                latch.countDown();
            }
        }
    }

    private LocalTieredStorageWatcher(final Builder builder) {
        this.remainingSegments = Collections.unmodifiableMap(builder.segmentCountDowns);
        final int segmentCount = remainingSegments.values().stream().collect(summingInt(AtomicInteger::get));
        this.latch = new CountDownLatch(segmentCount);
    }

    public static final class Builder {
        private final Map<TopicPartition, AtomicInteger> segmentCountDowns = new HashMap<>();

        /**
         * Add the expected number of segments from the provided topic-partitions as part of the condition
         * to wait upon.
         */
        public Builder addSegmentsToWaitFor(final TopicPartition topicPartition, final int numberOfSegments) {
            segmentCountDowns.compute(topicPartition, (k, v) -> {
                int current = v == null ? 0 : v.get();
                return new AtomicInteger(current + numberOfSegments);
            });
            return this;
        }

        /**
         * Builds a new waiter listening for notifications from the given storage.
         */
        public LocalTieredStorageWatcher create(final LocalTieredStorage storage) {
            final LocalTieredStorageWatcher waiter = new LocalTieredStorageWatcher(this);
            storage.addListener(waiter.new InternalListener());
            return waiter;
        }
    }
}
