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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.ExpirationService;

import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class ThresholdPurgatory<T extends Comparable<T>> implements FuturePurgatory<T> {
    private final AtomicLong idGenerator = new AtomicLong(0);
    private final ExpirationService expirationService;
    private final ConcurrentNavigableMap<ThresholdKey<T>, CompletableFuture<Long>> thresholdMap =
        new ConcurrentSkipListMap<>();

    public ThresholdPurgatory(ExpirationService expirationService) {
        this.expirationService = expirationService;
    }

    @Override
    public CompletableFuture<Long> await(T threshold, long maxWaitTimeMs) {
        ThresholdKey<T> key = new ThresholdKey<>(idGenerator.incrementAndGet(), threshold);
        CompletableFuture<Long> future = expirationService.failAfter(maxWaitTimeMs);
        thresholdMap.put(key, future);
        future.whenComplete((timeMs, exception) -> thresholdMap.remove(key));
        return future;
    }

    @Override
    public void maybeComplete(T value, long currentTimeMs) {
        ThresholdKey<T> maxKey = new ThresholdKey<>(Long.MAX_VALUE, value);
        NavigableMap<ThresholdKey<T>, CompletableFuture<Long>> submap = thresholdMap.headMap(maxKey);
        for (CompletableFuture<Long> completion : submap.values()) {
            completion.complete(currentTimeMs);
        }
    }

    @Override
    public void completeAll(long currentTimeMs) {
        for (CompletableFuture<Long> completion : thresholdMap.values()) {
            completion.complete(currentTimeMs);
        }
    }

    @Override
    public void completeAllExceptionally(Throwable exception) {
        for (CompletableFuture<Long> completion : thresholdMap.values()) {
            completion.completeExceptionally(exception);
        }
    }

    @Override
    public int numWaiting() {
        return thresholdMap.size();
    }

    private static class ThresholdKey<T extends Comparable<T>> implements Comparable<ThresholdKey<T>> {
        private final long id;
        private final T threshold;

        private ThresholdKey(long id, T threshold) {
            this.id = id;
            this.threshold = threshold;
        }

        @Override
        public int compareTo(ThresholdKey<T> o) {
            int res = this.threshold.compareTo(o.threshold);
            if (res != 0) {
                return res;
            } else {
                return Long.compare(this.id, o.id);
            }
        }
    }

}
