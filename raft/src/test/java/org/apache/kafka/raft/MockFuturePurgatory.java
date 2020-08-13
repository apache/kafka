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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class MockFuturePurgatory<T> implements FuturePurgatory<T>, MockTime.Listener {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger();
    private final MockTime time;
    private final PriorityQueue<DelayedFuture> delayedFutures = new PriorityQueue<>();

    public MockFuturePurgatory(MockTime time) {
        this.time = time;
        time.addListener(this);
    }

    @Override
    public CompletableFuture<Long> await(Predicate<T> condition, long maxWaitTimeMs) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        long expirationTimeMs = time.milliseconds() + maxWaitTimeMs;
        delayedFutures.add(new DelayedFuture(expirationTimeMs, condition, future));
        return future;
    }

    @Override
    public void maybeComplete(T value, long currentTimeMs) {
        Iterator<DelayedFuture> iterator = delayedFutures.iterator();
        while (iterator.hasNext()) {
            DelayedFuture delayedFuture = iterator.next();
            if (delayedFuture.canComplete(value)) {
                delayedFuture.future.complete(currentTimeMs);
                iterator.remove();
            }
        }
    }

    @Override
    public void completeAllExceptionally(Throwable exception) {
        while (!delayedFutures.isEmpty()) {
            DelayedFuture delayedFuture = delayedFutures.poll();
            delayedFuture.future.completeExceptionally(exception);
        }
    }

    @Override
    public int numWaiting() {
        return delayedFutures.size();
    }

    @Override
    public void onTimeUpdated() {
        Iterator<DelayedFuture> iterator = delayedFutures.iterator();
        while (iterator.hasNext()) {
            DelayedFuture delayedFuture = iterator.next();
            if (delayedFuture == null || time.milliseconds() < delayedFuture.expirationTime) {
                break;
            } else {
                iterator.remove();

                if (!delayedFuture.future.isDone()) {
                    delayedFuture.future.completeExceptionally(new TimeoutException());
                }
            }
        }
    }

    private class DelayedFuture implements Comparable<DelayedFuture> {
        private final long id;
        private final long expirationTime;
        private final Predicate<T> condition;
        private final CompletableFuture<Long> future;

        private DelayedFuture(
            long expirationTime,
            Predicate<T> condition,
            CompletableFuture<Long> future
        ) {
            this.id = ID_GENERATOR.incrementAndGet();
            this.expirationTime = expirationTime;
            this.condition = condition;
            this.future = future;
        }

        public boolean canComplete(T value) {
            return condition.test(value);
        }

        @Override
        public int compareTo(DelayedFuture o) {
            int compare = Long.compare(expirationTime, o.expirationTime);
            if (compare != 0)
                return compare;
            return Long.compare(id, o.id);
        }
    }
}
