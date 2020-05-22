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

import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MockFuturePurgatory<T> implements FuturePurgatory<T>, MockTime.Listener {
    private final AtomicLong idGenerator = new AtomicLong(1L);
    private final MockTime time;
    private final PriorityQueue<DelayedFuture> delayedFutures = new PriorityQueue<>();

    public MockFuturePurgatory(MockTime time) {
        this.time = time;
        time.addListener(this);
    }

    @Override
    public void await(
        CompletableFuture<T> future,
        long maxWaitTimeMs
    ) {
        long id = idGenerator.getAndIncrement();
        long expirationTimeMs = time.milliseconds() + maxWaitTimeMs;
        delayedFutures.add(new DelayedFuture(id, expirationTimeMs, future));
    }

    @Override
    public void completeAll(T value) {
        while (!delayedFutures.isEmpty()) {
            DelayedFuture delayedFuture = delayedFutures.poll();
            delayedFuture.future.complete(value);
        }
    }

    @Override
    public int numWaiting() {
        return delayedFutures.size();
    }

    @Override
    public void onTimeUpdated() {
        while (true) {
            DelayedFuture delayedFuture = delayedFutures.peek();
            if (delayedFuture == null || time.milliseconds() < delayedFuture.expirationTime) {
                return;
            } else {
                delayedFutures.poll();

                if (!delayedFuture.future.isDone()) {
                    delayedFuture.future.completeExceptionally(new TimeoutException());
                }
            }
        }
    }

    private class DelayedFuture implements Comparable<DelayedFuture> {
        private final long id;
        private final long expirationTime;
        private final CompletableFuture<T> future;

        private DelayedFuture(
            long id,
            long expirationTime,
            CompletableFuture<T> future
        ) {
            this.id = id;
            this.expirationTime = expirationTime;
            this.future = future;
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
