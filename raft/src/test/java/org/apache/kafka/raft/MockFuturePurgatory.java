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

public class MockFuturePurgatory<T extends Comparable<T>> implements FuturePurgatory<T>, MockTime.Listener {
    private final MockTime time;
    private final PriorityQueue<DelayedFuture> delayedFutures = new PriorityQueue<>();

    public MockFuturePurgatory(MockTime time) {
        this.time = time;
        time.addListener(this);
    }

    @Override
    public CompletableFuture<Long> await(T value, long maxWaitTimeMs) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        long expirationTimeMs = time.milliseconds() + maxWaitTimeMs;
        delayedFutures.add(new DelayedFuture(value, expirationTimeMs, future));

        return future;
    }

    @Override
    public void complete(T value, long currentTimeMs) {
        while (!delayedFutures.isEmpty()) {
            if (delayedFutures.peek().id.compareTo(value) < 0) {
                DelayedFuture delayedFuture = delayedFutures.poll();
                delayedFuture.future.complete(currentTimeMs);
            } else {
                break;
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
        private final T id;
        private final long expirationTime;
        private final CompletableFuture<Long> future;

        private DelayedFuture(
            T id,
            long expirationTime,
            CompletableFuture<Long> future
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
            return id.compareTo(o.id);
        }
    }
}
