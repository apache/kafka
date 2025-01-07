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

public final class MockExpirationService implements ExpirationService, MockTime.Listener {
    private final AtomicLong idGenerator = new AtomicLong(0);
    private final MockTime time;
    private final PriorityQueue<ExpirationFuture<?>> queue = new PriorityQueue<>();

    public MockExpirationService(MockTime time) {
        this.time = time;
        time.addListener(this);
    }

    @Override
    public <T> CompletableFuture<T> failAfter(long timeoutMs) {
        long deadlineMs = time.milliseconds() + timeoutMs;
        long id = idGenerator.incrementAndGet();
        ExpirationFuture<T> future = new ExpirationFuture<>(id, deadlineMs);
        queue.add(future);
        return future;
    }

    @Override
    public void onTimeUpdated() {
        long currentTimeMs = time.milliseconds();
        while (true) {
            ExpirationFuture<?> future = queue.peek();
            if (future == null || future.deadlineMs > currentTimeMs) {
                break;
            }
            ExpirationFuture<?> polled = queue.poll();
            polled.completeExceptionally(new TimeoutException());
        }
    }

    private static class ExpirationFuture<T> extends CompletableFuture<T> implements Comparable<ExpirationFuture<?>> {
        private final long id;
        private final long deadlineMs;

        private ExpirationFuture(long id, long deadlineMs) {
            this.id = id;
            this.deadlineMs = deadlineMs;
        }

        @Override
        public int compareTo(ExpirationFuture<?> o) {
            int res = Long.compare(this.deadlineMs, o.deadlineMs);
            if (res != 0) {
                return res;
            } else {
                return Long.compare(this.id, o.id);
            }
        }
    }

}
