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

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mocked implementation which does not block in {@link #poll(long)}..
 */
public class MockMessageQueue implements RaftMessageQueue {
    private final Queue<QueueEntry> messages = new ArrayDeque<>();
    private final AtomicBoolean wakeupRequested = new AtomicBoolean(false);
    private final AtomicLong lastPollTimeout = new AtomicLong(-1);

    private static final class MessageEntry implements QueueEntry {
        private final CompletableFuture<RaftMessage> future = new CompletableFuture<>();
        private final RaftMessage message;

        MessageEntry(RaftMessage message) {
            this.message = message;
        }

        @Override
        public RaftMessage message() {
            return message;
        }

        @Override
        public CompletableFuture<RaftMessage> future() {
            return future;
        }

        @Override
        public String toString() {
            return String.format(
                "MessageEntry(message=%s, future.isDone=%s)",
                message,
                future.isDone()
            );
        }
    }

    @Override
    public Optional<QueueEntry> poll(long timeoutMs) {
        wakeupRequested.set(false);
        lastPollTimeout.set(timeoutMs);
        return Optional.ofNullable(messages.poll());
    }

    @Override
    public CompletionStage<RaftMessage> add(RaftMessage message) {
        MessageEntry entry = new MessageEntry(message);
        messages.offer(entry);
        return entry.future();
    }

    public OptionalLong lastPollTimeoutMs() {
        long lastTimeoutMs = lastPollTimeout.get();
        if (lastTimeoutMs < 0) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(lastTimeoutMs);
        }
    }

    public boolean wakeupRequested() {
        return wakeupRequested.get();
    }

    @Override
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Override
    public void wakeup() {
        wakeupRequested.set(true);
    }
}
