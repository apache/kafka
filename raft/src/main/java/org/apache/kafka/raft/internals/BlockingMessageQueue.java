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

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.raft.RaftMessage;
import org.apache.kafka.raft.RaftMessageQueue;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingMessageQueue implements RaftMessageQueue {
    private final BlockingQueue<InternalQueueEntry> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger messageCount = new AtomicInteger(0);

    /**
     * Internal queue entry type used to discriminate between messages and wakeup signals.
     *
     * This sealed interface ensures type safety when polling the queue.
     */
    private sealed interface InternalQueueEntry { }

    /**
     * Marker entry used to unblock threads waiting on {@link #poll(long)} without delivering a message.
     *
     * Wakeup entries are drained during polling and do not contribute to the message count.
     */
    private record WakeupMarker() implements InternalQueueEntry { }

    private static final WakeupMarker WAKEUP = new WakeupMarker();

    /**
     * A queue entry that contains a message and its associated future.
     */
    private static final class MessageEntry implements QueueEntry, InternalQueueEntry {
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
        try {
            InternalQueueEntry entry = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            // Drain all wakeup markers until we find a message or the queue is empty
            while (entry instanceof WakeupMarker) {
                entry = queue.poll();
            }
            if (entry instanceof MessageEntry messageEntry) {
                messageCount.decrementAndGet();
                return Optional.of(messageEntry);
            }
            return Optional.empty();
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public CompletionStage<RaftMessage> add(RaftMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("message cannot be null");
        }
        var entry = new MessageEntry(message);
        queue.add(entry);
        messageCount.incrementAndGet();
        return entry.future();
    }

    @Override
    public boolean isEmpty() {
        return messageCount.get() == 0;
    }

    @Override
    public void wakeup() {
        queue.add(WAKEUP);
    }
}
