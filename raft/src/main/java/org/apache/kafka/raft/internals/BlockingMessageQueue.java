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
import org.apache.kafka.raft.RaftMessageQueue;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingMessageQueue implements RaftMessageQueue {
    private final BlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger messageCount = new AtomicInteger(0);

    @Override
    public Optional<QueueEntry> poll(long timeoutMs) {
        try {
            var entry = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            while (entry != null && entry.message() == null) {
                // Drain the queue of all of the wakeup events
                entry = queue.poll();
            }
            if (entry != null) {
                messageCount.decrementAndGet();
            }
            return Optional.ofNullable(entry);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public void add(QueueEntry entry) {
        if (entry == null || entry.message() == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Either entry or entry.message is null: %s",
                    entry
                )
            );
        }
        queue.add(entry);
        messageCount.incrementAndGet();
    }

    @Override
    public boolean isEmpty() {
        return messageCount.get() == 0;
    }

    @Override
    public void wakeup() {
        queue.add(new QueueEntry(null));
    }

}
