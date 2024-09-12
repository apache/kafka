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
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.raft.RaftMessage;
import org.apache.kafka.raft.RaftMessageQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingMessageQueue implements RaftMessageQueue {
    private static final RaftMessage WAKEUP_MESSAGE = new RaftMessage() {
        @Override
        public int correlationId() {
            return 0;
        }

        @Override
        public ApiMessage data() {
            return null;
        }
    };

    private final BlockingQueue<RaftMessage> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger size = new AtomicInteger(0);

    @Override
    public RaftMessage poll(long timeoutMs) {
        try {
            RaftMessage message = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (message == null || message == WAKEUP_MESSAGE) {
                return null;
            } else {
                size.decrementAndGet();
                return message;
            }
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public void add(RaftMessage message) {
        queue.add(message);
        size.incrementAndGet();
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public void wakeup() {
        queue.add(WAKEUP_MESSAGE);
    }

}
