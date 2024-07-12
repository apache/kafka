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
package org.apache.kafka.server.util.timer;

import org.apache.kafka.server.util.MockTime;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MockTimer implements Timer {
    private final MockTime time;
    private final PriorityQueue<TimerTaskEntry> taskQueue = new PriorityQueue<>(
        Comparator.comparingLong(entry -> entry.expirationMs)
    );

    public MockTimer() {
        this(new MockTime());
    }

    public MockTimer(MockTime time) {
        this.time = time;
    }

    @Override
    public void add(TimerTask timerTask) {
        if (timerTask.delayMs <= 0) {
            timerTask.run();
        } else {
            synchronized (taskQueue) {
                taskQueue.add(new TimerTaskEntry(timerTask, timerTask.delayMs + time.milliseconds()));
            }
        }
    }

    @Override
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        time.sleep(timeoutMs);

        final long now = time.milliseconds();
        boolean executed = false;
        boolean hasMore = true;

        while (hasMore) {
            hasMore = false;
            TimerTaskEntry taskEntry = null;

            synchronized (taskQueue) {
                if (!taskQueue.isEmpty() && now > taskQueue.peek().expirationMs) {
                    taskEntry = taskQueue.poll();
                    hasMore = !taskQueue.isEmpty();
                }
            }

            if (taskEntry != null) {
                if (!taskEntry.cancelled()) {
                    taskEntry.timerTask.run();
                    executed = true;
                }
            }
        }

        return executed;
    }

    public MockTime time() {
        return time;
    }

    public int size() {
        return taskQueue.size();
    }

    public PriorityQueue<TimerTaskEntry> taskQueue() {
        return taskQueue;
    }

    @Override
    public void close() throws Exception {}
}
