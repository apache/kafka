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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.queue.KafkaDeadlineEventQueue;

import java.util.Optional;

public class DeadlineTaskManager {
    private final KafkaDeadlineEventQueue<DeferredTask> eventQueue;
    private final Time time;

    DeadlineTaskManager(Time time, KafkaDeadlineEventQueue<DeferredTask> eventQueue) {
        this.eventQueue = eventQueue;
        this.time = time;
    }

    public void addTask(String taskName, DeferredTask task, long timeoutMs) {
        eventQueue.enqueue(new KafkaDeadlineEventQueue.Event<>(time.timer(timeoutMs), taskName, task));
    }

    public void poll(long currentTimeMs) {
        Optional<DeferredTask> taskOpt = eventQueue.dequeue(currentTimeMs);
        taskOpt.ifPresent(deferredTask -> deferredTask.action.run());
    }

    public record DeferredTask(long deadlineMs, Runnable action, Runnable onTimeout) { }
}
