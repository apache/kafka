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
package org.apache.kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This queue is used to collect actions which need to be executed later. One use case is that ReplicaManager#appendRecords
 * produces record changes, so we need to check and complete delayed requests. In order to avoid conflicting locking,
 * we add those actions to this queue and then complete them at the end of KafkaApis.handle() or DelayedJoin.onExpiration.
 */
public class DelayedActionQueue implements ActionQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedActionQueue.class);
    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

    @Override
    public void add(Runnable action) {
        queue.add(action);
    }

    @Override
    public void tryCompleteActions() {
        int maxToComplete = queue.size();
        for (int count = 0; count < maxToComplete; count++) {
            try {
                Runnable action = queue.poll();
                if (action == null) return;
                action.run();
            } catch (Throwable e) {
                LOGGER.error("failed to complete delayed actions", e);
            }
        }
    }
}
