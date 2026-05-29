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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The default share group DLQ manager responsible for processing
 * incoming messages and writing them to the appropriate dlq topic.
 */
public class DefaultShareGroupDLQManager implements ShareGroupDLQManager {
    /**
     * Reference to state manager responsible for actually sending
     * the relevant RPCs and writing records.
     */
    private final ShareGroupDLQStateManager stateManager;

    private static final Logger log = LoggerFactory.getLogger(DefaultShareGroupDLQManager.class);

    public static ShareGroupDLQManager instance(KafkaClient client, ShareGroupDLQMetadataCacheHelper cacheHelper, Time time, Timer timer) {
        DefaultShareGroupDLQManager instance = new DefaultShareGroupDLQManager(client, cacheHelper, time, timer);
        instance.start();
        return instance;
    }

    private DefaultShareGroupDLQManager(KafkaClient client, ShareGroupDLQMetadataCacheHelper cacheHelper, Time time, Timer timer) {
        this.stateManager = new ShareGroupDLQStateManager(client, cacheHelper, time, timer);
    }

    private void start() {
        this.stateManager.start();
    }

    @Override
    public CompletableFuture<Void> enqueue(ShareGroupDLQRecordParameter param) {
        try {
            validate(param);
        } catch (Exception e) {
            log.error("Unable to validate dlq record parameters", e);
            return CompletableFuture.failedFuture(e);
        }
        return stateManager.dlq(param);
    }

    @Override
    public void stop() {
        try {
            stateManager.stop();
        } catch (Exception e) {
            log.error("Unable to stop DLQ state manager", e);
        }
    }

    private static void validate(ShareGroupDLQRecordParameter param) {
        String prefix = "DLQ records parameters";
        if (param == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }

        if (param.groupId() == null || param.groupId().isEmpty()) {
            throw new IllegalArgumentException(prefix + " group cannot be null or empty.");
        }

        if (param.topicIdPartition() == null) {
            throw new IllegalArgumentException(prefix + " topic/partition data cannot be null or empty.");
        }

        if (param.topicIdPartition().topicId() == null) {
            throw new IllegalArgumentException(prefix + " topic id data cannot be null or empty.");
        }

        if (param.topicIdPartition().partition() < 0) {
            throw new IllegalArgumentException(prefix + " partition cannot be negative.");
        }

        if (param.lastOffset() < param.firstOffset()) {
            throw new IllegalArgumentException(prefix + " last offset cannot be less than first offset.");
        }

        if (param.firstOffset() < 0) {
            throw new IllegalArgumentException(prefix + " first offset cannot be negative.");
        }

        if (param.lastOffset() < 0) {
            throw new IllegalArgumentException(prefix + " last offset cannot be negative.");
        }
    }
}
