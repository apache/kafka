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
package org.apache.kafka.test;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.TaskId;

import java.util.HashMap;
import java.util.Map;

public final class MockStandbyUpdateListener implements StandbyUpdateListener {

    public final Map<String, String> storeNameCalledUpdate = new HashMap<>();
    public static final String UPDATE_BATCH = "update_batch";
    public static final String UPDATE_SUSPENDED = "update_suspended";

    public static final String UPDATE_START = "update_start";

    public SuspendReason updateSuspendedReason;

    public TopicPartition updatePartition;

    @Override
    public void onUpdateStart(TopicPartition partition, String storeName, long startingOffset) {
        storeNameCalledUpdate.put(UPDATE_START, storeName);
        this.updatePartition = partition;
    }

    @Override
    public void onBatchLoaded(TopicPartition topicPartition, String storeName, TaskId taskId, long batchEndOffset, long batchSize, long currentEndOffset) {
        storeNameCalledUpdate.put(UPDATE_BATCH, storeName);
    }

    @Override
    public void onUpdateSuspended(TopicPartition partition, String storeName, long storeOffset, long currentEndOffset, SuspendReason reason) {
        storeNameCalledUpdate.put(UPDATE_SUSPENDED, storeName);
        this.updateSuspendedReason = reason;
        this.updatePartition = partition;
    }

    public String capturedStore(String callbackName) {
        return storeNameCalledUpdate.get(callbackName);
    }
}