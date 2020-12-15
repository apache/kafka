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
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.HashMap;
import java.util.Map;

public class MockStateRestoreListener implements StateRestoreListener {

    // verifies store name called for each state
    public final Map<String, String> storeNameCalledStates = new HashMap<>();
    public long restoreStartOffset;
    public long restoreEndOffset;
    public long restoredBatchOffset;
    public long numBatchRestored;
    public long totalNumRestored;
    public TopicPartition restoreTopicPartition;

    public static final String RESTORE_START = "restore_start";
    public static final String RESTORE_BATCH = "restore_batch";
    public static final String RESTORE_END = "restore_end";

    @Override
    public void onRestoreStart(final TopicPartition topicPartition,
                               final String storeName,
                               final long startingOffset,
                               final long endingOffset) {
        restoreTopicPartition = topicPartition;
        storeNameCalledStates.put(RESTORE_START, storeName);
        restoreStartOffset = startingOffset;
        restoreEndOffset = endingOffset;
    }

    @Override
    public void onBatchRestored(final TopicPartition topicPartition,
                                final String storeName,
                                final long batchEndOffset,
                                final long numRestored) {
        restoreTopicPartition = topicPartition;
        storeNameCalledStates.put(RESTORE_BATCH, storeName);
        restoredBatchOffset = batchEndOffset;
        numBatchRestored = numRestored;
    }

    @Override
    public void onRestoreEnd(final TopicPartition topicPartition,
                             final String storeName,
                             final long totalRestored) {
        restoreTopicPartition = topicPartition;
        storeNameCalledStates.put(RESTORE_END, storeName);
        totalNumRestored = totalRestored;
    }

    @Override
    public String toString() {
        return "MockStateRestoreListener{" +
               "storeNameCalledStates=" + storeNameCalledStates +
               ", restoreStartOffset=" + restoreStartOffset +
               ", restoreEndOffset=" + restoreEndOffset +
               ", restoredBatchOffset=" + restoredBatchOffset +
               ", numBatchRestored=" + numBatchRestored +
               ", totalNumRestored=" + totalNumRestored +
               ", restoreTopicPartition=" + restoreTopicPartition +
               '}';
    }
}
