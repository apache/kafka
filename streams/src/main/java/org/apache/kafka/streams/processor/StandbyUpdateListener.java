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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.TopicPartition;

public interface StandbyUpdateListener {

    enum SuspendReason {
        MIGRATED,
        PROMOTED
    }

    /**
     * A callback that will be invoked after registering the changelogs for each state store in a standby
     * task. It is guaranteed to always be invoked before any records are loaded into the standby store.
     *
     * @param topicPartition the changelog TopicPartition for this standby task
     * @param storeName the name of the store being loaded
     * @param startingOffset the offset from which the standby task begins consuming from the changelog
     */
    void onUpdateStart(final TopicPartition topicPartition,
                       final String storeName,
                       final long startingOffset);

    /**
     * Method called after loading a batch of records. In this case the maximum size of the batch is whatever
     * the value of the MAX_POLL_RECORDS is set to.
     * <n>
     * This method is called after loading each batch and it is advised to keep processing to a minimum.
     * Any heavy processing will block the state updater thread and slow down the rate of standby task 
     * loading. Therefore, if you need to do any extended processing or connect to an external service,
     * consider doing so asynchronously.
     *
     * @param topicPartition the changelog TopicPartition for this standby task
     * @param storeName the name of the store being loaded
     * @param batchEndOffset batchEndOffset the changelog end offset (inclusive) of the batch that was just loaded
     * @param batchSize the total number of records in the batch that was just loaded
     * @param currentEndOffset the current end offset of the changelog topic partition.
     */
    void onBatchLoaded(final TopicPartition topicPartition,
                       final String storeName,
                       final TaskId taskId,
                       final long batchEndOffset,
                       final long batchSize,
                       final long currentEndOffset);

    /**
     * This method is called when the corresponding standby task stops updating, for the provided reason.
     * <p>
     * If the task was {@code MIGRATED} to another instance, this callback will be invoked after this
     * state store (and the task itself) are closed (in which case the data will be cleaned up after 
     * state.cleanup.delay.ms).
     * If the task was {@code PROMOTED} to an active task, the state store will not be closed, and the 
     * callback will be invoked after unregistering it as a standby task but before re-registering it as an active task 
     * and beginning restoration. In other words, this will always called before the corresponding 
     * {@link StateRestoreListener#onRestoreStart} call is made.
     *
     * @param topicPartition the changelog TopicPartition for this standby task
     * @param storeName the name of the store being loaded
     * @param storeOffset is the offset of the last changelog record that was read and put into the store at the time
     * of suspension.
     * @param currentEndOffset the current end offset of the changelog topic partition.
     * @param reason is the reason why the standby task was suspended.
     */
    void onUpdateSuspended(final TopicPartition topicPartition,
                           final String storeName,
                           final long storeOffset,
                           final long currentEndOffset,
                           final SuspendReason reason);
}