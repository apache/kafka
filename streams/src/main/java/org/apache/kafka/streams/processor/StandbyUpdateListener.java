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
     * Method called upon the creation of the Standby Task.
     *
     * @param topicPartition   the TopicPartition of the Standby Task.
     * @param storeName        the name of the store being watched by this Standby Task.
     * @param startingOffset   the offset from which the Standby Task starts watching.
     * @param currentEndOffset the current latest offset on the associated changelog partition.
     */
    void onUpdateStart(final TopicPartition topicPartition,
                       final String storeName,
                       final long startingOffset,
                       final long currentEndOffset);

    /**
     * Method called after restoring a batch of records.  In this case the maximum size of the batch is whatever
     * the value of the MAX_POLL_RECORDS is set to.
     *
     * This method is called after restoring each batch and it is advised to keep processing to a minimum.
     * Any heavy processing will hold up recovering the next batch, hence slowing down the restore process as a
     * whole.
     *
     * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName the name of the store undergoing restoration
     * @param batchEndOffset the inclusive ending offset for the current restored batch for this TopicPartition
     * @param numRestored the total number of records restored in this batch for this TopicPartition
     * @param currentEndOffset the current end offset of the changelog topic partition.
     */
    void onBatchLoaded(final TopicPartition topicPartition,
                       final String storeName,
                       final TaskId taskId,
                       final long batchEndOffset,
                       final long numRestored,
                       final long currentEndOffset);

    /**
     * Method called after a Standby Task is closed, either because the Standby Task was promoted to an Active Task
     * or because the Standby Task was migrated to another instance (in which case the data will be cleaned up
     * after state.cleanup.delay.ms).
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName the name of the store undergoing restoration
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