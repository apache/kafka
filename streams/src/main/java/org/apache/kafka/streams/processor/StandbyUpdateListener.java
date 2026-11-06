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
import org.apache.kafka.common.annotation.InterfaceAudience;

/**
 * Class for listening to the progress of the standby and warm-up tasks of a Kafka Streams client.
 *
 * <p>When calling {@link org.apache.kafka.streams.KafkaStreams#setStandbyUpdateListener(StandbyUpdateListener)}
 * the passed instance is expected to be stateless since the {@code StandbyUpdateListener} is shared across all state
 * updater threads, of which there is one per stream thread ({@code num.stream.threads}).
 *
 * <p>Users desiring stateful operations will need to provide synchronization internally in
 * the {@code StandbyUpdateListener} implementation.
 *
 * <p>Note that this listener is only registered at the per-client level and users can use the {@code storeName}
 * parameter to define specific monitoring for different {@link StateStore StateStores}.
 *
 * <p>Warm-up tasks are reported through this interface as well as standby tasks.
 * A warm-up task is one that the group hands out so that this client can build a task's state up before taking the task
 * over, and the runtime runs it as a standby task, so the two are indistinguishable to this listener.
 * Relabelling a task between the two roles does not restart the task, and does not trigger a callback; a task might be
 * converted from a warm-up task to a standby task between two calls of {@link #onBatchLoaded} transparently.
 *
 * <p>Also note that, unlike the restoration of an active task, the update of a standby task never finishes, since it
 * keeps applying the changelog records written by the active task.
 * There is therefore no callback corresponding to
 * {@link StateRestoreListener#onRestoreEnd(TopicPartition, String, long)}; a standby task is reported only once it
 * stops being updated, through {@link #onUpdateSuspended(TopicPartition, String, long, long, SuspendReason)}.
 *
 * <p>Even if warm-up tasks are expected to "complete" restoration, the same pattern as for standby tasks applies.
 * Once a warm-up task is considered hot (based on {@code acceptable.recovery.lag}), its restoration will be
 * suspended when it is promoted to an active task, and the active task will complete the restoration (if
 * registered, the corresponding {@link StateRestoreListener} callback is invoked on the active task for this
 * case).
 *
 * <p>Incremental updates are exposed so users can estimate how much progress has been made.
 *
 * @see StateRestoreListener
 */
@InterfaceAudience.Public
public interface StandbyUpdateListener {

    /**
     * The reason why a standby or warm-up task stopped being updated.
     */
    enum SuspendReason {
        /**
         * The task stopped being updated as a standby or warm-up task, and its state store was closed.
         * This covers every reason other than a promotion: the task was revoked, or it was assigned to another instance,
         * or this instance is shutting down.
         */
        MIGRATED,

        /**
         * The task stopped being updated as a standby or warm-up task because this instance is taking it over as an
         * active task.
         * Its state store stays open and keeps its contents, so the active task resumes restoring where the update left
         * off rather than from the beginning of the changelog.
         * Restoring as an active task is reported through {@link StateRestoreListener}.
         */
        PROMOTED
    }

    /**
     * Method called at the very beginning of {@link StateStore} updating.
     *
     * @param topicPartition
     *        the changelog {@link TopicPartition} for this task
     * @param storeName
     *        the name of the store being loaded
     * @param startingOffset
     *        the offset from which the task begins consuming from the changelog
     */
    void onUpdateStart(
        final TopicPartition topicPartition,
        final String storeName,
        final long startingOffset
    );

    /**
     * Method called after loading a batch of records.
     * In this case the maximum size of the batch is whatever the value of {@code max.poll.records} is set to.
     *
     * <p>This method is called after loading each batch and it is advised to keep processing to a minimum.
     * Any heavy processing will block the state updater thread and slow down the rate of standby task loading.
     * Therefore, if you need to do any extended processing or connect to an external service, consider doing so
     * asynchronously.
     *
     * @param topicPartition
     *        the changelog {@link TopicPartition} for this task
     * @param storeName
     *        the name of the store being loaded
     * @param batchEndOffset
     *        the changelog end offset (inclusive) of the batch that was just loaded
     * @param batchSize
     *        the total number of records in the batch that was just loaded
     * @param currentEndOffset
     *        the current end offset of the changelog topic partition
     */
    default void onBatchLoaded(
        final TopicPartition topicPartition,
        final String storeName,
        final long batchEndOffset,
        final long batchSize,
        final long currentEndOffset
    ) {
    }

    /**
     * Method called after loading a batch of records.
     *
     * @param topicPartition
     *        the changelog {@link TopicPartition} for this task
     * @param storeName
     *        the name of the store being loaded
     * @param taskId
     *        the {@link TaskId} of the task that owns the store being loaded
     * @param batchEndOffset
     *        the changelog end offset (inclusive) of the batch that was just loaded
     * @param batchSize
     *        the total number of records in the batch that was just loaded
     * @param currentEndOffset
     *        the current end offset of the changelog topic partition
     * @deprecated Since 4.5. Use {@link #onBatchLoaded(TopicPartition, String, long, long, long)} instead.
     */
    @Deprecated(since = "4.5")
    default void onBatchLoaded(
        final TopicPartition topicPartition,
        final String storeName,
        final TaskId taskId,
        final long batchEndOffset,
        final long batchSize,
        final long currentEndOffset
    ) {
        onBatchLoaded(topicPartition, storeName, batchEndOffset, batchSize, currentEndOffset);
    }

    /**
     * Method called when the corresponding standby or warm-up task stops being updated, for the provided reason.
     *
     * <p>If the reason is {@link SuspendReason#MIGRATED}, this callback will be invoked after this state store (and the
     * task itself) are closed, in which case the data will be cleaned up after {@code state.cleanup.delay.ms}.
     * If the task was {@link SuspendReason#PROMOTED} to an active task, the state store will not be closed, and the
     * callback will be invoked after unregistering it as a standby task but before re-registering it as an active task
     * and beginning restoration.
     * In other words, this will always be called before the corresponding
     * {@link StateRestoreListener#onRestoreStart(TopicPartition, String, long, long)} call is made.
     *
     * @param topicPartition
     *        the changelog {@link TopicPartition} for this task
     * @param storeName
     *        the name of the store being loaded
     * @param storeOffset
     *        the offset of the last changelog record that was read and put into the store at the time of suspension
     * @param currentEndOffset
     *        the current end offset of the changelog topic partition
     * @param reason
     *        the reason why the task stopped being updated
     */
    void onUpdateSuspended(
        final TopicPartition topicPartition,
        final String storeName,
        final long storeOffset,
        final long currentEndOffset,
        final SuspendReason reason
    );
}
