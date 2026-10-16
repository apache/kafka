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
 * Class for listening to various states of the restoration process of a StateStore.
 *
 * <p>When calling {@link org.apache.kafka.streams.KafkaStreams#setGlobalStateRestoreListener(StateRestoreListener)}
 * the passed instance is expected to be stateless since the {@code StateRestoreListener} is shared across all state
 * updater threads, of which there is one per stream thread ({@code num.stream.threads}).
 *
 * <p>Users desiring stateful operations will need to provide synchronization internally in
 * the {@code StateRestoreListener} implementation.
 *
 * <p>Note that this listener is only registered at the per-client level and users can use the {@code storeName}
 * parameter to define specific monitoring for different {@link StateStore StateStores}.
 *
 * <p>{@code StateRestoreListener} must not be confused with the {@link StateRestoreCallback} interface.
 * The {@code StateRestoreListener} is a callback to observe restoration progress.
 * In contrast, the {@link StateRestoreCallback} interface is used by custom state store implementations to provide
 * the actual state restoration functionality.
 *
 * <p>Also note that the update process of standby tasks is not monitored via this interface, since a standby task does
 * not actually <em>restore</em> state, but keeps updating its state from the changelogs written by the active task
 * which does not ever finish.
 * The same holds for warm-up tasks, which the runtime runs as standby tasks.
 * Both are monitored via {@link StandbyUpdateListener} instead.
 *
 * <p>Incremental updates are exposed so users can estimate how much progress has been made.
 *
 * @see StandbyUpdateListener
 */
@InterfaceAudience.Public
public interface StateRestoreListener {

    /**
     * Method called at the very beginning of {@link StateStore} restoration.
     *
     * <p>Called for every active task that begins restoring, including one that has nothing to restore because its
     * local state is already up to date -- a task whose state was just built up as a standby or warm-up task, for
     * instance.
     * In that case {@code startingOffset} equals {@code endingOffset} and
     * {@link #onRestoreEnd(TopicPartition, String, long)} follows with a total of zero records restored, so an empty
     * restoration is told apart by the offsets and the count rather than by the callbacks being skipped.
     *
     * @param topicPartition
     *        the {@link TopicPartition} containing the values to restore
     * @param storeName
     *        the name of the store undergoing restoration
     * @param startingOffset
     *        the starting offset of the entire restoration process for this {@link TopicPartition}
     * @param endingOffset
     *        the exclusive ending offset of the entire restoration process for this {@link TopicPartition}
     */
    void onRestoreStart(
        final TopicPartition topicPartition,
        final String storeName,
        final long startingOffset,
        final long endingOffset
    );

    /**
     * Method called after restoring a batch of records.
     * In this case the maximum size of the batch is whatever the value of {@code max.poll.records} is set to.
     *
     * <p>This method is called after restoring each batch and it is advised to keep processing to a minimum.
     * Any heavy processing will block the state updater thread and hold up recovering the next batch, hence slowing
     * down the restore process as a whole.
     * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
     *
     * @param topicPartition
     *        the {@link TopicPartition} containing the values to restore
     * @param storeName
     *        the name of the store undergoing restoration
     * @param batchEndOffset
     *        the inclusive ending offset for the current restored batch for this {@link TopicPartition}
     * @param numRestored
     *        the total number of records restored in this batch for this {@link TopicPartition}
     */
    void onBatchRestored(
        final TopicPartition topicPartition,
        final String storeName,
        final long batchEndOffset,
        final long numRestored
    );

    /**
     * Method called when restoring the {@link StateStore} is complete.
     *
     * @param topicPartition
     *        the {@link TopicPartition} containing the values to restore
     * @param storeName
     *        the name of the store just restored
     * @param totalRestored
     *        the total number of records restored for this {@link TopicPartition}
     */
    void onRestoreEnd(
        final TopicPartition topicPartition,
        final String storeName,
        final long totalRestored
    );

    /**
     * Method called when restoring the {@link StateStore} is suspended before it finished, because the task stopped
     * being an active task on this host: it was assigned elsewhere, demoted to a standby task, or this host is shutting
     * down (the concrete reason is not reported).
     * If the task becomes an active task on this host again -- assigned back to it, or promoted from the standby task
     * it was demoted to -- another {@link #onRestoreStart(TopicPartition, String, long, long)} would be called.
     *
     * <p>A task whose restoration had already finished, and was reported through
     * {@link #onRestoreEnd(TopicPartition, String, long)}, is not reported here when it is closed later on.
     * In particular, an active task that had finished restoring and transitioned into the {@code RUNNING} state and
     * is then demoted to a standby task is not reported here at all; it surfaces only as a
     * {@link StandbyUpdateListener#onUpdateStart(TopicPartition, String, long)} for the standby task that replaces it.
     *
     * @param topicPartition
     *        the {@link TopicPartition} containing the values to restore
     * @param storeName
     *        the name of the store just restored
     * @param totalRestored
     *        the total number of records restored for this {@link TopicPartition} before being suspended
     */
    default void onRestoreSuspended(
        final TopicPartition topicPartition,
        final String storeName,
        final long totalRestored) {
    }
}
