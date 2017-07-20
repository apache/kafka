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

/**
 * Abstract implementation of the  {@link BatchingStateRestoreCallback} used for batch restoration operations.
 *
 * Includes default no-op methods of the {@link StateRestoreListener} {@link StateRestoreListener#onRestoreStart(TopicPartition, String, long, long)},
 * {@link StateRestoreListener#onBatchRestored(TopicPartition, String, long, long)}, and {@link StateRestoreListener#onRestoreEnd(TopicPartition, String, long)}.
 */
public abstract class AbstractNotifyingBatchingRestoreCallback
    implements BatchingStateRestoreCallback, StateRestoreListener {

    /**
     * Single put restore operations not supported, please use {@link AbstractNotifyingRestoreCallback}
     * or {@link StateRestoreCallback} instead for single action restores.
     */
    @Override
    public void restore(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Single restore not supported");
    }


    /**
     * @see StateRestoreListener#onRestoreStart(TopicPartition, String, long, long)
     *
     * <p>
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     * </p>
     *
     * @param topicPartition the TopicPartition containing the values to restore.
     * @param storeName      the name of the store undergoing restoration.
     * @param startingOffset the starting offset of the entire restoration process for this TopicPartition.
     * @param endingOffset   the ending offset of the entire restoration process for this TopicPartition.
     */
    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset,
                               long endingOffset) {

    }


    /**
     * @see StateRestoreListener#onBatchRestored(TopicPartition, String, long, long)
     *
     * <p>
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     * </p>
     *
     * @param topicPartition the TopicPartition containing the values to restore.
     * @param storeName the name of the store undergoing restoration.
     * @param batchEndOffset the ending offset for the current restored batch for this TopicPartition.
     * @param numRestored the total number of records restored in this batch for this TopicPartition.
     */
    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset,
                                long numRestored) {

    }

    /**
     * @see StateRestoreListener#onRestoreEnd(TopicPartition, String, long)
     *
     * <p>
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     * </p>
     *
     *
     * @param topicPartition the TopicPartition containing the values to restore.
     * @param storeName the name of the store just restored.
     * @param totalRestored the total number of records restored for this TopicPartition.
     */
    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {

    }
}
