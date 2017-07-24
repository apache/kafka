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
 * Abstract implementation of the  {@link StateRestoreCallback} used for batch restoration operations.
 *
 * Includes default no-op methods of the {@link StateRestoreListener} {@link StateRestoreListener#onRestoreStart(TopicPartition, String, long, long)},
 * {@link StateRestoreListener#onBatchRestored(TopicPartition, String, long, long)}, and {@link StateRestoreListener#onRestoreEnd(TopicPartition, String, long)}.
 */
public abstract class AbstractNotifyingRestoreCallback implements StateRestoreCallback, StateRestoreListener {


    /**
     * @see StateRestoreListener#onRestoreStart(TopicPartition, String, long, long)
     *
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     *
     */
    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset,
                               long endingOffset) {

    }


    /**
     * @see StateRestoreListener#onBatchRestored(TopicPartition, String, long, long)
     *
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     *
     */
    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset,
                                long numRestored) {

    }

    /**
     * @see StateRestoreListener#onRestoreEnd(TopicPartition, String, long)
     *
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     *
     */
    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {

    }
}
