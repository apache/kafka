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

package org.apache.kafka.streams.processor.internals;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Collection;

class CompositeRestoreListener implements BatchingStateRestoreCallback, StateRestoreListener {

    private final InternalStateRestoreAdapter stateRestoreAdapter;
    private final StateRestoreListener storeRestoreListener;
    private StateRestoreListener reportingStoreListener = new NoOpStateRestoreListener();

    CompositeRestoreListener(StateRestoreCallback stateRestoreCallback) {

        if (stateRestoreCallback instanceof StateRestoreListener) {
            storeRestoreListener = (StateRestoreListener) stateRestoreCallback;
        } else {
            storeRestoreListener = new NoOpStateRestoreListener();
        }

        if (stateRestoreCallback == null) {
            stateRestoreCallback = new NoOpStateRestoreCallback();
        }

        this.stateRestoreAdapter = new InternalStateRestoreAdapter(stateRestoreCallback);
    }

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName,
                               long startingOffset, long endingOffset) {
        reportingStoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
        storeRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName,
                                long batchEndOffset, long numRestored) {
        reportingStoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
        storeRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName,
                             long totalRestored) {
        reportingStoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
        storeRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);

    }

    @Override
    public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
        stateRestoreAdapter.restoreAll(records);
    }

    void setReportingStoreListener(StateRestoreListener reportingStoreListener) {
        if (reportingStoreListener != null) {
            this.reportingStoreListener = reportingStoreListener;
        }
    }

    @Override
    public void restore(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Single restore functionality shouldn't be called directly but "
                                                + "through the delegated StateRestoreCallback instance");
    }


    private static final class NoOpStateRestoreListener implements StateRestoreListener {

        @Override
        public void onRestoreStart(TopicPartition topicPartition, String storeName,
                                   long startingOffset, long endingOffset) {

        }

        @Override
        public void onBatchRestored(TopicPartition topicPartition, String storeName,
                                    long batchEndOffset, long numRestored) {

        }

        @Override
        public void onRestoreEnd(TopicPartition topicPartition, String storeName,
                                 long totalRestored) {

        }
    }

    private static final class NoOpStateRestoreCallback implements StateRestoreCallback {

        @Override
        public void restore(byte[] key, byte[] value) {

        }
    }
}
