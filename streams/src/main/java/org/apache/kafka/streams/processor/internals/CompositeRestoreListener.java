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


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Collection;

class CompositeRestoreListener implements BatchingStateRestoreCallback, StateRestoreListener {

    private final StateRestoreCallback stateRestoreCallback;
    private final BatchingStateRestoreCallback batchingStateRestoreCallback;
    private final StateRestoreListener storeRestoreListener;
    private StateRestoreListener reportingStoreListener;

    CompositeRestoreListener(StateRestoreCallback stateRestoreCallback) {
        this.stateRestoreCallback = stateRestoreCallback;
        this.batchingStateRestoreCallback = stateRestoreCallback instanceof BatchingStateRestoreCallback ?
                                            (BatchingStateRestoreCallback) stateRestoreCallback : null;

        if (stateRestoreCallback instanceof StateRestoreListener) {
            storeRestoreListener = (StateRestoreListener) stateRestoreCallback;
        } else if (batchingStateRestoreCallback instanceof StateRestoreListener) {
            storeRestoreListener = (StateRestoreListener) batchingStateRestoreCallback;
        } else {
            storeRestoreListener = new NoOpStateRestoreListener();
        }
    }

    @Override
    public void onRestoreStart(String storeName, long startingOffset, long endingOffset) {
        reportingStoreListener.onRestoreStart(storeName, startingOffset, endingOffset);
        storeRestoreListener.onBatchRestored(storeName, startingOffset, endingOffset);
    }

    @Override
    public void onBatchRestored(String storeName, long batchEndOffset, long numRestored) {
        reportingStoreListener.onBatchRestored(storeName, batchEndOffset, numRestored);
        storeRestoreListener.onBatchRestored(storeName, batchEndOffset, numRestored);
    }

    @Override
    public void onRestoreEnd(String storeName, long totalRestored) {
        reportingStoreListener.onRestoreEnd(storeName, totalRestored);
        storeRestoreListener.onRestoreEnd(storeName, totalRestored);

    }

    @Override
    public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
        if (batchingStateRestoreCallback != null) {
            batchingStateRestoreCallback.restoreAll(records);
        } else {
            for (KeyValue<byte[], byte[]> record : records) {
                restore(record.key, record.value);
            }
        }
    }

    void setReportingStoreListener(StateRestoreListener reportingStoreListener) {
        this.reportingStoreListener = (reportingStoreListener != null) ? reportingStoreListener : new NoOpStateRestoreListener();
    }

    @Override
    public void restore(byte[] key, byte[] value) {
        stateRestoreCallback.restore(key, value);
    }


    private static final class NoOpStateRestoreListener implements StateRestoreListener {

        @Override
        public void onRestoreStart(String storeName, long startingOffset, long endingOffset) {

        }

        @Override
        public void onBatchRestored(String storeName, long batchEndOffset, long numRestored) {

        }

        @Override
        public void onRestoreEnd(String storeName, long totalRestored) {

        }
    }
}
