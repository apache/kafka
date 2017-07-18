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
import org.apache.kafka.test.MockStateRestoreListener;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;

import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class CompositeRestoreListenerTest {

    private final MockStateRestoreCallback stateRestoreCallback = new MockStateRestoreCallback();
    private final MockBatchingStateRestoreCallback
        batchingStateRestoreCallback =
        new MockBatchingStateRestoreCallback();
    private final MockNoListenBatchingStateRestoreCallback
        noListenBatchingStateRestoreCallback =
        new MockNoListenBatchingStateRestoreCallback();
    private final MockStateRestoreListener reportingStoreListener = new MockStateRestoreListener();
    private final byte[] key = "key".getBytes(Charset.forName("UTF-8"));
    private final byte[] value = "value".getBytes(Charset.forName("UTF-8"));
    private final Collection<KeyValue<byte[], byte[]>> records = Collections.singletonList(KeyValue.pair(key, value));
    private final String storeName = "test_store";
    private final long startOffset = 0L;
    private final long endOffset = 1L;
    private final long batchOffset = 1L;
    private final long numberRestored = 1L;

    private CompositeRestoreListener compositeRestoreListener;


    @Test
    public void shouldReportRestoreProgressInSinglePutRestore() {
        compositeRestoreListener = new CompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.setReportingStoreListener(reportingStoreListener);

        compositeRestoreListener.restoreAll(records);
        compositeRestoreListener.onRestoreStart(storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(storeName, numberRestored);

        assertThat(stateRestoreCallback.restoredKey, is(key));
        assertThat(stateRestoreCallback.restoredValue, is(value));

        assertStateRestoreListener(stateRestoreCallback);
        assertStateRestoreListener(reportingStoreListener);
    }

    @Test
    public void shouldReportRestoreProgressInBatchRestore() {
        compositeRestoreListener = new CompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.setReportingStoreListener(reportingStoreListener);

        compositeRestoreListener.restoreAll(records);
        compositeRestoreListener.onRestoreStart(storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(storeName, numberRestored);

        assertThat(batchingStateRestoreCallback.restoredRecords, is(records));
        assertStateRestoreListener(batchingStateRestoreCallback);
        assertStateRestoreListener(reportingStoreListener);
    }

    @Test
    public void shouldHandleNullReportStoreListener() {
        compositeRestoreListener = new CompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.setReportingStoreListener(null);

        compositeRestoreListener.restoreAll(records);
        compositeRestoreListener.onRestoreStart(storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(storeName, numberRestored);

        assertThat(batchingStateRestoreCallback.restoredRecords, is(records));
        assertStateRestoreListener(batchingStateRestoreCallback);
    }

    @Test
    public void shouldHandleNoRestoreListener() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.setReportingStoreListener(null);

        compositeRestoreListener.restoreAll(records);
        compositeRestoreListener.onRestoreStart(storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(storeName, numberRestored);

        assertThat(noListenBatchingStateRestoreCallback.restoredRecords, is(records));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowExceptionWhenSinglePutDirectlyCalled() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.setReportingStoreListener(null);

        compositeRestoreListener.restore(key, value);
    }

    private void assertStateRestoreListener(MockStateRestoreListener restoreListener) {
        assertTrue(restoreListener.storeNameCalledStates.containsKey(RESTORE_START));
        assertTrue(restoreListener.storeNameCalledStates.containsKey(RESTORE_BATCH));
        assertTrue(restoreListener.storeNameCalledStates.containsKey(RESTORE_END));
        assertThat(restoreListener.restoreStartOffset, is(startOffset));
        assertThat(restoreListener.restoreEndOffset, is(endOffset));
        assertThat(restoreListener.restoredBatchOffset, is(batchOffset));
        assertThat(restoreListener.numBatchRestored, is(numberRestored));
        assertThat(restoreListener.totalNumRestored, is(numberRestored));
    }


    private static class MockStateRestoreCallback extends MockStateRestoreListener implements StateRestoreCallback {

        byte[] restoredKey;
        byte[] restoredValue;

        @Override
        public void restore(byte[] key, byte[] value) {
            restoredKey = key;
            restoredValue = value;
        }
    }

    private static class MockBatchingStateRestoreCallback extends MockStateRestoreListener
        implements BatchingStateRestoreCallback {

        Collection<KeyValue<byte[], byte[]>> restoredRecords;

        @Override
        public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
            restoredRecords = records;
        }

        @Override
        public void restore(byte[] key, byte[] value) {
            throw new IllegalStateException("Should not be called");

        }
    }

    private static class MockNoListenBatchingStateRestoreCallback implements BatchingStateRestoreCallback {

        Collection<KeyValue<byte[], byte[]>> restoredRecords;

        @Override
        public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
            restoredRecords = records;
        }

        @Override
        public void restore(byte[] key, byte[] value) {
            throw new IllegalStateException("Should not be called");

        }
    }

}