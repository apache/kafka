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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.test.MockBatchingStateRestoreListener;
import org.apache.kafka.test.MockStateRestoreListener;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_BATCH;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_END;
import static org.apache.kafka.test.MockStateRestoreListener.RESTORE_START;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;


public class CompositeRestoreListenerTest {

    private final MockStateRestoreCallback stateRestoreCallback = new MockStateRestoreCallback();
    private final MockBatchingStateRestoreListener batchingStateRestoreCallback = new MockBatchingStateRestoreListener();
    private final MockNoListenBatchingStateRestoreCallback
        noListenBatchingStateRestoreCallback =
        new MockNoListenBatchingStateRestoreCallback();
    private final MockStateRestoreListener reportingStoreListener = new MockStateRestoreListener();
    private final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    private final byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    private final Collection<KeyValue<byte[], byte[]>> records = Collections.singletonList(KeyValue.pair(key, value));
    private final Collection<ConsumerRecord<byte[], byte[]>> consumerRecords = Collections.singletonList(
        new ConsumerRecord<>("", 0, 0L, key, value)
    );
    private final String storeName = "test_store";
    private final long startOffset = 0L;
    private final long endOffset = 1L;
    private final long batchOffset = 1L;
    private final long numberRestored = 1L;
    private final TopicPartition topicPartition = new TopicPartition("testTopic", 1);

    private CompositeRestoreListener compositeRestoreListener;


    @Test
    public void shouldRestoreInNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.restoreBatch(consumerRecords);
        assertThat(stateRestoreCallback.restoredKey, is(key));
        assertThat(stateRestoreCallback.restoredValue, is(value));
    }

    @Test
    public void shouldRestoreInBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.restoreBatch(consumerRecords);
        assertThat(batchingStateRestoreCallback.getRestoredRecords(), is(records));
    }

    @Test
    public void shouldNotifyRestoreStartNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        assertStateRestoreListenerOnStartNotification(stateRestoreCallback);
        assertStateRestoreListenerOnStartNotification(reportingStoreListener);
    }

    @Test
    public void shouldNotifyRestoreStartBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        assertStateRestoreListenerOnStartNotification(batchingStateRestoreCallback);
        assertStateRestoreListenerOnStartNotification(reportingStoreListener);
    }

    @Test
    public void shouldNotifyRestoreProgressNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, endOffset, numberRestored);
        assertStateRestoreListenerOnBatchCompleteNotification(stateRestoreCallback);
        assertStateRestoreListenerOnBatchCompleteNotification(reportingStoreListener);
    }

    @Test
    public void shouldNotifyRestoreProgressBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, endOffset, numberRestored);
        assertStateRestoreListenerOnBatchCompleteNotification(batchingStateRestoreCallback);
        assertStateRestoreListenerOnBatchCompleteNotification(reportingStoreListener);
    }

    @Test
    public void shouldNotifyRestoreEndInNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);
        assertStateRestoreOnEndNotification(stateRestoreCallback);
        assertStateRestoreOnEndNotification(reportingStoreListener);
    }

    @Test
    public void shouldNotifyRestoreEndInBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);
        assertStateRestoreOnEndNotification(batchingStateRestoreCallback);
        assertStateRestoreOnEndNotification(reportingStoreListener);
    }

    @Test
    public void shouldHandleNullReportStoreListener() {
        compositeRestoreListener = new CompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.setUserRestoreListener(null);

        compositeRestoreListener.restoreBatch(consumerRecords);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);

        assertThat(batchingStateRestoreCallback.getRestoredRecords(), is(records));
        assertStateRestoreOnEndNotification(batchingStateRestoreCallback);
    }

    @Test
    public void shouldHandleNoRestoreListener() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.setUserRestoreListener(null);

        compositeRestoreListener.restoreBatch(consumerRecords);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);

        assertThat(noListenBatchingStateRestoreCallback.restoredRecords, is(records));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowExceptionWhenSinglePutDirectlyCalled() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.restore(key, value);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowExceptionWhenRestoreAllDirectlyCalled() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.restoreAll(Collections.emptyList());
    }

    private void assertStateRestoreListenerOnStartNotification(final MockStateRestoreListener restoreListener) {
        assertTrue(restoreListener.storeNameCalledStates.containsKey(RESTORE_START));
        assertThat(restoreListener.restoreTopicPartition, is(topicPartition));
        assertThat(restoreListener.restoreStartOffset, is(startOffset));
        assertThat(restoreListener.restoreEndOffset, is(endOffset));
    }

    private void assertStateRestoreListenerOnBatchCompleteNotification(final MockStateRestoreListener restoreListener) {
        assertTrue(restoreListener.storeNameCalledStates.containsKey(RESTORE_BATCH));
        assertThat(restoreListener.restoreTopicPartition, is(topicPartition));
        assertThat(restoreListener.restoredBatchOffset, is(batchOffset));
        assertThat(restoreListener.numBatchRestored, is(numberRestored));
    }

    private void assertStateRestoreOnEndNotification(final MockStateRestoreListener restoreListener) {
        assertTrue(restoreListener.storeNameCalledStates.containsKey(RESTORE_END));
        assertThat(restoreListener.restoreTopicPartition, is(topicPartition));
        assertThat(restoreListener.totalNumRestored, is(numberRestored));
    }


    private void setUpCompositeRestoreListener(final StateRestoreCallback stateRestoreCallback) {
        compositeRestoreListener = new CompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.setUserRestoreListener(reportingStoreListener);
    }


    private static class MockStateRestoreCallback extends MockStateRestoreListener implements StateRestoreCallback {

        byte[] restoredKey;
        byte[] restoredValue;

        @Override
        public void restore(final byte[] key, final byte[] value) {
            restoredKey = key;
            restoredValue = value;
        }
    }

    private static class MockNoListenBatchingStateRestoreCallback implements BatchingStateRestoreCallback {

        Collection<KeyValue<byte[], byte[]>> restoredRecords;

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            restoredRecords = records;
        }

        @Override
        public void restore(final byte[] key, final byte[] value) {
            throw new IllegalStateException("Should not be called");

        }
    }

}
