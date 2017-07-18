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
import org.apache.kafka.test.MockRestoreCallback;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class InternalStateRestoreAdapterTest {

    private final MockRestoreCallback mockRestoreCallback = new MockRestoreCallback();
    private final MockBatchingRestoreCallback mockBatchingRestoreCallback = new MockBatchingRestoreCallback();
    private final byte[] key = "key".getBytes(Charset.forName("UTF-8"));
    private final byte[] value = "value".getBytes(Charset.forName("UTF-8"));
    private final Collection<KeyValue<byte[], byte[]>> records = Collections.singletonList(KeyValue.pair(key, value));
    private InternalStateRestoreAdapter internalStateRestoreAdapter;


    @Test
    public void shouldCallRestoreForCollection() {
        internalStateRestoreAdapter = new InternalStateRestoreAdapter(mockRestoreCallback);
        internalStateRestoreAdapter.restoreAll(records);

        assertThat(mockRestoreCallback.restored.size(), is(1));
        assertThat(mockRestoreCallback.restored.get(0).key, is(key));
        assertThat(mockRestoreCallback.restored.get(0).value, is(value));
    }

    @Test
    public void shouldCallRestoreAllForCollection() {
        internalStateRestoreAdapter = new InternalStateRestoreAdapter(mockBatchingRestoreCallback);
        internalStateRestoreAdapter.restoreAll(records);

        assertThat(mockBatchingRestoreCallback.singleRestoreRecords.size(), is(0));
        assertThat(mockBatchingRestoreCallback.batchRestoredRecords.size(), is(1));
        assertThat(mockBatchingRestoreCallback.batchRestoredRecords.get(0).key, is(key));
        assertThat(mockBatchingRestoreCallback.batchRestoredRecords.get(0).value, is(value));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateRestoreCallbacks() {
        new InternalStateRestoreAdapter(null);
    }


    private static final class MockBatchingRestoreCallback implements BatchingStateRestoreCallback {

        List<KeyValue<byte[], byte[]>> batchRestoredRecords = new ArrayList<>();
        List<KeyValue<byte[], byte[]>> singleRestoreRecords = new ArrayList<>();

        @Override
        public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
            batchRestoredRecords.addAll(records);
        }

        @Override
        public void restore(byte[] key, byte[] value) {
            singleRestoreRecords.add(KeyValue.pair(key, value));
        }
    }

}