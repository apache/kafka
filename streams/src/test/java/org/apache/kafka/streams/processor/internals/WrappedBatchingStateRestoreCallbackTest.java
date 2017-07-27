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
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class WrappedBatchingStateRestoreCallbackTest {

    private final MockRestoreCallback mockRestoreCallback = new MockRestoreCallback();
    private final byte[] key = "key".getBytes(Charset.forName("UTF-8"));
    private final byte[] value = "value".getBytes(Charset.forName("UTF-8"));
    private final Collection<KeyValue<byte[], byte[]>> records = Collections.singletonList(KeyValue.pair(key, value));
    private final BatchingStateRestoreCallback wrappedBatchingStateRestoreCallback = new WrappedBatchingStateRestoreCallback(mockRestoreCallback);

    @Test
    public void shouldRestoreSinglePutsFromArray() {
        wrappedBatchingStateRestoreCallback.restoreAll(records);
        assertThat(mockRestoreCallback.restored, is(records));
        KeyValue<byte[], byte[]> record = mockRestoreCallback.restored.get(0);
        assertThat(record.key, is(key));
        assertThat(record.value, is(value));
    }


}