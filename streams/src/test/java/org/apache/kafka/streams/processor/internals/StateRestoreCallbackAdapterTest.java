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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

public class StateRestoreCallbackAdapterTest {
    @Test
    public void shouldThrowOnRestoreAll() {
        assertThrows(UnsupportedOperationException.class, () -> adapt(mock(StateRestoreCallback.class)).restoreAll(null));
    }

    @Test
    public void shouldThrowOnRestore() {
        assertThrows(UnsupportedOperationException.class, () -> adapt(mock(StateRestoreCallback.class)).restore(null, null));
    }

    @Test
    public void shouldPassRecordsThrough() {
        final ArrayList<ConsumerRecord<byte[], byte[]>> actual = new ArrayList<>();
        final RecordBatchingStateRestoreCallback callback = actual::addAll;

        final RecordBatchingStateRestoreCallback adapted = adapt(callback);

        final byte[] key1 = {1};
        final byte[] value1 = {2};
        final byte[] key2 = {3};
        final byte[] value2 = {4};

        final List<ConsumerRecord<byte[], byte[]>> recordList = asList(
            new ConsumerRecord<>("topic1", 0, 0L, key1, value1),
            new ConsumerRecord<>("topic2", 1, 1L, key2, value2)
        );

        adapted.restoreBatch(recordList);

        validate(actual, recordList);
    }

    @Test
    public void shouldConvertToKeyValueBatches() {
        final ArrayList<KeyValue<byte[], byte[]>> actual = new ArrayList<>();
        final BatchingStateRestoreCallback callback = new BatchingStateRestoreCallback() {
            @Override
            public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
                actual.addAll(records);
            }

            @Override
            public void restore(final byte[] key, final byte[] value) {
                // unreachable
            }
        };

        final RecordBatchingStateRestoreCallback adapted = adapt(callback);

        final byte[] key1 = {1};
        final byte[] value1 = {2};
        final byte[] key2 = {3};
        final byte[] value2 = {4};
        adapted.restoreBatch(asList(
            new ConsumerRecord<>("topic1", 0, 0L, key1, value1),
            new ConsumerRecord<>("topic2", 1, 1L, key2, value2)
        ));

        assertThat(
            actual,
            is(asList(
                new KeyValue<>(key1, value1),
                new KeyValue<>(key2, value2)
            ))
        );
    }

    @Test
    public void shouldConvertToKeyValue() {
        final ArrayList<KeyValue<byte[], byte[]>> actual = new ArrayList<>();
        final StateRestoreCallback callback = (key, value) -> actual.add(new KeyValue<>(key, value));

        final RecordBatchingStateRestoreCallback adapted = adapt(callback);

        final byte[] key1 = {1};
        final byte[] value1 = {2};
        final byte[] key2 = {3};
        final byte[] value2 = {4};
        adapted.restoreBatch(asList(
            new ConsumerRecord<>("topic1", 0, 0L, key1, value1),
            new ConsumerRecord<>("topic2", 1, 1L, key2, value2)
        ));

        assertThat(
            actual,
            is(asList(
                new KeyValue<>(key1, value1),
                new KeyValue<>(key2, value2)
            ))
        );
    }

    private void validate(final List<ConsumerRecord<byte[], byte[]>> actual,
                          final List<ConsumerRecord<byte[], byte[]>> expected) {
        assertThat(actual.size(), is(expected.size()));
        for (int i = 0; i < actual.size(); i++) {
            final ConsumerRecord<byte[], byte[]> actual1 = actual.get(i);
            final ConsumerRecord<byte[], byte[]> expected1 = expected.get(i);
            assertThat(actual1.topic(), is(expected1.topic()));
            assertThat(actual1.partition(), is(expected1.partition()));
            assertThat(actual1.offset(), is(expected1.offset()));
            assertThat(actual1.key(), is(expected1.key()));
            assertThat(actual1.value(), is(expected1.value()));
            assertThat(actual1.timestamp(), is(expected1.timestamp()));
            assertThat(actual1.headers(), is(expected1.headers()));
        }
    }


}