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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;

import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingVersionedKeyValueBytesStoreTest {

    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Serializer<ValueAndTimestamp<String>> VALUE_AND_TIMESTAMP_SERIALIZER
        = new ValueAndTimestampSerializer<>(STRING_SERIALIZER);
    private static final long HISTORY_RETENTION = 1000L;

    private final MockRecordCollector collector = new MockRecordCollector();
    private InternalMockProcessorContext<String, Long> context;
    private VersionedBytesStore inner;
    private ChangeLoggingVersionedKeyValueBytesStore store;

    @BeforeEach
    public void before() {
        inner = (VersionedBytesStore) new RocksDbVersionedKeyValueBytesStoreSupplier("bytes_store", HISTORY_RETENTION).get();
        store = new ChangeLoggingVersionedKeyValueBytesStore(inner);

        context = mockContext();
        context.setTime(0);
        store.init(context, store);
    }

    private InternalMockProcessorContext<String, Long> mockContext() {
        return new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
    }

    @AfterEach
    public void after() {
        store.close();
    }

    @Test
    public void shouldThrowIfInnerIsNotVersioned() {
        assertThrows(IllegalArgumentException.class,
            () -> new ChangeLoggingVersionedKeyValueBytesStore(new InMemoryKeyValueStore("kv")));
    }

    @Test
    public void shouldDelegateInit() {
        // recreate store with mock inner
        store.close();
        final VersionedBytesStore mockInner = mock(VersionedBytesStore.class);
        store = new ChangeLoggingVersionedKeyValueBytesStore(mockInner);

        store.init(context, store);

        verify(mockInner).init(context, store);
    }

    @Test
    public void shouldPropagateAndLogOnPut() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        final String value = "foo";
        final long timestamp = 10L;

        final long validTo = store.put(rawKey, rawBytes(value), timestamp);

        assertThat(validTo, equalTo(PUT_RETURN_CODE_VALID_TO_UNDEFINED));
        assertThat(inner.get(rawKey), equalTo(rawValueAndTimestamp(value, timestamp)));
        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(rawKey));
        assertThat(collector.collected().get(0).value(), equalTo(rawBytes(value)));
        assertThat(collector.collected().get(0).timestamp(), equalTo(timestamp));
    }

    @Test
    public void shouldPropagateAndLogOnPutNull() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        final long timestamp = 10L;
        // put initial record to inner, so that later verification confirms that store.put() has an effect
        inner.put(rawKey, rawBytes("foo"), timestamp - 1);
        assertThat(inner.get(rawKey), equalTo(rawValueAndTimestamp("foo", timestamp - 1)));

        final long validTo = store.put(rawKey, null, timestamp);

        assertThat(validTo, equalTo(PUT_RETURN_CODE_VALID_TO_UNDEFINED));
        assertThat(inner.get(rawKey), nullValue());
        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(rawKey));
        assertThat(collector.collected().get(0).value(), nullValue());
        assertThat(collector.collected().get(0).timestamp(), equalTo(timestamp));
    }

    @Test
    public void shouldPropagateAndLogOnDeleteWithTimestamp() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        final long timestamp = 10L;
        final byte[] rawValueAndTimestamp = rawValueAndTimestamp("foo", timestamp - 1);
        // put initial record to inner, so that later verification confirms that store.put() has an effect
        inner.put(rawKey, rawBytes("foo"), timestamp - 1);
        assertThat(inner.get(rawKey), equalTo(rawValueAndTimestamp));

        final byte[] result = store.delete(rawKey, timestamp);

        assertThat(result, equalTo(rawValueAndTimestamp));
        assertThat(inner.get(rawKey), nullValue());
        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(rawKey));
        assertThat(collector.collected().get(0).value(), nullValue());
        assertThat(collector.collected().get(0).timestamp(), equalTo(timestamp));
    }

    @Test
    public void shouldNotLogOnDeleteIfInnerStoreThrows() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        assertThrows(UnsupportedOperationException.class, () -> inner.delete(rawKey));

        assertThrows(UnsupportedOperationException.class, () -> store.delete(rawKey));
        assertThat(collector.collected().size(), equalTo(0));
    }

    @Test
    public void shouldNotLogOnPutAllIfInnerStoreThrows() {
        final List<KeyValue<Bytes, byte[]>> entries = Collections.singletonList(KeyValue.pair(
            Bytes.wrap(rawBytes("k")),
            rawValueAndTimestamp("v", 12L)));
        assertThrows(UnsupportedOperationException.class, () -> inner.putAll(entries));

        assertThrows(UnsupportedOperationException.class, () -> store.putAll(entries));
        assertThat(collector.collected().size(), equalTo(0));
    }

    @Test
    public void shouldNotLogOnPutIfAbsentIfInnerStoreThrows() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        final byte[] rawValue = rawBytes("v");
        assertThrows(UnsupportedOperationException.class, () -> inner.putIfAbsent(rawKey, rawValue));

        assertThrows(UnsupportedOperationException.class, () -> store.putIfAbsent(rawKey, rawValue));
        assertThat(collector.collected().size(), equalTo(0));
    }

    @Test
    public void shouldDelegateGet() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        inner.put(rawKey, rawBytes("v"), 8L);

        assertThat(store.get(rawKey), equalTo(rawValueAndTimestamp("v", 8L)));
    }

    @Test
    public void shouldDelegateGetWithTimestamp() {
        final Bytes rawKey = Bytes.wrap(rawBytes("k"));
        inner.put(rawKey, rawBytes("v"), 8L);

        assertThat(store.get(rawKey, 10L), equalTo(rawValueAndTimestamp("v", 8L)));
    }

    private static byte[] rawBytes(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static byte[] rawValueAndTimestamp(final String value, final long timestamp) {
        return VALUE_AND_TIMESTAMP_SERIALIZER.serialize(null, ValueAndTimestamp.makeAllowNullable(value, timestamp));
    }
}