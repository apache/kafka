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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class MeteredTimestampedWindowStoreWithHeadersTest {
    private static final String STORE_NAME = "mocked-store";
    private static final String STORE_TYPE = "scope";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    // timestamp is 97 what is ASCII of 'a'
    private static final long TIMESTAMP = 97L;
    private static final RecordHeaders HEADERS = makeHeaders();
    private static final ValueTimestampHeaders<String> VALUE_TIMESTAMP_HEADERS =
        ValueTimestampHeaders.make("value", TIMESTAMP, HEADERS);
    private static final byte[] VALUE_TIMESTAMP_HEADERS_BYTES = serializeValueTimestampHeaders();
    private static final int WINDOW_SIZE_MS = 10;

    private InternalMockProcessorContext<String, Long> context;
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    @Mock
    private WindowStore<Bytes, byte[]> innerStoreMock;
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private MeteredTimestampedWindowStoreWithHeaders<String, String> store;

    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", new MockTime());

        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics),
            Time.SYSTEM,
            taskId
        );

        when(innerStoreMock.name()).thenReturn(STORE_NAME);

        store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(new SerdeThatDoesntHandleNull())
        );
    }

    public void setUpWithoutContextName() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", new MockTime());

        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            MockRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics),
            Time.SYSTEM,
            taskId
        );

        store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(new SerdeThatDoesntHandleNull())
        );
    }

    @Test
    public void shouldDelegateInit() {
        setUpWithoutContextName();
        @SuppressWarnings("unchecked")
        final WindowStore<Bytes, byte[]> inner = mock(WindowStore.class);
        final MeteredTimestampedWindowStoreWithHeaders<String, String> outer = new MeteredTimestampedWindowStoreWithHeaders<>(
            inner,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(new SerdeThatDoesntHandleNull())
        );
        when(inner.name()).thenReturn("store");

        outer.init(context, outer);

        verify(inner).init(context, outer);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        setUp();
        context.addChangelogForStore(STORE_NAME, CHANGELOG_TOPIC);
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        setUp();
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), STORE_NAME, taskId.topologyName());
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        setUp();
        store.init(context, store);
        store.close();

        verify(innerStoreMock).close();
    }

    @Test
    public void shouldNotExceptionIfFetchReturnsNull() {
        setUp();
        when(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).thenReturn(null);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        setUp();
        when(innerStoreMock.name()).thenReturn("mocked-store");
        final MeteredTimestampedWindowStoreWithHeaders<String, Long> store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            null,
            null
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueTimestampHeaders.make(42L, 60000, new RecordHeaders()), 60000L);
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from processor context.");
            }
            throw exception;
        }
    }

    @Test
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        setUp();
        when(innerStoreMock.name()).thenReturn("mocked-store");
        final MeteredTimestampedWindowStoreWithHeaders<String, Long> store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueTimestampHeadersSerde<>(Serdes.Long())
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueTimestampHeaders.make(42L, 60000, new RecordHeaders()), 60000L);
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }

    private static RecordHeaders makeHeaders() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        return headers;
    }

    private static byte[] serializeValueTimestampHeaders() {
        final ValueTimestampHeadersSerializer<String> serializer = new ValueTimestampHeadersSerializer<>(Serdes.String().serializer());
        return serializer.serialize("topic", VALUE_TIMESTAMP_HEADERS);
    }

    @SuppressWarnings("unchecked")
    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = mock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<ValueTimestampHeaders<String>> valueSerde = mock(Serde.class);
        final Deserializer<ValueTimestampHeaders<String>> valueDeserializer = mock(Deserializer.class);
        final Serializer<ValueTimestampHeaders<String>> valueSerializer = mock(Serializer.class);
        when(keySerde.serializer()).thenReturn(keySerializer);
        // For fetch: key serialization uses empty headers (no value context available)
        when(keySerializer.serialize(topic, new RecordHeaders(), KEY)).thenReturn(KEY.getBytes());
        // For put: key serialization uses value's headers
        when(keySerializer.serialize(topic, HEADERS, KEY)).thenReturn(KEY.getBytes());
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);
        when(valueDeserializer.deserialize(topic, new RecordHeaders(), VALUE_TIMESTAMP_HEADERS_BYTES)).thenReturn(VALUE_TIMESTAMP_HEADERS);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        // For put: value serialization uses value's headers
        when(valueSerializer.serialize(topic, HEADERS, VALUE_TIMESTAMP_HEADERS)).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        when(innerStoreMock.fetch(KEY_BYTES, TIMESTAMP)).thenReturn(VALUE_TIMESTAMP_HEADERS_BYTES);
        store = new MeteredTimestampedWindowStoreWithHeaders<>(
            innerStoreMock,
            WINDOW_SIZE_MS,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );

        store.init(context, store);
        store.fetch(KEY, TIMESTAMP);
        store.put(KEY, VALUE_TIMESTAMP_HEADERS, TIMESTAMP);

        verify(innerStoreMock).fetch(KEY_BYTES, TIMESTAMP);
        verify(innerStoreMock).put(KEY_BYTES, VALUE_TIMESTAMP_HEADERS_BYTES, TIMESTAMP);
    }
}
