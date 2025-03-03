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
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
public class MeteredTimestampedWindowStoreTest {

    private static final String STORE_NAME = "mocked-store";
    private static final String STORE_TYPE = "scope";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    // timestamp is 97 what is ASCII of 'a'
    private static final long TIMESTAMP = 97L;
    private static final ValueAndTimestamp<String> VALUE_AND_TIMESTAMP =
        ValueAndTimestamp.make("value", TIMESTAMP);
    private static final byte[] VALUE_AND_TIMESTAMP_BYTES = "\0\0\0\0\0\0\0avalue".getBytes();
    private static final int WINDOW_SIZE_MS = 10;

    private InternalMockProcessorContext<String, Long> context;
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");
    @Mock
    private WindowStore<Bytes, byte[]> innerStoreMock;
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private MeteredTimestampedWindowStore<String, String> store;

    public void setUp() {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test", "processId", new MockTime());

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

        store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(new SerdeThatDoesntHandleNull())
        );
    }

    public void setUpWithoutContextName() {
        final StreamsMetricsImpl streamsMetrics =
                new StreamsMetricsImpl(metrics, "test", "processId", new MockTime());

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

        store = new MeteredTimestampedWindowStore<>(
                innerStoreMock,
                WINDOW_SIZE_MS, // any size
                STORE_TYPE,
                new MockTime(),
                Serdes.String(),
                new ValueAndTimestampSerde<>(new SerdeThatDoesntHandleNull())
        );
    }

    @Test
    public void shouldDelegateInit() {
        setUpWithoutContextName();
        @SuppressWarnings("unchecked")
        final WindowStore<Bytes, byte[]> inner = mock(WindowStore.class);
        final MeteredTimestampedWindowStore<String, String> outer = new MeteredTimestampedWindowStore<>(
            inner,
            WINDOW_SIZE_MS, // any size
            STORE_TYPE,
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(new SerdeThatDoesntHandleNull())
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

    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        @SuppressWarnings("unchecked")
        final Serde<String> keySerde = mock(Serde.class);
        @SuppressWarnings("unchecked")
        final Serializer<String> keySerializer = mock(Serializer.class);
        @SuppressWarnings("unchecked")
        final Serde<ValueAndTimestamp<String>> valueSerde = mock(Serde.class);
        @SuppressWarnings("unchecked")
        final Deserializer<ValueAndTimestamp<String>> valueDeserializer = mock(Deserializer.class);
        @SuppressWarnings("unchecked")
        final Serializer<ValueAndTimestamp<String>> valueSerializer = mock(Serializer.class);
        when(keySerde.serializer()).thenReturn(keySerializer);
        when(keySerializer.serialize(topic, KEY)).thenReturn(KEY.getBytes());
        when(valueSerde.deserializer()).thenReturn(valueDeserializer);
        when(valueDeserializer.deserialize(topic, VALUE_AND_TIMESTAMP_BYTES)).thenReturn(VALUE_AND_TIMESTAMP);
        when(valueSerde.serializer()).thenReturn(valueSerializer);
        when(valueSerializer.serialize(topic, VALUE_AND_TIMESTAMP)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);
        when(innerStoreMock.fetch(KEY_BYTES, TIMESTAMP)).thenReturn(VALUE_AND_TIMESTAMP_BYTES);
        store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            WINDOW_SIZE_MS,
            STORE_TYPE,
            new MockTime(),
            keySerde,
            valueSerde
        );

        store.init(context, store);
        store.fetch(KEY, TIMESTAMP);
        store.put(KEY, VALUE_AND_TIMESTAMP, TIMESTAMP);

        verify(innerStoreMock).fetch(KEY_BYTES, TIMESTAMP);
        verify(innerStoreMock).put(KEY_BYTES, VALUE_AND_TIMESTAMP_BYTES, TIMESTAMP);
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
        final MeteredTimestampedWindowStore<String, Long> store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            null,
            null
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000), 60000L);
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
        final MeteredTimestampedWindowStore<String, Long> store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.Long())
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000), 60000L);
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }
}
