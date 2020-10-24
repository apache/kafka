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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MeteredSessionStoreTest {

    @Rule
    public EasyMockRule rule = new EasyMockRule(this);

    private static final String APPLICATION_ID = "test-app";
    private static final String STORE_TYPE = "scope";
    private static final String STORE_NAME = "mocked-store";
    private static final String STORE_LEVEL_GROUP_FROM_0100_TO_24 = "stream-" + STORE_TYPE + "-state-metrics";
    private static final String STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String THREAD_ID_TAG_KEY_FROM_0100_TO_24 = "client-id";
    private static final String THREAD_ID_TAG_KEY = "thread-id";
    private static final String CHANGELOG_TOPIC = "changelog-topic";
    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final Windowed<String> WINDOWED_KEY = new Windowed<>(KEY, new SessionWindow(0, 0));
    private static final Windowed<Bytes> WINDOWED_KEY_BYTES = new Windowed<>(KEY_BYTES, new SessionWindow(0, 0));
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    private static final long START_TIMESTAMP = 24L;
    private static final long END_TIMESTAMP = 42L;

    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0);
    private final Metrics metrics = new Metrics();
    private MeteredSessionStore<String, String> store;
    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> innerStore;
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

    private String storeLevelGroup;
    private String threadIdTagKey;
    private Map<String, String> tags;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {StreamsConfig.METRICS_LATEST},
            {StreamsConfig.METRICS_0100_TO_24}
        });
    }

    @Parameter
    public String builtInMetricsVersion;

    @Before
    public void before() {
        final Time mockTime = new MockTime();
        store = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            mockTime
        );
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        expect(context.applicationId()).andStubReturn(APPLICATION_ID);
        expect(context.metrics())
            .andStubReturn(new StreamsMetricsImpl(metrics, "test", builtInMetricsVersion, mockTime));
        expect(context.taskId()).andStubReturn(taskId);
        expect(context.changelogFor(STORE_NAME)).andStubReturn(CHANGELOG_TOPIC);
        expect(innerStore.name()).andStubReturn(STORE_NAME);
        storeLevelGroup =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? STORE_LEVEL_GROUP_FROM_0100_TO_24 : STORE_LEVEL_GROUP;
        threadIdTagKey =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? THREAD_ID_TAG_KEY_FROM_0100_TO_24 : THREAD_ID_TAG_KEY;
        tags = mkMap(
            mkEntry(threadIdTagKey, threadId),
            mkEntry("task-id", taskId.toString()),
            mkEntry(STORE_TYPE + "-state-id", STORE_NAME)
        );
    }

    private void init() {
        replay(innerStore, context);
        store.init((StateStoreContext) context, store);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        final SessionStore<Bytes, byte[]> inner = mock(SessionStore.class);
        final MeteredSessionStore<String, String> outer = new MeteredSessionStore<>(
            inner,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
        expect(inner.name()).andStubReturn("store");
        inner.init((ProcessorContext) context, outer);
        expectLastCall();
        replay(inner, context);
        outer.init((ProcessorContext) context, outer);
        verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        final SessionStore<Bytes, byte[]> inner = mock(SessionStore.class);
        final MeteredSessionStore<String, String> outer = new MeteredSessionStore<>(
            inner,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime()
        );
        expect(inner.name()).andStubReturn("store");
        inner.init((StateStoreContext) context, outer);
        expectLastCall();
        replay(inner, context);
        outer.init((StateStoreContext) context, outer);
        verify(inner);
    }

    @Test
    public void shouldPassChangelogTopicNameToStateStoreSerde() {
        doShouldPassChangelogTopicNameToStateStoreSerde(CHANGELOG_TOPIC);
    }

    @Test
    public void shouldPassDefaultChangelogTopicNameToStateStoreSerdeIfLoggingDisabled() {
        final String defaultChangelogTopicName =
            ProcessorStateManager.storeChangelogTopic(APPLICATION_ID, STORE_NAME);
        expect(context.changelogFor(STORE_NAME)).andReturn(null);
        doShouldPassChangelogTopicNameToStateStoreSerde(defaultChangelogTopicName);
    }

    private void doShouldPassChangelogTopicNameToStateStoreSerde(final String topic) {
        final Serde<String> keySerde = niceMock(Serde.class);
        final Serializer<String> keySerializer = mock(Serializer.class);
        final Serde<String> valueSerde = niceMock(Serde.class);
        final Deserializer<String> valueDeserializer = mock(Deserializer.class);
        final Serializer<String> valueSerializer = mock(Serializer.class);
        expect(keySerde.serializer()).andStubReturn(keySerializer);
        expect(keySerializer.serialize(topic, KEY)).andStubReturn(KEY.getBytes());
        expect(valueSerde.deserializer()).andStubReturn(valueDeserializer);
        expect(valueDeserializer.deserialize(topic, VALUE_BYTES)).andStubReturn(VALUE);
        expect(valueSerde.serializer()).andStubReturn(valueSerializer);
        expect(valueSerializer.serialize(topic, VALUE)).andStubReturn(VALUE_BYTES);
        expect(innerStore.fetchSession(KEY_BYTES, START_TIMESTAMP, END_TIMESTAMP)).andStubReturn(VALUE_BYTES);
        replay(innerStore, context, keySerializer, keySerde, valueDeserializer, valueSerializer, valueSerde);
        store = new MeteredSessionStore<>(
            innerStore,
            STORE_TYPE,
            keySerde,
            valueSerde,
            new MockTime()
        );
        store.init((StateStoreContext) context, store);

        store.fetchSession(KEY, START_TIMESTAMP, END_TIMESTAMP);
        store.put(WINDOWED_KEY, VALUE);

        verify(keySerializer, valueDeserializer, valueSerializer);
    }

    @Test
    public void testMetrics() {
        init();
        final JmxReporter reporter = new JmxReporter();
        final MetricsContext metricsContext = new KafkaMetricsContext("kafka.streams");
        reporter.contextChange(metricsContext);

        metrics.addReporter(reporter);
        assertTrue(reporter.containsMbean(String.format(
            "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
            storeLevelGroup,
            threadIdTagKey,
            threadId,
            taskId.toString(),
            STORE_TYPE,
            STORE_NAME
        )));
        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertTrue(reporter.containsMbean(String.format(
                "kafka.streams:type=%s,%s=%s,task-id=%s,%s-state-id=%s",
                storeLevelGroup,
                threadIdTagKey,
                threadId,
                taskId.toString(),
                STORE_TYPE,
                ROLLUP_VALUE
            )));
        }
    }

    @Test
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        innerStore.put(eq(WINDOWED_KEY_BYTES), aryEq(VALUE_BYTES));
        expectLastCall();
        init();

        store.put(WINDOWED_KEY, VALUE);

        final KafkaMetric metric = metric("put-rate");
        assertTrue(((Double) metric.metricValue()) > 0);
        verify(innerStore);
    }

    @Test
    public void shouldFindSessionsFromStoreAndRecordFetchMetric() {
        expect(innerStore.findSessions(KEY_BYTES, 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.findSessions(KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldBackwardFindSessionsFromStoreAndRecordFetchMetric() {
        expect(innerStore.backwardFindSessions(KEY_BYTES, 0, 0))
            .andReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFindSessions(KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldFindSessionRangeFromStoreAndRecordFetchMetric() {
        expect(innerStore.findSessions(KEY_BYTES, KEY_BYTES, 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.findSessions(KEY, KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldBackwardFindSessionRangeFromStoreAndRecordFetchMetric() {
        expect(innerStore.backwardFindSessions(KEY_BYTES, KEY_BYTES, 0, 0))
            .andReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFindSessions(KEY, KEY, 0, 0);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldRemoveFromStoreAndRecordRemoveMetric() {
        innerStore.remove(WINDOWED_KEY_BYTES);
        expectLastCall();

        init();

        store.remove(new Windowed<>(KEY, new SessionWindow(0, 0)));

        final KafkaMetric metric = metric("remove-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldFetchForKeyAndRecordFetchMetric() {
        expect(innerStore.fetch(KEY_BYTES))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.fetch(KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldBackwardFetchForKeyAndRecordFetchMetric() {
        expect(innerStore.backwardFetch(KEY_BYTES))
            .andReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFetch(KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldFetchRangeFromStoreAndRecordFetchMetric() {
        expect(innerStore.fetch(KEY_BYTES, KEY_BYTES))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()));
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.fetch(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldBackwardFetchRangeFromStoreAndRecordFetchMetric() {
        expect(innerStore.backwardFetch(KEY_BYTES, KEY_BYTES))
            .andReturn(
                new KeyValueIteratorStub<>(
                    Collections.singleton(KeyValue.pair(WINDOWED_KEY_BYTES, VALUE_BYTES)).iterator()
                )
            );
        init();

        final KeyValueIterator<Windowed<String>, String> iterator = store.backwardFetch(KEY, KEY);
        assertThat(iterator.next().value, equalTo(VALUE));
        assertFalse(iterator.hasNext());
        iterator.close();

        final KafkaMetric metric = metric("fetch-rate");
        assertTrue((Double) metric.metricValue() > 0);
        verify(innerStore);
    }

    @Test
    public void shouldRecordRestoreTimeOnInit() {
        init();
        final KafkaMetric metric = metric("restore-rate");
        assertTrue((Double) metric.metricValue() > 0);
    }

    @Test
    public void shouldNotThrowNullPointerExceptionIfFetchSessionReturnsNull() {
        expect(innerStore.fetchSession(Bytes.wrap("a".getBytes()), 0, Long.MAX_VALUE)).andReturn(null);

        init();
        assertNull(store.fetchSession("a", 0, Long.MAX_VALUE));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        store.put(null, "a");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnRemoveIfKeyIsNull() {
        store.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFetchIfKeyIsNull() {
        store.fetch(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFetchRangeIfFromIsNull() {
        store.fetch(null, "to");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFetchRangeIfToIsNull() {
        store.fetch("from", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFindSessionsIfKeyIsNull() {
        store.findSessions(null, 0, 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFindSessionsRangeIfFromIsNull() {
        store.findSessions(null, "a", 0, 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFindSessionsRangeIfToIsNull() {
        store.findSessions("a", null, 0, 0);
    }

    private interface CachedSessionStore extends SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        final CachedSessionStore cachedSessionStore = mock(CachedSessionStore.class);

        expect(cachedSessionStore.setFlushListener(anyObject(CacheFlushListener.class), eq(false))).andReturn(true);
        replay(cachedSessionStore);

        store = new MeteredSessionStore<>(
            cachedSessionStore,
            STORE_TYPE,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
        assertTrue(store.setFlushListener(null, false));

        verify(cachedSessionStore);
    }

    @Test
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        assertFalse(store.setFlushListener(null, false));
    }

    @Test
    public void shouldRemoveMetricsOnClose() {
        innerStore.close();
        expectLastCall();
        init(); // replays "inner"

        // There's always a "count" metric registered
        assertThat(storeMetrics(), not(empty()));
        store.close();
        assertThat(storeMetrics(), empty());
        verify(innerStore);
    }

    @Test
    public void shouldRemoveMetricsEvenIfWrappedStoreThrowsOnClose() {
        innerStore.close();
        expectLastCall().andThrow(new RuntimeException("Oops!"));
        init(); // replays "inner"

        assertThat(storeMetrics(), not(empty()));
        assertThrows(RuntimeException.class, store::close);
        assertThat(storeMetrics(), empty());
        verify(innerStore);
    }

    private KafkaMetric metric(final String name) {
        return this.metrics.metric(new MetricName(name, storeLevelGroup, "", this.tags));
    }

    private List<MetricName> storeMetrics() {
        return metrics.metrics()
                      .keySet()
                      .stream()
                      .filter(name -> name.group().equals(storeLevelGroup) && name.tags().equals(tags))
                      .collect(Collectors.toList());
    }
}
