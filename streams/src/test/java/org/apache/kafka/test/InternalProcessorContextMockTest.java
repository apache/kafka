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
package org.apache.kafka.test;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class InternalProcessorContextMockTest {

    @Test
    public void shouldReturnDefaultApplicationId() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final String applicationId = mock.applicationId();

        assertEquals(processorContext.applicationId(), applicationId);
    }

    @Test
    public void shouldUpdateApplicationId() {
        final InternalProcessorContextMock.Builder mockBuilder = builder();

        final String applicationId = "new_application_id";
        mockBuilder.applicationId(applicationId);
        final InternalProcessorContext mock = mockBuilder.build();

        assertSame(applicationId, mock.applicationId());
    }

    @Test
    public void shouldReturnDefaultTaskId() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final TaskId taskId = mock.taskId();

        assertEquals(processorContext.taskId(), taskId);
    }

    @Test
    public void shouldUpdateTaskId() {
        final InternalProcessorContextMock.Builder mockBuilder = builder();

        final TaskId taskId = new TaskId(23, 3);
        mockBuilder.taskId(taskId);
        final InternalProcessorContext mock = mockBuilder.build();

        assertSame(taskId, mock.taskId());
    }

    @Test
    public void shouldReturnDefaultKeySerde() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final Serde<?> keySerde = mock.keySerde();

        assertThat(keySerde, samePropertyValuesAs(processorContext.keySerde()));
    }

    @Test
    public void shouldUpdateKeySerde() {
        final InternalProcessorContextMock.Builder mockBuilder = builder();

        final Serde<String> keySerde = Serdes.String();
        mockBuilder.keySerde(keySerde);
        final InternalProcessorContext mock = mockBuilder.build();

        assertSame(keySerde, mock.keySerde());
    }

    @Test
    public void shouldReturnDefaultValueSerde() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final Serde<?> valueSerde = mock.valueSerde();

        assertThat(valueSerde, samePropertyValuesAs(processorContext.valueSerde()));
    }

    @Test
    public void shouldUpdateValueSerde() {
        final InternalProcessorContextMock.Builder mockBuilder = builder();

        final Serde<String> valueSerde = Serdes.String();
        mockBuilder.valueSerde(valueSerde);
        final InternalProcessorContext mock = mockBuilder.build();

        assertSame(valueSerde, mock.valueSerde());
    }

    @Test
    public void shouldReturnDefaultStateDir() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final File stateDir = mock.stateDir();

        assertSame(processorContext.stateDir(), stateDir);
    }

    @Test
    public void shouldUpdateStateDir() {
        final InternalProcessorContextMock.Builder mockBuilder = builder();

        final File stateDir = new File("/");
        mockBuilder.stateDir(stateDir);
        final InternalProcessorContext mock = mockBuilder.build();

        assertSame(stateDir, mock.stateDir());
    }

    @Test
    public void shouldReturnDefaultMetrics() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final StreamsMetricsImpl metrics = mock.metrics();

        assertSame(processorContext.metrics(), metrics);
    }

    @Test
    public void shouldUpdateMetrics() {
        final InternalProcessorContextMock.Builder mockBuilder = builder();

        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(
                new Metrics(), "client_id", StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG
        );
        mockBuilder.metrics(metrics);
        final InternalProcessorContext mock = mockBuilder.build();

        assertSame(metrics, mock.metrics());
    }

    @Test
    public void shouldRegisterAndReturnStore() {
        final InternalProcessorContext mock = mock();
        final String storeName = "store_name";
        final StateStore stateStore = new MockKeyValueStore(storeName, false);
        final StateRestoreCallback stateRestoreCallback = new MockRestoreCallback();

        mock.register(stateStore, stateRestoreCallback);
        final StateStore store = mock.getStateStore(storeName);

        assertSame(stateStore, store);
    }

    @Test
    public void shouldCapturePunctuatorsOnSchedule() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);
        final Duration interval = Duration.ofMillis(1);
        final PunctuationType type = PunctuationType.WALL_CLOCK_TIME;
        final Punctuator punctuator = timestamp -> {
        };

        final int size = 2;
        for (int i = 0; i < size; i++) {
            final Cancellable cancellable = mock.schedule(interval, type, punctuator);
            assertNotNull(cancellable);
        }

        final List<MockProcessorContext.CapturedPunctuator> punctuatorList = punctuatorList(processorContext);
        assertEquals(size, punctuatorList.size());

        final long millis = interval.toMillis();
        for (MockProcessorContext.CapturedPunctuator capturedPunctuator : punctuatorList) {
            assertEquals(millis, capturedPunctuator.getIntervalMs());
            assertEquals(type, capturedPunctuator.getType());
            assertEquals(punctuator, capturedPunctuator.getPunctuator());
        }
    }

    @Test
    public <K, V> void shouldForwardKeyValueAndCapture() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);
        @SuppressWarnings("unchecked") final KeyValue<K, V>[] expected = new KeyValue[]{
                new KeyValue<>("key1", "value1"),
                new KeyValue<>("key2", "value2"),
        };

        for (final KeyValue<K, V> kv : expected) {
            mock.forward(kv.key, kv.value);
        }
        equals(expected, capturedForwards(processorContext));
    }

    @Test
    public void shouldForwardKeyValueToAll() {
        final ProcessorContext processorContext = EasyMock.niceMock(ProcessorContext.class);
        final Capture<To> toCapture = Capture.newInstance();
        processorContext.forward(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.capture(toCapture));
        EasyMock.expectLastCall().once();
        EasyMock.replay(processorContext);

        final InternalProcessorContext mock = mock(processorContext);
        mock.forward("", "");

        EasyMock.verify(processorContext);
        assertEquals(To.all(), toCapture.getValue());
    }

    @Test
    public void shouldForwardKeyValueAndUpdateToInternal() {
        // TODO(pierDipi) Add test
    }

    @Test
    public void shouldBeCommitted() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        mock.commit();

        assertTrue(committed(processorContext));
    }

    @Test
    public void shouldNotBeCommitted() {
        final ProcessorContext processorContext = createProcessorContext();

        assertFalse(committed(processorContext));
    }

    @Test
    public void shouldReturnEmptyTopicByDefault() {
        final InternalProcessorContext mock = mock();

        assertEquals("", mock.topic());
    }

    @Test
    public void shouldUpdateTopic() {
        final InternalProcessorContext mock = mock();

        final String topic = "my-topic";
        mock.setRecordContext(new ProcessorRecordContext(0, 0, 0, topic, new RecordHeaders()));

        assertEquals(topic, mock.topic());
    }

    @Test
    public void shouldReturnPartition0ByDefault() {
        final InternalProcessorContext mock = mock();

        assertEquals(0, mock.partition());
    }

    @Test
    public void shouldUpdatePartition() {
        final InternalProcessorContext mock = mock();

        final int partition = 1;
        mock.setRecordContext(new ProcessorRecordContext(0, 0, partition, "", new RecordHeaders()));

        assertEquals(partition, mock.partition());
    }

    @Test
    public void shouldReturnOffset0ByDefault() {
        final InternalProcessorContext mock = mock();

        assertEquals(0, mock.offset());
    }

    @Test
    public void shouldUpdateOffset() {
        final InternalProcessorContext mock = mock();

        final int offset = 1;
        mock.setRecordContext(new ProcessorRecordContext(0, offset, 0, "", new RecordHeaders()));

        assertEquals(offset, mock.offset());
    }

    @Test
    public void shouldReturnEmptyHeadersByDefault() {
        final InternalProcessorContext mock = mock();

        assertEquals(new RecordHeaders(), mock.headers());
    }

    @Test
    public void shouldUpdateHeaders() {
        final InternalProcessorContext mock = mock();

        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("header-key", "header-value".getBytes()));
        mock.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", headers));

        assertEquals(headers, mock.headers());
    }

    @Test
    public void shouldReturnTimestamp0ByDefault() {
        final InternalProcessorContext mock = mock();

        assertEquals(0, mock.timestamp());
    }

    @Test
    public void shouldUpdateTimestamp() {
        final InternalProcessorContext mock = mock();

        final long timestamp = 1;
        mock.setRecordContext(new ProcessorRecordContext(timestamp, 0, 0, "", new RecordHeaders()));

        assertEquals(timestamp, mock.timestamp());
    }

    @Test
    public void shouldReturnDefaultAppConfigs() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        assertEquals(processorContext.appConfigs(), mock.appConfigs());
    }

    @Test
    public void shouldUpdateAppConfigs() {
        final Map<String, Object> appConfigs = new HashMap<>();
        final String key = "key";
        final String value = "value";
        appConfigs.put(key, value);
        final InternalProcessorContext mock = builder()
                .appConfigs(appConfigs)
                .build();

        assertEquals(value, mock.appConfigs().get(key));
    }

    @Test
    public void shouldReturnDefaultAppConfigsWithPrefix() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final String prefix = "";

        assertEquals(processorContext.appConfigsWithPrefix(prefix), mock.appConfigsWithPrefix(prefix));
    }

    @Test
    public void shouldReturnDefaultRecordContext() {
        final InternalProcessorContext mock = mock();
        final ProcessorRecordContext expected = new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders());

        assertEquals(expected, mock.recordContext());
    }

    private static <K, V> void equals(final KeyValue<K, V>[] expected, final List<CapturedForward> forwarded) {
        assertEquals(expected.length, forwarded.size());
        for (int i = 0; i < expected.length; i++) {
            final KeyValue<K, V> kv = expected[i];
            final CapturedForward forward = forwarded.get(i);

            assertEquals(kv.key, forward.keyValue().key);
            assertEquals(kv.value, forward.keyValue().value);
        }
    }

    private static ProcessorContext createProcessorContext() {
        return new MockProcessorContext();
    }

    private static List<CapturedForward> capturedForwards(final ProcessorContext processorContext) {
        return ((MockProcessorContext) processorContext).forwarded();
    }

    private static List<MockProcessorContext.CapturedPunctuator> punctuatorList(final ProcessorContext processorContext) {
        return ((MockProcessorContext) processorContext).scheduledPunctuators();
    }

    private static boolean committed(final ProcessorContext processorContext) {
        return ((MockProcessorContext) processorContext).committed();
    }

    private static InternalProcessorContext mock() {
        return builder().build();
    }

    private static InternalProcessorContext mock(final ProcessorContext processorContext) {
        return new InternalProcessorContextMock.Builder(processorContext).build();
    }

    private static InternalProcessorContextMock.Builder builder() {
        return new InternalProcessorContextMock.Builder();
    }
}