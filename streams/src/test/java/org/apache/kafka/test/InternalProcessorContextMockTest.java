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
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
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
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        final String applicationId = "new_application_id";
        final InternalProcessorContext mock = builder()
                .applicationId(applicationId)
                .build();

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
        final TaskId taskId = new TaskId(23, 3);
        final InternalProcessorContext mock = builder()
                .taskId(taskId)
                .build();

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
        final Serde<String> keySerde = Serdes.String();
        final InternalProcessorContext mock = builder()
                .keySerde(keySerde)
                .build();

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
        final Serde<String> valueSerde = Serdes.String();
        final InternalProcessorContext mock = builder()
                .valueSerde(valueSerde)
                .build();

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
        final File stateDir = new File("/");
        final InternalProcessorContext mock = builder()
                .stateDir(stateDir)
                .build();

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
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(new Metrics(), "client_id", StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG);
        final InternalProcessorContext mock = builder()
                .metrics(metrics)
                .build();

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
    public void shouldForwardKeyValueToAllAndCapture() {
        final List<KeyValue<String, String>> keyValuesToForward = Arrays.asList(new KeyValue<>("key1", "value1"), new KeyValue<>("key2", "value2"));
        final ProcessorContext processorContext = createProcessorContext();
        final MockProcessor<String, String> processor = new MockProcessor<>();
        final ProcessorNode<String, String> processorNode = new ProcessorNode<>("pn-test", processor, new HashSet<>());
        processorNode.addChild(processorNode); // add itself as children

        final InternalProcessorContext mock = mock(processorContext);
        mock.setCurrentNode(processorNode);
        processorNode.init(mock);

        for (final KeyValue<String, String> kv : keyValuesToForward) {
            mock.forward(kv.key, kv.value);
        }

        final List<CapturedForward> forwarded = capturedForwards(processorContext);
        assertEquals(keyValuesToForward.size(), forwarded.size());
        final To toAll = To.all();
        for (int i = 0; i < keyValuesToForward.size(); i++) {
            final KeyValue<String, String> kv = keyValuesToForward.get(i);
            final CapturedForward forward = forwarded.get(i);
            final To to = To.child(forward.childName()).withTimestamp(forward.timestamp());

            assertEquals(kv.key, forward.keyValue().key);
            assertEquals(kv.value, forward.keyValue().value);
            assertEquals(toAll, to);
            assertEquals(kv.value, processor.lastValueAndTimestampPerKey.get(kv.key).value());
        }
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
        assertFalse(committed(createProcessorContext()));
    }

    @Test
    public void shouldReturnNullTopicByDefault() {
        assertNull(mock().topic());
    }

    @Test
    public void shouldUpdateTopic() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final String topic = "my-topic";
        mock.setRecordContext(new ProcessorRecordContext(0, 0, 0, topic, new RecordHeaders()));

        assertEquals(topic, mock.topic());
        assertEquals(topic, processorContext.topic());
    }

    @Test
    public void shouldReturnNegativePartitionByDefault() {
        assertTrue(mock().partition() < 0);
    }

    @Test
    public void shouldUpdatePartition() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final int partition = 1;
        mock.setRecordContext(new ProcessorRecordContext(0, 0, partition, "", new RecordHeaders()));

        assertEquals(partition, mock.partition());
        assertEquals(partition, processorContext.partition());
    }

    @Test
    public void shouldReturnNegativeOffsetByDefault() {
        assertTrue(mock().offset() < 0);
    }

    @Test
    public void shouldUpdateOffset() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final int offset = 1;
        mock.setRecordContext(new ProcessorRecordContext(0, offset, 0, "", new RecordHeaders()));

        assertEquals(offset, mock.offset());
        assertEquals(offset, processorContext.offset());
    }

    @Test
    public void shouldReturnEmptyHeadersByDefault() {
        assertEquals(new RecordHeaders(), mock().headers());
    }

    @Test
    public void shouldUpdateHeaders() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("header-key", "header-value".getBytes()));
        mock.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", headers));

        assertEquals(headers, mock.headers());
        assertEquals(headers, processorContext.headers());
    }

    @Test
    public void shouldReturnNegativeTimestampByDefault() {
        assertTrue(mock().timestamp() < 0);
    }

    @Test
    public void shouldUpdateTimestamp() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = mock(processorContext);

        final long timestamp = 1;
        mock.setRecordContext(new ProcessorRecordContext(timestamp, 0, 0, "", new RecordHeaders()));

        assertEquals(timestamp, mock.timestamp());
        assertEquals(timestamp, processorContext.timestamp());
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
    public void shouldSetRecordContext() {
        final InternalProcessorContext mock = mock();
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders());
        mock.setRecordContext(recordContext);

        assertSame(recordContext, mock.recordContext());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void shouldSetCurrentNode() {
        final InternalProcessorContext mock = mock();
        final ProcessorNode newNode = new ProcessorNode("new-node");
        mock.setCurrentNode(newNode);

        assertSame(newNode, mock.currentNode());
    }

    @Test
    public void shouldReturnDefaultCache() {
        assertNull(mock().getCache());
    }

    @Test
    public void shouldUpdateCache() {
        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test", "");
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 0, streamsMetrics);

        final InternalProcessorContext mock = builder()
                .cache(cache)
                .build();

        assertSame(cache, mock.getCache());
    }

    @Test
    public void shouldExpectInitializeCall() {
        final InternalProcessorContext mock = mock();
        mock.initialize();
    }

    @Test
    public void shouldExpectUninitializeCall() {
        final InternalProcessorContext mock = mock();
        mock.uninitialize();
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
        return new InternalProcessorContextMock.Builder((MockProcessorContext) processorContext).build();
    }

    private static InternalProcessorContextMock.Builder builder() {
        return new InternalProcessorContextMock.Builder();
    }
}