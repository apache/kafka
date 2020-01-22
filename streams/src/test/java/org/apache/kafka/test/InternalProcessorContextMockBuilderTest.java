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
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_APPLICATION_ID;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_KEY_SERDE;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_OFFSET;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_PARTITION;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_RECORD_HEADERS;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_STATE_DIR;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_STREAMS_CONFIG;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_STREAMS_METRICS;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_TASK_ID;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_TIMESTAMP;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_TOPIC;
import static org.apache.kafka.test.InternalProcessorContextMockBuilder.DEFAULT_VALUE_SERDE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class InternalProcessorContextMockBuilderTest {

    @Test
    public void shouldReturnDefaultApplicationId() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final String applicationId = mock.applicationId();
        assertSame(DEFAULT_APPLICATION_ID, applicationId);
    }

    @Test
    public void shouldUpdateApplicationId() {
        final String applicationId = "new_application_id";
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .applicationId(applicationId)
                .build();
        assertSame(applicationId, mock.applicationId());
    }

    @Test
    public void shouldReturnDefaultTaskId() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final TaskId taskId = mock.taskId();
        assertSame(DEFAULT_TASK_ID, taskId);
    }

    @Test
    public void shouldUpdateTaskId() {
        final TaskId taskId = new TaskId(23, 3);
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .taskId(taskId)
                .build();
        assertSame(taskId, mock.taskId());
    }

    @Test
    public void shouldReturnDefaultKeySerde() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final Serde<?> keySerde = mock.keySerde();
        assertThat(keySerde, samePropertyValuesAs(DEFAULT_KEY_SERDE));
    }

    @Test
    public void shouldUpdateKeySerde() {
        final Serde<String> keySerde = Serdes.String();
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .keySerde(keySerde)
                .build();
        assertSame(keySerde, mock.keySerde());
    }

    @Test
    public void shouldReturnDefaultValueSerde() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final Serde<?> valueSerde = mock.valueSerde();
        assertThat(valueSerde, samePropertyValuesAs(DEFAULT_VALUE_SERDE));
    }

    @Test
    public void shouldUpdateValueSerde() {
        final Serde<String> valueSerde = Serdes.String();
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .valueSerde(valueSerde)
                .build();
        assertSame(valueSerde, mock.valueSerde());
    }

    @Test
    public void shouldReturnDefaultStateDir() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final File stateDir = mock.stateDir();
        assertSame(DEFAULT_STATE_DIR, stateDir);
    }

    @Test
    public void shouldUpdateStateDir() {
        final File stateDir = new File("/");
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .stateDir(stateDir)
                .build();
        assertSame(stateDir, mock.stateDir());
    }

    @Test
    public void shouldReturnDefaultMetrics() {
        assertEquals(DEFAULT_STREAMS_METRICS, new InternalProcessorContextMockBuilder().build().metrics());
    }

    @Test
    public void shouldUpdateMetrics() {
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(new Metrics(), "client_id", StreamsConfig.METRICS_LATEST);
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .metrics(metrics)
                .build();
        assertSame(metrics, mock.metrics());
    }

    @Test
    public void shouldRegisterAndReturnStore() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final String storeName = "store_name";
        final StateStore stateStore = new MockKeyValueStore(storeName, false);
        final StateRestoreCallback stateRestoreCallback = new MockRestoreCallback();
        mock.register(stateStore, stateRestoreCallback);
        final StateStore store = mock.getStateStore(storeName);
        assertSame(stateStore, store);
    }

    @Test
    public void shouldOverrideRegister() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.register(null, null);
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.register(null, null);
        EasyMock.verify(mock);
    }

    @Test
    public void shouldOverrideGetStateStore() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        final StateStore stateStore = new InMemoryKeyValueStore("");
        mock.getStateStore(EasyMock.anyString());
        EasyMock.expectLastCall().andReturn(stateStore).once();
        EasyMock.replay(mock);
        final StateStore store = mock.getStateStore("");
        assertSame(stateStore, store);
        EasyMock.verify(mock);
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
    public void shouldFailOnScheduleWithoutProcessorContext() {
        assertThrows("processorContext must be set before use schedule() via constructor.",
                IllegalStateException.class,
                () -> new InternalProcessorContextMockBuilder().build().schedule(null, null, null));
    }

    @Test
    public void shouldOverrideSchedule() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.schedule(null, null, null);
        final Cancellable expectedCancellable = () -> {
        };
        EasyMock.expectLastCall().andReturn(expectedCancellable).once();
        EasyMock.replay(mock);
        assertSame(expectedCancellable, mock.schedule(null, null, null));
        EasyMock.verify(mock);
    }

    @Test
    public void shouldForwardKeyValueToAllAndCapture() {
        final List<KeyValue<String, String>> keyValuesToForward = Arrays.asList(new KeyValue<>("key1", "value1"), new KeyValue<>("key2", "value2"));
        final ProcessorContext processorContext = createProcessorContext();
        final MockProcessor<String, String> processor = new MockProcessor<>();
        final ProcessorNode<String, String> processorNode = new ProcessorNode<>("pn-test", processor, new HashSet<>());
        processorNode.addChild(processorNode); // add itself as child

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
    public void shouldUpdateTimestampWhenForwardToDownstreamProcessorWithTimestamp() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final long timestamp = 0;
        final ProcessorNode<String, String> processorNode = new ProcessorNode<>("pn-test", new MockProcessor<>(), new HashSet<>());
        mock.setCurrentNode(processorNode);
        mock.forward("", "", To.all().withTimestamp(timestamp));
        assertEquals(timestamp, mock.timestamp());
    }

    @Test
    public void shouldOverrideForwardKeyValue() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.forward("", "");
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.forward("", "");
        EasyMock.verify(mock);
    }

    @Test
    public void shouldOverrideForwardKeyValueTo() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.forward("", "", To.all());
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.forward("", "");
        EasyMock.verify(mock);
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
    public void shouldFailOnCommitWithoutProcessorContext() {
        assertThrows("processorContext must be set before use commit() via constructor.",
                IllegalStateException.class,
                () -> new InternalProcessorContextMockBuilder().build().commit());
    }

    @Test
    public void shouldOverrideCommit() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.commit();
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.commit();
        EasyMock.verify(mock);
    }

    @Test
    public void shouldReturnDefaultTopic() {
        assertEquals(DEFAULT_TOPIC, new InternalProcessorContextMockBuilder().build().topic());
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
    public void shouldReturnDefaultPartition() {
        assertEquals(DEFAULT_PARTITION, new InternalProcessorContextMockBuilder().build().partition());
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
    public void shouldReturnDefaultOffset() {
        assertEquals(DEFAULT_OFFSET, new InternalProcessorContextMockBuilder().build().offset());
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
    public void shouldReturnDefaultHeaders() {
        assertEquals(DEFAULT_RECORD_HEADERS, new InternalProcessorContextMockBuilder().build().headers());
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
    public void shouldReturnDefaultTimestamp() {
        assertEquals(DEFAULT_TIMESTAMP, new InternalProcessorContextMockBuilder().build().timestamp());
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
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final StreamsConfig defaultAppConfig = DEFAULT_STREAMS_CONFIG;
        final Map<String, Object> combined = new HashMap<>();
        combined.putAll(defaultAppConfig.originals());
        combined.putAll(defaultAppConfig.values());
        assertEquals(combined, mock.appConfigs());
    }

    @Test
    public void shouldUpdateAppConfigs() {
        final Map<String, Object> appConfigs = new HashMap<>();
        final String key = "key";
        final String value = "value";
        appConfigs.put(key, value);
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .appConfigs(appConfigs)
                .build();
        assertEquals(value, mock.appConfigs().get(key));
    }

    @Test
    public void shouldNotFailWithNoConfigs() {
        new InternalProcessorContextMockBuilder().appConfigs(null).build();
    }

    @Test
    public void shouldReturnDefaultAppConfigsWithPrefix() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final String prefix = "";
        assertEquals(DEFAULT_STREAMS_CONFIG.originalsWithPrefix(prefix), mock.appConfigsWithPrefix(prefix));
    }

    @Test
    public void shouldSetRecordContext() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders());
        mock.setRecordContext(recordContext);
        assertSame(recordContext, mock.recordContext());
    }

    @Test
    public void shouldOverrideSetRecordContext() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.setRecordContext(null);
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.setRecordContext(null);
        EasyMock.verify(mock);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void shouldSetCurrentNode() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        final ProcessorNode newNode = new ProcessorNode("new-node");
        mock.setCurrentNode(newNode);
        assertSame(newNode, mock.currentNode());
    }

    @Test
    public void shouldOverrideSetCurrentNode() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.setCurrentNode(null);
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.setCurrentNode(null);
        EasyMock.verify(mock);
    }

    @Test
    public void shouldReturnDefaultCache() {
        assertNull(new InternalProcessorContextMockBuilder().build().getCache());
    }

    @Test
    public void shouldUpdateCache() {
        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test", "");
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 0, streamsMetrics);
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder()
                .cache(cache)
                .build();
        assertSame(cache, mock.getCache());
    }

    @Test
    public void shouldExpectInitializeCall() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        mock.initialize();
    }

    @Test
    public void shouldOverrideInitialize() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.initialize();
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.initialize();
        EasyMock.verify(mock);
    }

    @Test
    public void shouldExpectUninitializeCall() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().build();
        mock.uninitialize();
    }

    @Test
    public void shouldOverrideUninitialize() {
        final InternalProcessorContext mock = new InternalProcessorContextMockBuilder().buildWithoutReplaying();
        mock.uninitialize();
        EasyMock.expectLastCall().once();
        EasyMock.replay(mock);
        mock.uninitialize();
        EasyMock.verify(mock);
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

    private static InternalProcessorContext mock(final ProcessorContext processorContext) {
        return new InternalProcessorContextMockBuilder((MockProcessorContext) processorContext).build();
    }
}