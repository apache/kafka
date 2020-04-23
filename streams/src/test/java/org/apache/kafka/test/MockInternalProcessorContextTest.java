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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.AbstractNotifyingRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.CompositeRestoreListener;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.test.MockInternalProcessorContext.DEFAULT_TASK_ID;

public class MockInternalProcessorContextTest {

    @Test
    public void shouldReturnDefaults() {
        final MockInternalProcessorContext context = new MockInternalProcessorContext();

        verifyDefaultMetricsVersion(context);
        verifyDefaultRecordCollector(context);
        verifyDefaultTaskId(context);
        verifyDefaultTopic(context);
        verifyDefaultPartition(context);
        verifyDefaultTimestamp(context);
        verifyDefaultOffset(context);
        verifyDefaultHeaders(context);
        verifyDefaultProcessorNodeName(context);
    }

    @Test
    public void shouldReturnMetricsVersionLatest() {
        shouldReturnMetricsVersion(Version.LATEST, StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldReturnMetricsVersionFrom0100To24() {
        shouldReturnMetricsVersion(Version.FROM_0100_TO_24, StreamsConfig.METRICS_0100_TO_24);
    }

    private static void shouldReturnMetricsVersion(final Version version, final String builtInMetricsVersion) {
        final Properties properties = StreamsTestUtils.getStreamsConfig();
        properties.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
        final MockInternalProcessorContext context = new MockInternalProcessorContext(properties, new Metrics());

        Assert.assertEquals(version, context.metrics().version());
        verifyDefaultRecordCollector(context);
        verifyDefaultTaskId(context);
        verifyDefaultTopic(context);
        verifyDefaultPartition(context);
        verifyDefaultTimestamp(context);
        verifyDefaultOffset(context);
        verifyDefaultHeaders(context);
        verifyDefaultProcessorNodeName(context);
    }

    @Test
    public void shouldHaveStateDirAtTheSpecifiedPath() {
        final String stateDir = "state-dir";
        final Properties properties = StreamsTestUtils.getStreamsConfig();
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        final InternalProcessorContext<Object, Object> context = new MockInternalProcessorContext(properties, new Metrics());

        Assert.assertEquals(new File(stateDir).getAbsolutePath(), context.stateDir().getAbsolutePath());
    }

    @Test
    public void shouldRegisterStateStore() {
        final MockInternalProcessorContext context = new MockInternalProcessorContext();

        final String storeName = "store-name";
        final StateStore stateStore = new MockKeyValueStore(storeName, false);
        context.register(stateStore, null);

        Assert.assertSame(stateStore, context.getStateStore(storeName));
    }

    @Test
    public void shouldRegisterStateRestoreListener() {
        final MockInternalProcessorContext context = new MockInternalProcessorContext();

        final String storeName = "store-name";
        final StateRestoreCallback callback = new MockStateRestoreListener();
        context.register(new MockKeyValueStore(storeName, false), callback);

        Assert.assertSame(callback, context.getRestoreListener(storeName));
    }

    @Test
    public void shouldReturnNoOpStateRestoreListener() {
        final MockInternalProcessorContext context = new MockInternalProcessorContext();

        final String storeName = "store-name";
        context.register(new MockKeyValueStore(storeName, false), new MockRestoreCallback());

        Assert.assertEquals(CompositeRestoreListener.NO_OP_STATE_RESTORE_LISTENER, context.getRestoreListener(storeName));
    }

    @Test
    public void shouldCallOnRestoreStartAndOnRestoreEndWhenRestore() {
        final String storeName = "store-name";

        final AbstractNotifyingRestoreCallback stateRestoreListener = EasyMock.mock(AbstractNotifyingRestoreCallback.class);
        final Capture<TopicPartition> topicPartitionCapture = Capture.newInstance();
        final Capture<String> storeNameCapture = Capture.newInstance();
        final Capture<Long> startingOffset = Capture.newInstance();
        final Capture<Long> endingOffset = Capture.newInstance();
        stateRestoreListener.onRestoreStart(
                EasyMock.capture(topicPartitionCapture),
                EasyMock.capture(storeNameCapture),
                EasyMock.captureLong(startingOffset),
                EasyMock.captureLong(endingOffset)
        );
        EasyMock.expectLastCall().andAnswer(() -> {
            Assert.assertNull(topicPartitionCapture.getValue());
            Assert.assertEquals(storeName, storeNameCapture.getValue());
            Assert.assertEquals(0L, startingOffset.getValue().longValue());
            Assert.assertEquals(0L, endingOffset.getValue().longValue());
            return null;
        });
        final Capture<Long> totalRestoredCapture = Capture.newInstance();
        stateRestoreListener.onRestoreEnd(
                EasyMock.capture(topicPartitionCapture),
                EasyMock.capture(storeNameCapture),
                EasyMock.captureLong(totalRestoredCapture)
        );
        EasyMock.expectLastCall().andAnswer(() -> {
            Assert.assertNull(topicPartitionCapture.getValue());
            Assert.assertEquals(storeName, storeNameCapture.getValue());
            Assert.assertEquals(0L, totalRestoredCapture.getValue().longValue());
            return null;
        });
        EasyMock.replay(stateRestoreListener);

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        context.register(new MockKeyValueStore(storeName, false), stateRestoreListener);
        context.restore(storeName, Collections.emptyList());

        EasyMock.verify(stateRestoreListener);
    }

    @Test
    public void shouldRestoreBatch() {
        final String storeName = "store-name";
        final RecordBatchingStateRestoreCallback callback = EasyMock.mock(RecordBatchingStateRestoreCallback.class);
        final List<KeyValue<byte[], byte[]>> changelog = Arrays.asList(
                new KeyValue<>("key1".getBytes(), "value1".getBytes()),
                new KeyValue<>("key2".getBytes(), "value2".getBytes())
        );
        Capture<List<ConsumerRecord<byte[], byte[]>>> recordsCapture = Capture.newInstance();
        callback.restoreBatch(EasyMock.capture(recordsCapture));
        EasyMock.expectLastCall().andAnswer(() -> {
            final List<ConsumerRecord<byte[], byte[]>> records = recordsCapture.getValue();
            Assert.assertEquals(records.size(), changelog.size());
            for (int i = 0; i < records.size(); i++) {
                final ConsumerRecord<byte[], byte[]> consumerRecord = records.get(i);
                final KeyValue<byte[], byte[]> keyValue = changelog.get(i);
                Assert.assertEquals(MockInternalProcessorContext.DEFAULT_TOPIC, consumerRecord.topic());
                Assert.assertEquals(MockInternalProcessorContext.DEFAULT_PARTITION, consumerRecord.partition());
                Assert.assertEquals(MockInternalProcessorContext.DEFAULT_OFFSET, consumerRecord.offset());
                Assert.assertEquals(keyValue.key, consumerRecord.key());
                Assert.assertEquals(keyValue.value, consumerRecord.value());
            }
            return null;
        });
        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        context.register(new MockKeyValueStore(storeName, false), callback);

        EasyMock.replay(callback);

        context.restore(storeName, changelog);

        EasyMock.verify(callback);
    }

    @Test
    public void shouldSetKeySerdeFromConfig() {
        final Properties config = StreamsTestUtils.getStreamsConfig();
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        final MockInternalProcessorContext context = new MockInternalProcessorContext(config, new Metrics());

        Assert.assertEquals(Serdes.StringSerde.class, context.keySerde().getClass());
    }

    @Test
    public void shouldSetValueSerdeFromConfig() {
        final Properties config = StreamsTestUtils.getStreamsConfig();
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        final MockInternalProcessorContext context = new MockInternalProcessorContext(config, new Metrics());

        Assert.assertEquals(Serdes.StringSerde.class, context.valueSerde().getClass());
    }

    @Test
    public void shouldSetRecordContext() {
        final InternalProcessorContext<Object, Object> context = new MockInternalProcessorContext();

        final ProcessorRecordContext processorRecordContext = new ProcessorRecordContext(
                1L,
                1L,
                1,
                "topic",
                new RecordHeaders(Arrays.asList(
                        new RecordHeader("key1", "value1".getBytes()),
                        new RecordHeader("key2", "value2".getBytes())
                ))
        );
        context.setRecordContext(processorRecordContext);

        Assert.assertEquals(processorRecordContext.timestamp(), context.timestamp());
        Assert.assertEquals(processorRecordContext.offset(), context.offset());
        Assert.assertEquals(processorRecordContext.partition(), context.partition());
        Assert.assertEquals(processorRecordContext.topic(), context.topic());
        Assert.assertEquals(processorRecordContext.headers(), context.headers());
    }

    private static void verifyDefaultRecordCollector(final MockInternalProcessorContext context) {
        Assert.assertNotNull(context.recordCollector());
    }

    private static void verifyDefaultMetricsVersion(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(Version.LATEST, context.metrics().version());
    }

    private static void verifyDefaultProcessorNodeName(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(MockInternalProcessorContext.DEFAULT_PROCESSOR_NODE_NAME, context.currentNode().name());
    }

    private static void verifyDefaultHeaders(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(MockInternalProcessorContext.DEFAULT_HEADERS, context.recordContext().headers());
    }

    private static void verifyDefaultOffset(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(MockInternalProcessorContext.DEFAULT_OFFSET, context.recordContext().offset());
    }

    private static void verifyDefaultTimestamp(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(MockInternalProcessorContext.DEFAULT_TIMESTAMP, context.recordContext().timestamp());
    }

    private static void verifyDefaultPartition(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(MockInternalProcessorContext.DEFAULT_PARTITION, context.recordContext().partition());
    }

    private static void verifyDefaultTopic(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(MockInternalProcessorContext.DEFAULT_TOPIC, context.recordContext().topic());
    }

    private static void verifyDefaultTaskId(final InternalProcessorContext<Object, Object> context) {
        Assert.assertEquals(DEFAULT_TASK_ID, context.taskId());
    }
}