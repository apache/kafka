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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockStateStore;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamTaskTest {

    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();
    private final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final TopicPartition partition1 = new TopicPartition(topic1, 1);
    private final TopicPartition partition2 = new TopicPartition(topic2, 1);
    private final Set<TopicPartition> partitions = Utils.mkSet(partition1, partition2);

    private final MockSourceNode<Integer, Integer> source1 = new MockSourceNode<>(new String[]{topic1}, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source2 = new MockSourceNode<>(new String[]{topic2}, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source3 = new MockSourceNode<Integer, Integer>(new String[]{topic2}, intDeserializer, intDeserializer) {
        @Override
        public void process(Integer key, Integer value) {
            throw new RuntimeException("KABOOM!");
        }

        @Override
        public void close() {
            throw new RuntimeException("KABOOM!");
        }
    };
    private final MockProcessorNode<Integer, Integer> processorStreamTime = new MockProcessorNode<>(10L);
    private final MockProcessorNode<Integer, Integer> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private final String storeName = "store";
    private final StateStore stateStore = new MockStateStore(storeName, false);
    private final TopicPartition changelogPartition = new TopicPartition("store-changelog", 0);
    private final Long offset = 543L;

    private final ProcessorTopology topology = ProcessorTopology.withSources(
            Utils.<ProcessorNode>mkList(source1, source2, processorStreamTime, processorSystemTime),
            new HashMap<String, SourceNode>() {
                {
                    put(topic1, source1);
                    put(topic2, source2);
                }
            }
    );

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer);
    private final MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, stateRestoreListener, new LogContext("stream-task-test ")) {
        @Override
        public Map<TopicPartition, Long> restoredOffsets() {
            return Collections.singletonMap(changelogPartition, offset);
        }
    };
    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);
    private final Metrics metrics = new Metrics();
    private final StreamsMetrics streamsMetrics = new MockStreamsMetrics(metrics);
    private final TaskId taskId00 = new TaskId(0, 0);
    private final MockTime time = new MockTime();
    private File baseDir = TestUtils.tempDirectory();
    private StateDirectory stateDirectory;
    private StreamsConfig config;
    private StreamsConfig eosConfig;
    private StreamTask task;
    private long punctuatedAt;

    private Punctuator punctuator = new Punctuator() {
        @Override
        public void punctuate(long timestamp) {
            punctuatedAt = timestamp;
        }
    };

    private StreamsConfig createConfig(final boolean enableEoS) throws IOException {
        return new StreamsConfig(new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-task-test");
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
                setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
                if (enableEoS) {
                    setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
                }
            }
        });
    }

    @Before
    public void setup() throws IOException {
        consumer.assign(Arrays.asList(partition1, partition2));
        config = createConfig(false);
        eosConfig = createConfig(true);
        stateDirectory = new StateDirectory(config, new MockTime());
    }

    @After
    public void cleanup() throws IOException {
        try {
            if (task != null) {
                try {
                    task.close(true, false);
                } catch (Exception e) {
                    // swallow
                }
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessOrder() {
        task = createStatelessTask(false);

        task.addRecords(partition1, records(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        task.addRecords(partition2, records(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 25, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);
    }


    private void testSpecificMetrics(final String operation, final String groupName, final Map<String, String> tags) {
        assertNotNull(metrics.metrics().get(metrics.metricName(operation + "-latency-avg", groupName,
                "The average latency of " + operation + " operation.", tags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(operation + "-latency-max", groupName,
                "The max latency of " + operation + " operation.", tags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(operation + "-rate", groupName,
                "The average number of occurrence of " + operation + " operation per second.", tags)));
    }


    @Test
    public void testMetrics() {
        task = createStatelessTask(false);

        final String name = task.id().toString();
        final Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("task-id", name);
        final String operation = "commit";

        final String groupName = "stream-task-metrics";

        assertNotNull(metrics.getSensor(operation));
        testSpecificMetrics(operation, groupName, metricTags);
        metricTags.put("task-id", "all");
        testSpecificMetrics(operation, groupName, metricTags);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPauseResume() {
        task = createStatelessTask(false);

        task.addRecords(partition1, records(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        task.addRecords(partition2, records(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 55, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 65, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        assertTrue(task.process());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        task.addRecords(partition1, records(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 50, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        assertEquals(2, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition1));
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(3, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertEquals(1, consumer.paused().size());
        assertTrue(consumer.paused().contains(partition2));

        assertTrue(task.process());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertEquals(0, consumer.paused().size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMaybePunctuateStreamTime() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, records(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 32, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 60, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        task.addRecords(partition2, records(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 25, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 61, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(8, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(7, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(6, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(3, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(5, source1.numReceived);
        assertEquals(3, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(5, source1.numReceived);
        assertEquals(4, source2.numReceived);

        assertFalse(task.process());
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.supplier.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 0L, 20L, 32L, 40L, 60L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPunctuateOnceStreamTimeAfterGap() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, records(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 142, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 155, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 160, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        task.addRecords(partition2, records(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 25, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 145, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 159, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 161, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        assertTrue(task.maybePunctuateStreamTime()); // punctuate at 20

        assertTrue(task.process());
        assertEquals(7, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(6, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime()); // punctuate at 142

        // only one punctuation after 100ms gap
        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime()); // punctuate at 155

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime()); // punctuate at 160, still aligned on the initial punctuation

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(3, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(4, source1.numReceived);
        assertEquals(4, source2.numReceived);

        assertFalse(task.process());
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.supplier.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCancelPunctuateStreamTime() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, records(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 20, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 30, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 40, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        task.addRecords(partition2, records(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 25, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 35, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue),
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 45, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)
        ));

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());

        processorStreamTime.supplier.scheduleCancellable.cancel();

        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.supplier.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
    }

    @Test
    public void shouldPunctuateSystemTimeWhenIntervalElapsed() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(20);
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30, now + 50);
    }

    @Test
    public void shouldNotPunctuateSystemTimeWhenIntervalNotElapsed() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
    }

    @Test
    public void shouldPunctuateOnceSystemTimeAfterGap() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        long now = time.milliseconds();
        time.sleep(100);
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(12);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(7);
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(1); // punctuate at now + 130
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(105); // punctuate at now + 235
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        time.sleep(5); // punctuate at now + 240, still aligned on the initial punctuation
        assertTrue(task.maybePunctuateSystemTime());
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 100, now + 110, now + 122, now + 130, now + 235, now + 240);
    }

    @Test
    public void testCancelPunctuateSystemTime() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.scheduleCancellable.cancel();
        time.sleep(10);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext() {
        task = createTaskThatThrowsException();
        task.initializeStateStores();
        task.initializeTopology();
        task.addRecords(partition2, Collections.singletonList(
                new ConsumerRecord<>(partition2.topic(), partition2.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));

        try {
            task.process();
            fail("Should've thrown StreamsException");
        } catch (final Exception e) {
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorStreamTime.name() + "'"));
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor '" + processorSystemTime.name() + "'"));
            assertThat(task.processorContext.currentNode(), nullValue());
        }
    }

    @Test
    public void shouldFlushRecordCollectorOnFlushState() {
        final AtomicBoolean flushed = new AtomicBoolean(false);
        final StreamsMetrics streamsMetrics = new MockStreamsMetrics(new Metrics());
        final StreamTask streamTask = new StreamTask(taskId00, partitions, topology, consumer,
            changelogReader, config, streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            RecordCollector createRecordCollector(final LogContext logContext,
                                                  final ProductionExceptionHandler exHandler) {
                return new NoOpRecordCollector() {
                    @Override
                    public void flush() {
                        flushed.set(true);
                    }
                };
            }
        };
        streamTask.flushState();
        assertTrue(flushed.get());
    }

    @Test
    public void shouldCheckpointOffsetsOnCommit() throws IOException {
        task = createStatefulTask(false, true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId00),
                                                                          ProcessorStateManager.CHECKPOINT_FILE_NAME));

        assertThat(checkpoint.read(), equalTo(Collections.singletonMap(changelogPartition, offset)));
    }

    @Test
    public void shouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        task = createStatefulTask(true, true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        final File checkpointFile = new File(stateDirectory.directoryForTask(taskId00),
                                             ProcessorStateManager.CHECKPOINT_FILE_NAME);

        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        task.processorContext.setCurrentNode(processorStreamTime);
        try {
            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
            fail("Should throw illegal state exception as current node is not null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldCallPunctuateOnPassedInProcessorNode() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccesfullPunctuate() {
        task = createStatelessTask(false);
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(((ProcessorContextImpl) task.context()).currentNode(), nullValue());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        task = createStatelessTask(false);
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                // no-op
            }
        });
    }

    @Test
    public void shouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        task = createStatelessTask(false);
        task.processorContext.setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                // no-op
            }
        });
    }

    @Test
    public void shouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology() {
        task = createTaskThatThrowsException();
        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException e) {
            task = null;
        }
        assertTrue(processorSystemTime.closed);
        assertTrue(processorStreamTime.closed);
        assertTrue(source1.closed);
    }

    @Test
    public void shouldInitAndBeginTransactionOnCreateIfEosEnabled() {
        task = createStatelessTask(true);
        task.initializeTopology();

        assertTrue(producer.transactionInitialized());
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotThrowOnCloseIfTaskWasNotInitializedWithEosEnabled() {
        task = createStatelessTask(true);

        assertTrue(!producer.transactionInFlight());
        task.close(false, false);
    }

    @Test
    public void shouldNotInitOrBeginTransactionOnCreateIfEosDisabled() {
        task = createStatelessTask(false);

        assertFalse(producer.transactionInitialized());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled() {
        task = createStatelessTask(true);
        task.initializeTopology();

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();

        task.suspend();
        assertTrue(producer.sentOffsets());
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldCommitTransactionOnSuspendEvenIfTransactionIsEmptyIfEosEnabled() {
        task = createStatelessTask(true);
        task.initializeTopology();
        task.suspend();

        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled() {
        task = createStatelessTask(false);
        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();
        task.suspend();

        assertFalse(producer.sentOffsets());
        assertFalse(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldStartNewTransactionOnResumeIfEosEnabled() {
        task = createStatelessTask(true);
        task.initializeTopology();

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();
        task.suspend();

        task.resume();
        task.initializeTopology();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnResumeIfEosDisabled() {
        task = createStatelessTask(false);

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();
        task.suspend();

        task.resume();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldStartNewTransactionOnCommitIfEosEnabled() {
        task = createStatelessTask(true);
        task.initializeTopology();

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();

        task.commit();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnCommitIfEosDisabled() {
        task = createStatelessTask(false);

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();

        task.commit();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldAbortTransactionOnDirtyClosedIfEosEnabled() {
        task = createStatelessTask(true);
        task.initializeTopology();
        task.close(false, false);
        task = null;

        assertTrue(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnZombieClosedIfEosEnabled() {
        task = createStatelessTask(true);
        task.close(false, true);
        task = null;

        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnDirtyClosedIfEosDisabled() {
        task = createStatelessTask(false);
        task.close(false, false);
        task = null;

        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldCloseProducerOnCloseWhenEosEnabled() {
        task = createStatelessTask(true);
        task.close(true, false);
        task = null;

        assertTrue(producer.closed());
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushing() {
        task = createTaskThatThrowsException();
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.commit();
            fail("should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension() {
        final StreamTask task = createTaskThatThrowsException();

        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.suspend();
            fail("should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
    }

    @Test
    public void shouldCloseStateManagerIfFailureOnTaskClose() {
        task = createStatefulTaskThatThrowsExceptionOnClose(true, false);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.close(true, false);
            fail("should have thrown an exception");
        } catch (Exception e) {
            // all good
        }

        task = null;
        assertFalse(stateStore.isOpen());
    }

    @Test
    public void shouldNotCloseTopologyProcessorNodesIfNotInitialized() {
        final StreamTask task = createTaskThatThrowsException();
        try {
            task.close(false, false);
        } catch (Exception e) {
            fail("should have not closed unitialized topology");
        }
    }

    @Test
    public void shouldBeInitializedIfChangelogPartitionsIsEmpty() {
        final StreamTask task = createStatefulTask(false, false);

        assertTrue(task.initializeStateStores());
    }

    @Test
    public void shouldNotBeInitializedIfChangelogPartitionsIsNonEmpty() {
        final StreamTask task = createStatefulTask(false, true);

        assertFalse(task.initializeStateStores());
    }

    @Test
    public void shouldReturnOffsetsForRepartitionTopicsForPurging() {
        final TopicPartition repartition = new TopicPartition("repartition", 1);
        final ProcessorTopology topology = ProcessorTopology.withRepartitionTopics(
                Utils.<ProcessorNode>mkList(source1, source2),
                new HashMap<String, SourceNode>() {
                    {
                        put(topic1, source1);
                        put(repartition.topic(), source2);
                    }
                },
                Collections.singleton(repartition.topic())
        );
        consumer.assign(Arrays.asList(partition1, repartition));

        task = new StreamTask(taskId00, Utils.mkSet(partition1, repartition), topology, consumer, changelogReader, config,
                streamsMetrics, stateDirectory, null, time, producer);
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, Collections.singletonList(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), 5L, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.addRecords(repartition, Collections.singletonList(
                new ConsumerRecord<>(repartition.topic(), repartition.partition(), 10L, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));

        assertTrue(task.process());
        assertTrue(task.process());

        task.commit();

        Map<TopicPartition, Long> map = task.purgableOffsets();

        assertThat(map, equalTo(Collections.singletonMap(repartition, 11L)));
    }

    private StreamTask createStatefulTask(final boolean eosEnabled, final boolean logged) {
        final ProcessorTopology topology = ProcessorTopology.with(
                Utils.<ProcessorNode>mkList(source1, source2),
                new HashMap<String, SourceNode>() {
                    {
                        put(topic1, source1);
                        put(topic2, source2);
                    }
                },
                Collections.singletonList(stateStore),
                logged ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.<String, String>emptyMap());

        return new StreamTask(taskId00, partitions, topology, consumer, changelogReader, eosEnabled ? eosConfig : config,
                streamsMetrics, stateDirectory, null, time, producer);
    }

    private StreamTask createStatefulTaskThatThrowsExceptionOnClose(final boolean eosEnabled, final boolean logged) {
        final ProcessorTopology topology = ProcessorTopology.with(
                Utils.<ProcessorNode>mkList(source1, source3),
                new HashMap<String, SourceNode>() {
                    {
                        put(topic1, source1);
                        put(topic2, source3);
                    }
                },
                Collections.singletonList(stateStore),
                logged ? Collections.singletonMap(storeName, changelogPartition.topic()) : Collections.<String, String>emptyMap());

        return new StreamTask(taskId00, partitions, topology, consumer, changelogReader, eosEnabled ? eosConfig : config,
                streamsMetrics, stateDirectory, null, time, producer);
    }

    private StreamTask createStatelessTask(final boolean eosEnabled) {
        final ProcessorTopology topology = ProcessorTopology.withSources(
                Utils.<ProcessorNode>mkList(source1, source2, processorStreamTime, processorSystemTime),
                new HashMap<String, SourceNode>() {
                    {
                        put(topic1, source1);
                        put(topic2, source2);
                    }
                }
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        return new StreamTask(taskId00, partitions, topology, consumer, changelogReader, eosEnabled ? eosConfig : config,
                streamsMetrics, stateDirectory, null, time, producer);
    }

    // this task will throw exception when processing (on partition2), flushing, suspending and closing
    private StreamTask createTaskThatThrowsException() {
        final ProcessorTopology topology = ProcessorTopology.withSources(
                Utils.<ProcessorNode>mkList(source1, source3, processorStreamTime, processorSystemTime),
                new HashMap<String, SourceNode>() {
                    {
                        put(topic1, source1);
                        put(topic2, source3);
                    }
                }
        );

        source1.addChild(processorStreamTime);
        source3.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source3.addChild(processorSystemTime);

        return new StreamTask(taskId00, partitions, topology, consumer, changelogReader, config,
            streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            protected void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };
    }

    private Iterable<ConsumerRecord<byte[], byte[]>> records(final ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }

}
