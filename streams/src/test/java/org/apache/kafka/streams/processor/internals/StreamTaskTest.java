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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.NoOpProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    private final String[] topic1 = {"topic1"};
    private final String[] topic2 = {"topic2"};
    private final TopicPartition partition1 = new TopicPartition(topic1[0], 1);
    private final TopicPartition partition2 = new TopicPartition(topic2[0], 1);
    private final Set<TopicPartition> partitions = Utils.mkSet(partition1, partition2);

    private final MockSourceNode<Integer, Integer> source1 = new MockSourceNode<>(topic1, intDeserializer, intDeserializer);
    private final MockSourceNode<Integer, Integer> source2 = new MockSourceNode<>(topic2, intDeserializer, intDeserializer);
    private final MockProcessorNode<Integer, Integer> processorStreamTime = new MockProcessorNode<>(10L);
    private final MockProcessorNode<Integer, Integer> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private final ProcessorTopology topology = new ProcessorTopology(
            Arrays.<ProcessorNode>asList(source1, source2, processorStreamTime, processorSystemTime),
            new HashMap<String, SourceNode>() {
                {
                    put(topic1[0], source1);
                    put(topic2[0], source2);
                }
            },
            Collections.<String, SinkNode>emptyMap(),
            Collections.<StateStore>emptyList(),
            Collections.<String, String>emptyMap(),
            Collections.<StateStore>emptyList());
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(false, bytesSerializer, bytesSerializer);
    private final MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, stateRestoreListener, new LogContext("stream-task-test "));
    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);
    private final String applicationId = "applicationId";
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
        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);
        config = createConfig(false);
        eosConfig = createConfig(true);
        stateDirectory = new StateDirectory("applicationId", baseDir.getPath(), new MockTime());
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer,
                              changelogReader, config, streamsMetrics, stateDirectory, null, time, producer);
        task.initialize();
    }

    @After
    public void cleanup() throws IOException {
        try {
            if (task != null) {
                task.close(true, false);
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessOrder() {
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
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertFalse(task.maybePunctuateStreamTime());

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);

        assertFalse(task.process());
        assertFalse(task.maybePunctuateStreamTime());

        processorStreamTime.supplier.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 30L, 40L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCancelPunctuateStreamTime() {
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
        long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30);
    }

    @Test
    public void shouldNotPunctuateSystemTimeWhenIntervalNotElapsed() {
        long now = time.milliseconds();
        assertTrue(task.maybePunctuateSystemTime()); // first time we always punctuate
        time.sleep(9);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now);
    }

    @Test
    public void testCancelPunctuateSystemTime() {
        long now = time.milliseconds();
        time.sleep(10);
        assertTrue(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.scheduleCancellable.cancel();
        time.sleep(10);
        assertFalse(task.maybePunctuateSystemTime());
        processorSystemTime.supplier.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext() {
        final MockSourceNode processorNode = new MockSourceNode(topic1, intDeserializer, intDeserializer) {

            @Override
            public void process(final Object key, final Object value) {
                throw new KafkaException("KABOOM!");
            }
        };

        final List<ProcessorNode> processorNodes = Collections.<ProcessorNode>singletonList(processorNode);
        final Map<String, SourceNode> sourceNodes = new HashMap() {
            {
                put(topic1[0], processorNode);
                put(topic2[0], processorNode);
            }
        };
        final ProcessorTopology topology = new ProcessorTopology(processorNodes,
                                                                 sourceNodes,
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>emptyList(),
                                                                 Collections.<String, String>emptyMap(),
                                                                 Collections.<StateStore>emptyList());

        task.close(true, false);

        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader, config,
            streamsMetrics, stateDirectory, null, time, producer);
        task.initialize();
        final int offset = 20;
        task.addRecords(partition1, Collections.singletonList(
                new ConsumerRecord<>(partition1.topic(), partition1.partition(), offset, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));

        try {
            task.process();
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain topic", message.contains("topic=" + topic1[0]));
            assertTrue("message=" + message + " should contain partition", message.contains("partition=" + partition1.partition()));
            assertTrue("message=" + message + " should contain offset", message.contains("offset=" + offset));
            assertTrue("message=" + message + " should contain processor", message.contains("processor=" + processorNode.name()));
        }
    }

    @SuppressWarnings(value = {"unchecked", "deprecation"})
    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingDeprecated() {
        final Processor processor = new AbstractProcessor() {
            @Override
            public void init(final ProcessorContext context) {
                context.schedule(1);
            }

            @Override
            public void process(final Object key, final Object value) {}

            @Override
            public void punctuate(final long timestamp) {
                throw new KafkaException("KABOOM!");
            }
        };

        final ProcessorNode punctuator = new ProcessorNode("test", processor, Collections.<String>emptySet());
        punctuator.init(new NoOpProcessorContext());

        try {
            task.punctuate(punctuator, 1, PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    processor.punctuate(timestamp);
                }
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor 'test'"));
            assertThat(((ProcessorContextImpl) task.processorContext()).currentNode(), nullValue());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        final Processor processor = new AbstractProcessor() {
            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public void process(final Object key, final Object value) {}

            @Override
            public void punctuate(final long timestamp) {}
        };

        final ProcessorNode punctuator = new ProcessorNode("test", processor, Collections.<String>emptySet());
        punctuator.init(new NoOpProcessorContext());

        try {
            task.punctuate(punctuator, 1, PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor 'test'"));
            assertThat(((ProcessorContextImpl) task.processorContext()).currentNode(), nullValue());
        }
    }

    @Test
    public void shouldFlushRecordCollectorOnFlushState() {
        final AtomicBoolean flushed = new AtomicBoolean(false);
        final StreamsMetrics streamsMetrics = new MockStreamsMetrics(new Metrics());
        final StreamTask streamTask = new StreamTask(taskId00, "appId", partitions, topology, consumer,
            changelogReader, config, streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            RecordCollector createRecordCollector(final LogContext logContext) {
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

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCheckpointOffsetsOnCommit() throws IOException {
        final String storeName = "test";
        final String changelogTopic = ProcessorStateManager.storeChangelogTopic("appId", storeName);
        final InMemoryKeyValueStore inMemoryStore = new InMemoryKeyValueStore(storeName, null, null) {
            @Override
            public void init(final ProcessorContext context, final StateStore root) {
                context.register(root, false, null);
            }

            @Override
            public boolean persistent() {
                return true;
            }
        };
        Map<String, SourceNode> sourceByTopics =  new HashMap() { {
                put(partition1.topic(), source1);
                put(partition2.topic(), source2);
            }
        };
        final ProcessorTopology topology = new ProcessorTopology(Collections.<ProcessorNode>emptyList(),
                                                                 sourceByTopics,
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>singletonList(inMemoryStore),
                                                                 Collections.singletonMap(storeName, changelogTopic),
                                                                 Collections.<StateStore>emptyList());

        final TopicPartition partition = new TopicPartition(changelogTopic, 0);

        restoreStateConsumer.updatePartitions(changelogTopic,
                                              Collections.singletonList(
                                                      new PartitionInfo(changelogTopic, 0, null, new Node[0], new Node[0])));
        restoreStateConsumer.updateEndOffsets(Collections.singletonMap(partition, 0L));
        restoreStateConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));

        final long offset = 543L;
        final StreamTask streamTask = new StreamTask(taskId00, "appId", partitions, topology, consumer,
            changelogReader, config, streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            RecordCollector createRecordCollector(final LogContext logContext) {
                return new NoOpRecordCollector() {
                    @Override
                    public Map<TopicPartition, Long> offsets() {

                        return Collections.singletonMap(partition, offset);
                    }
                };
            }
        };
        streamTask.initialize();

        time.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));

        streamTask.commit();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId00),
                                                                          ProcessorStateManager.CHECKPOINT_FILE_NAME));

        assertThat(checkpoint.read(), equalTo(Collections.singletonMap(partition, offset + 1)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        final Map<String, Object> properties = config.originals();
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final StreamsConfig testConfig = new StreamsConfig(properties);

        final String storeName = "test";
        final String changelogTopic = ProcessorStateManager.storeChangelogTopic("appId", storeName);
        final InMemoryKeyValueStore inMemoryStore = new InMemoryKeyValueStore(storeName, null, null) {
            @Override
            public void init(final ProcessorContext context, final StateStore root) {
                context.register(root, false, null);
            }

            @Override
            public boolean persistent() {
                return true;
            }
        };
        Map<String, SourceNode> sourceByTopics =  new HashMap() {
            {
                put(partition1.topic(), source1);
                put(partition2.topic(), source2);
            }
        };
        final ProcessorTopology topology = new ProcessorTopology(Collections.<ProcessorNode>emptyList(),
            sourceByTopics,
            Collections.<String, SinkNode>emptyMap(),
            Collections.<StateStore>singletonList(inMemoryStore),
            Collections.singletonMap(storeName, changelogTopic),
            Collections.<StateStore>emptyList());

        final TopicPartition partition = new TopicPartition(changelogTopic, 0);

        restoreStateConsumer.updatePartitions(changelogTopic,
            Collections.singletonList(
                new PartitionInfo(changelogTopic, 0, null, new Node[0], new Node[0])));
        restoreStateConsumer.updateEndOffsets(Collections.singletonMap(partition, 0L));
        restoreStateConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));

        final long offset = 543L;
        final StreamTask streamTask = new StreamTask(taskId00, "appId", partitions, topology, consumer,
            changelogReader, testConfig, streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            RecordCollector createRecordCollector(final LogContext logContext) {
                return new NoOpRecordCollector() {
                    @Override
                    public Map<TopicPartition, Long> offsets() {

                        return Collections.singletonMap(partition, offset);
                    }
                };
            }
        };

        time.sleep(testConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));

        streamTask.commit();

        final File checkpointFile = new File(stateDirectory.directoryForTask(taskId00),
                                             ProcessorStateManager.CHECKPOINT_FILE_NAME);
        assertFalse(checkpointFile.exists());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        ((ProcessorContextImpl) task.processorContext()).setCurrentNode(processorStreamTime);
        try {
            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
            fail("Should throw illegal state exception as current node is not null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldCallPunctuateOnPassedInProcessorNode() {
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        assertThat(punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccesfullPunctuate() {
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        assertThat(((ProcessorContextImpl) task.processorContext()).currentNode(), nullValue());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                // no-op
            }
        });
    }

    @Test
    public void shouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        ((ProcessorContextImpl) task.processorContext()).setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                // no-op
            }
        });
    }

    @Test
    public void shouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology() {
        task.close(true, false);
        task = createTaskThatThrowsExceptionOnClose();
        task.initialize();
        try {
            task.close(true, false);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException e) {
            task = null;
        }
        assertTrue(processorStreamTime.closed);
        assertTrue(source1.closed);
        assertTrue(source2.closed);
    }

    @Test
    public void shouldInitAndBeginTransactionOnCreateIfEosEnabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        assertTrue(producer.transactionInitialized());
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotInitOrBeginTransactionOnCreateIfEosDisabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

        assertFalse(producer.transactionInitialized());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

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
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.suspend();
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

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
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();
        task.suspend();

        task.resume();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnResumeIfEosDisabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();
        task.suspend();

        task.resume();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldStartNewTransactionOnCommitIfEosEnabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();

        task.commit();
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotStartNewTransactionOnCommitIfEosDisabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

        task.addRecords(partition1, Collections.singletonList(
            new ConsumerRecord<>(partition1.topic(), partition1.partition(), 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, recordKey, recordValue)));
        task.process();

        task.commit();
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldAbortTransactionOnDirtyClosedIfEosEnabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.close(false, false);
        task = null;
        assertTrue(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnZombieClosedIfEosEnabled() throws Exception {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.close(false, true);
        task = null;
        assertFalse(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnDirtyClosedIfEosDisabled() {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

        task.close(false, false);
        assertFalse(producer.transactionAborted());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCloseProducerOnCloseWhenEosEnabled() {
        final MockProducer producer = new MockProducer();

        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer,
            changelogReader, eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.close(true, false);
        task = null;
        assertTrue(producer.closed());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushStateWhenCommitting() {
        final MockProducer producer = new MockProducer();
        final Consumer<byte[], byte[]> consumer = EasyMock.createStrictMock(Consumer.class);
        EasyMock.expect(consumer.committed(EasyMock.anyObject(TopicPartition.class)))
                .andStubReturn(new OffsetAndMetadata(1L));
        EasyMock.replay(consumer);
        final StreamTask task = new StreamTask(taskId00, applicationId, partitions, topology, consumer,
                              changelogReader, eosConfig, streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            protected void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };

        try {
            task.commit();
            fail("should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
        EasyMock.verify(consumer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension() {
        final MockProducer producer = new MockProducer();
        final Consumer<byte[], byte[]> consumer = EasyMock.createStrictMock(Consumer.class);
        EasyMock.expect(consumer.committed(EasyMock.anyObject(TopicPartition.class)))
                .andStubReturn(new OffsetAndMetadata(1L));
        EasyMock.replay(consumer);
        MockSourceNode sourceNode = new MockSourceNode(topic1, intDeserializer, intDeserializer) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };

        final ProcessorTopology topology = new ProcessorTopology(Collections.<ProcessorNode>singletonList(sourceNode),
                                                                 Collections.<String, SourceNode>singletonMap(topic1[0], sourceNode),
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>emptyList(),
                                                                 Collections.<String, String>emptyMap(),
                                                                 Collections.<StateStore>emptyList());
        final StreamTask task = new StreamTask(taskId00, applicationId, Utils.mkSet(partition1), topology, consumer,
                                               changelogReader, eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.initialize();
        try {
            task.suspend();
            fail("should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
        EasyMock.verify(consumer);
    }

    @Test
    public void shouldCloseStateManagerIfFailureOnTaskClose() {
        final AtomicBoolean stateManagerCloseCalled = new AtomicBoolean(false);
        final StreamTask streamTask = new StreamTask(taskId00, applicationId, partitions, topology, consumer,
                                               changelogReader, eosConfig, streamsMetrics, stateDirectory, null,
                                                     time, new MockProducer<byte[], byte[]>()) {

            @Override
            void suspend(boolean val) {
                throw new RuntimeException("KABOOM!");
            }

            @Override
            void closeStateManager(final boolean writeCheckpoint) throws ProcessorStateException {
                stateManagerCloseCalled.set(true);
            }
        };

        try {
            streamTask.close(true, false);
            fail("should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
        assertTrue(stateManagerCloseCalled.get());
    }

    @Test
    public void shouldNotCloseTopologyProcessorNodesIfNotInitialized() {
        final StreamTask task = createTaskThatThrowsExceptionOnClose();
        try {
            task.close(true, false);
        } catch (Exception e) {
            fail("should have not closed unitialized topology");
        }
    }

    @Test
    public void shouldBeInitializedIfChangelogPartitionsIsEmpty() {
        final ProcessorTopology topology = new ProcessorTopology(Collections.<ProcessorNode>singletonList(source1),
                                                                 Collections.<String, SourceNode>singletonMap(topic1[0], source1),
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>singletonList(
                                                                         new MockStateStoreSupplier.MockStateStore("store",
                                                                                                                   false)),
                                                                 Collections.<String, String>emptyMap(),
                                                                 Collections.<StateStore>emptyList());


        final StreamTask task = new StreamTask(taskId00,
                                               applicationId,
                                               Utils.mkSet(partition1),
                                               topology,
                                               consumer,
                                               changelogReader,
                                               config,
                                               streamsMetrics,
                                               stateDirectory,
                                               null,
                                               time,
                                               producer);

        assertTrue(task.initialize());
    }

    @Test
    public void shouldNotBeInitializedIfChangelogPartitionsIsNonEmpty() {
        final ProcessorTopology topology = new ProcessorTopology(Collections.<ProcessorNode>singletonList(source1),
                                                                 Collections.<String, SourceNode>singletonMap(topic1[0], source1),
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>singletonList(
                                                                         new MockStateStoreSupplier.MockStateStore("store",
                                                                                                                   false)),
                                                                 Collections.singletonMap("store", "changelog"),
                                                                 Collections.<StateStore>emptyList());


        final StreamTask task = new StreamTask(taskId00,
                                               applicationId,
                                               Utils.mkSet(partition1),
                                               topology,
                                               consumer,
                                               changelogReader,
                                               config,
                                               streamsMetrics,
                                               stateDirectory,
                                               null,
                                               time,
                                               producer);

        assertFalse(task.initialize());
    }

    @SuppressWarnings("unchecked")
    private StreamTask createTaskThatThrowsExceptionOnClose() {
        final MockSourceNode processorNode = new MockSourceNode(topic1, intDeserializer, intDeserializer) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final List<ProcessorNode> processorNodes = Arrays.asList(processorNode, processorStreamTime, source1, source2);
        final Map<String, SourceNode> sourceNodes = new HashMap() {
            {
                put(topic1[0], processorNode);
                put(topic2[0], processorNode);
            }
        };
        final ProcessorTopology topology = new ProcessorTopology(processorNodes,
                                                                 sourceNodes,
                                                                 Collections.<String, SinkNode>emptyMap(),
                                                                 Collections.<StateStore>emptyList(),
                                                                 Collections.<String, String>emptyMap(),
                                                                 Collections.<StateStore>emptyList());

        return new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader, config,
            streamsMetrics, stateDirectory, null, time, producer);
    }

    private Iterable<ConsumerRecord<byte[], byte[]>> records(final ConsumerRecord<byte[], byte[]>... recs) {
        return Arrays.asList(recs);
    }

}
