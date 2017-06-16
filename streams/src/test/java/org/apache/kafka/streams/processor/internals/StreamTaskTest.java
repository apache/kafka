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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.NoOpProcessorContext;
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
import java.util.List;
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
    private final MockProcessorNode<Integer, Integer>  processor = new MockProcessorNode<>(10L);

    private final ProcessorTopology topology = new ProcessorTopology(
            Arrays.<ProcessorNode>asList(source1, source2, processor),
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
    private final StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, Time.SYSTEM, 5000);
    private final byte[] recordValue = intSerializer.serialize(null, 10);
    private final byte[] recordKey = intSerializer.serialize(null, 1);
    private final String applicationId = "applicationId";
    private final Metrics metrics = new Metrics();
    private final StreamsMetrics streamsMetrics = new MockStreamsMetrics(metrics);
    private final TaskId taskId00 = new TaskId(0, 0);
    private final MockTime time = new MockTime();
    private File baseDir = TestUtils.tempDirectory();
    private StateDirectory stateDirectory;
    private final RecordCollectorImpl recordCollector = new RecordCollectorImpl(producer, "taskId");
    private StreamsConfig config;
    private StreamsConfig eosConfig;
    private StreamTask task;

    private StreamsConfig createConfig(final boolean enableEoS) throws Exception {
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
    public void setup() throws Exception {
        consumer.assign(Arrays.asList(partition1, partition2));
        source1.addChild(processor);
        source2.addChild(processor);
        config = createConfig(false);
        eosConfig = createConfig(true);
        stateDirectory = new StateDirectory("applicationId", baseDir.getPath(), new MockTime());
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer,
                              changelogReader, config, streamsMetrics, stateDirectory, null, time, producer);
    }

    @After
    public void cleanup() throws IOException {
        try {
            if (task != null) {
                task.close(true);
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessOrder() throws Exception {
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

    @Test
    public void testMetrics() throws Exception {
        final String name = task.id().toString();
        final String[] entities = {"all", name};
        final String operation = "commit";

        final String groupName = "stream-task-metrics";
        final Map<String, String> tags = Collections.singletonMap("streams-task-id", name);

        assertNotNull(metrics.getSensor(operation));
        assertNotNull(metrics.getSensor(name + "-" + operation));

        for (final String entity : entities) {
            assertNotNull(metrics.metrics().get(metrics.metricName(entity + "-" + operation + "-latency-avg", groupName,
                "The average latency in milliseconds of " + entity + " " + operation + " operation.", tags)));
            assertNotNull(metrics.metrics().get(metrics.metricName(entity + "-" + operation + "-latency-max", groupName,
                "The max latency in milliseconds of " + entity + " " + operation + " operation.", tags)));
            assertNotNull(metrics.metrics().get(metrics.metricName(entity + "-" + operation + "-rate", groupName,
                "The average number of occurrence of " + entity + " " + operation + " operation per second.", tags)));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPauseResume() throws Exception {
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
    public void testMaybePunctuate() throws Exception {
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

        assertTrue(task.maybePunctuate());

        assertTrue(task.process());
        assertEquals(5, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(0, source2.numReceived);

        assertFalse(task.maybePunctuate());

        assertTrue(task.process());
        assertEquals(4, task.numBuffered());
        assertEquals(1, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertTrue(task.maybePunctuate());

        assertTrue(task.process());
        assertEquals(3, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(1, source2.numReceived);

        assertFalse(task.maybePunctuate());

        assertTrue(task.process());
        assertEquals(2, task.numBuffered());
        assertEquals(2, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertTrue(task.maybePunctuate());

        assertTrue(task.process());
        assertEquals(1, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(2, source2.numReceived);

        assertFalse(task.maybePunctuate());

        assertTrue(task.process());
        assertEquals(0, task.numBuffered());
        assertEquals(3, source1.numReceived);
        assertEquals(3, source2.numReceived);

        assertFalse(task.process());
        assertFalse(task.maybePunctuate());

        processor.supplier.checkAndClearPunctuateResult(20L, 30L, 40L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext() throws Exception {
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

        task.close(true);

        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader, config,
            streamsMetrics, stateDirectory, null, time, producer);
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

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuating() throws Exception {
        final ProcessorNode punctuator = new ProcessorNode("test", new AbstractProcessor() {
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
        }, Collections.<String>emptySet());
        punctuator.init(new NoOpProcessorContext());

        try {
            task.punctuate(punctuator, 1);
            fail("Should've thrown StreamsException");
        } catch (final StreamsException e) {
            final String message = e.getMessage();
            assertTrue("message=" + message + " should contain processor", message.contains("processor 'test'"));
            assertThat(((ProcessorContextImpl) task.processorContext()).currentNode(), nullValue());
        }
    }

    @Test
    public void shouldFlushRecordCollectorOnFlushState() throws Exception {
        final AtomicBoolean flushed = new AtomicBoolean(false);
        final StreamsMetrics streamsMetrics = new MockStreamsMetrics(new Metrics());
        final StreamTask streamTask = new StreamTask(taskId00, "appId", partitions, topology, consumer,
            changelogReader, config, streamsMetrics, stateDirectory, null, time, producer) {

            @Override
            RecordCollector createRecordCollector() {
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
    public void shouldCheckpointOffsetsOnCommit() throws Exception {
        final String storeName = "test";
        final String changelogTopic = ProcessorStateManager.storeChangelogTopic("appId", storeName);
        final InMemoryKeyValueStore inMemoryStore = new InMemoryKeyValueStore(storeName, null, null) {
            @Override
            public void init(final ProcessorContext context, final StateStore root) {
                context.register(root, true, null);
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
            RecordCollector createRecordCollector() {
                return new NoOpRecordCollector() {
                    @Override
                    public Map<TopicPartition, Long> offsets() {

                        return Collections.singletonMap(partition, offset);
                    }
                };
            }
        };

        time.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));

        streamTask.commit();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId00),
                                                                          ProcessorStateManager.CHECKPOINT_FILE_NAME));

        assertThat(checkpoint.read(), equalTo(Collections.singletonMap(partition, offset + 1)));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() throws Exception {
        ((ProcessorContextImpl) task.processorContext()).setCurrentNode(processor);
        try {
            task.punctuate(processor, 10);
            fail("Should throw illegal state exception as current node is not null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldCallPunctuateOnPassedInProcessorNode() throws Exception {
        task.punctuate(processor, 5);
        assertThat(processor.punctuatedAt, equalTo(5L));
        task.punctuate(processor, 10);
        assertThat(processor.punctuatedAt, equalTo(10L));
    }

    @Test
    public void shouldSetProcessorNodeOnContextBackToNullAfterSuccesfullPunctuate() throws Exception {
        task.punctuate(processor, 5);
        assertThat(((ProcessorContextImpl) task.processorContext()).currentNode(), nullValue());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() throws Exception {
        task.schedule(1);
    }

    @Test
    public void shouldNotThrowIExceptionOnScheduleIfCurrentNodeIsNotNull() throws Exception {
        ((ProcessorContextImpl) task.processorContext()).setCurrentNode(processor);
        task.schedule(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology() throws Exception {
        task.close(true);
        task = createTaskThatThrowsExceptionOnClose();
        try {
            task.close(true);
            fail("should have thrown runtime exception");
        } catch (final RuntimeException e) {
            task = null;
        }
        assertTrue(processor.closed);
        assertTrue(source1.closed);
        assertTrue(source2.closed);
    }

    @Test
    public void shouldInitAndBeginTransactionOnCreateIfEosEnabled() throws Exception {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        assertTrue(producer.transactionInitialized());
        assertTrue(producer.transactionInFlight());
    }

    @Test
    public void shouldNotInitOrBeginTransactionOnCreateIfEosDisabled() throws Exception {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

        assertFalse(producer.transactionInitialized());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled() throws Exception {
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
    public void shouldCommitTransactionOnSuspendEvenIfTransactionIsEmptyIfEosEnabled() throws Exception {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.suspend();
        assertTrue(producer.transactionCommitted());
        assertFalse(producer.transactionInFlight());
    }

    @Test
    public void shouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled() throws Exception {
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
    public void shouldStartNewTransactionOnResumeIfEosEnabled() throws Exception {
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
    public void shouldNotStartNewTransactionOnResumeIfEosDisabled() throws Exception {
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
    public void shouldStartNewTransactionOnCommitIfEosEnabled() throws Exception {
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
    public void shouldNotStartNewTransactionOnCommitIfEosDisabled() throws Exception {
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
    public void shouldAbortTransactionOnDirtyClosedIfEosEnabled() throws Exception {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.close(false);
        task = null;
        assertTrue(producer.transactionAborted());
    }

    @Test
    public void shouldNotAbortTransactionOnDirtyClosedIfEosDisabled() throws Exception {
        final MockProducer producer = new MockProducer();
        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer, changelogReader,
            config, streamsMetrics, stateDirectory, null, time, producer);

        task.close(false);
        assertFalse(producer.transactionAborted());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCloseProducerOnCloseWhenEosEnabled() throws Exception {
        final MockProducer producer = new MockProducer();

        task = new StreamTask(taskId00, applicationId, partitions, topology, consumer,
            changelogReader, eosConfig, streamsMetrics, stateDirectory, null, time, producer);

        task.close(true);
        task = null;
        assertTrue(producer.closed());
    }

    @SuppressWarnings("unchecked")
    private StreamTask createTaskThatThrowsExceptionOnClose() {
        final MockSourceNode processorNode = new MockSourceNode(topic1, intDeserializer, intDeserializer) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final List<ProcessorNode> processorNodes = Arrays.asList(processorNode, processor, source1, source2);
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
