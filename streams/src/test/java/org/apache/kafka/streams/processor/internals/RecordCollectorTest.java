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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.AlwaysContinueProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecordCollectorTest {

    private final TaskId taskId = new TaskId(0, 0);
    private final LogContext logContext = new LogContext("test ");
    private final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
    private final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test"));

    private final List<PartitionInfo> infos = Arrays.asList(
        new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0])
    );

    private final Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
        Collections.emptySet(), Collections.emptySet());


    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private final StringSerializer stringSerializer = new StringSerializer();

    private final StreamPartitioner<String, Object> streamPartitioner = (topic, key, value, numPartitions) -> Integer.parseInt(key) % numPartitions;

    @Test
    public void testSpecificPartition() {

        final RecordCollectorImpl collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

        collector.send("topic1", "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send("topic1", "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send("topic1", "999", "0", null, 0, null, stringSerializer, stringSerializer);

        collector.send("topic1", "999", "0", headers, 1, null, stringSerializer, stringSerializer);
        collector.send("topic1", "999", "0", headers, 1, null, stringSerializer, stringSerializer);

        collector.send("topic1", "999", "0", headers, 2, null, stringSerializer, stringSerializer);

        final Map<TopicPartition, Long> offsets = collector.offsets();

        assertEquals((Long) 2L, offsets.get(new TopicPartition("topic1", 0)));
        assertEquals((Long) 1L, offsets.get(new TopicPartition("topic1", 1)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition("topic1", 2)));

        // ignore StreamPartitioner
        collector.send("topic1", "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send("topic1", "999", "0", null, 1, null, stringSerializer, stringSerializer);
        collector.send("topic1", "999", "0", headers, 2, null, stringSerializer, stringSerializer);

        assertEquals((Long) 3L, offsets.get(new TopicPartition("topic1", 0)));
        assertEquals((Long) 2L, offsets.get(new TopicPartition("topic1", 1)));
        assertEquals((Long) 1L, offsets.get(new TopicPartition("topic1", 2)));
    }

    @Test
    public void testStreamPartitioner() {

        final RecordCollectorImpl collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "9", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "27", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "81", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "243", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.send("topic1", "28", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "82", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "244", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.send("topic1", "245", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final Map<TopicPartition, Long> offsets = collector.offsets();

        assertEquals((Long) 4L, offsets.get(new TopicPartition("topic1", 0)));
        assertEquals((Long) 2L, offsets.get(new TopicPartition("topic1", 1)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition("topic1", 2)));
    }

    @Test
    public void shouldNotAllowOffsetsToBeUpdatedExternally() {
        final String topic = "topic1";
        final TopicPartition topicPartition = new TopicPartition(topic, 0);

        final RecordCollectorImpl collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);

        final Map<TopicPartition, Long> offsets = collector.offsets();

        assertThat(offsets.get(topicPartition), equalTo(2L));
        assertThrows(UnsupportedOperationException.class, () -> offsets.put(new TopicPartition(topic, 0), 50L));

        assertThat(collector.offsets().get(topicPartition), equalTo(2L));
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionOnAnyExceptionButProducerFencedException() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    throw new KafkaException();
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @Test(expected = TaskMigratedException.class)
    public void shouldThrowRecoverableExceptionWhenProducerFencedInCallback() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new ProducerFencedException("asdf"));
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentCallIfASendFailsWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new Exception());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        try {
            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* ok */ }
    }

    @Test
    public void shouldNotThrowStreamsExceptionOnSubsequentCallIfASendFailsWithContinueExceptionHandler() {
        final Metrics metrics = new Metrics();
        final LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister();
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, AlwaysContinueProductionExceptionHandler.class.getName());
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            new MockStreamsMetrics(metrics),
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new Exception());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.flush();

        final Metric metric = metrics.metrics().get(new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            Utils.mkMap(Utils.mkEntry("thread-id", "main"), Utils.mkEntry("task-id", taskId.toString()))));
        assertEquals(1.0, metric.metricValue());

        final List<String> messages = logCaptureAppender.getMessages();
        assertTrue(messages.get(messages.size() - 1).endsWith("Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded."));
        LogCaptureAppender.unregister(logCaptureAppender);

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @Test
    public void shouldThrowStreamsExceptionOnFlushIfASendFailedWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new Exception());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        try {
            collector.flush();
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* ok */ }
    }

    @Test
    public void shouldThrowStreamsExceptionWithTimeoutHintOnProducerTimeoutWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new TimeoutException());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException expected = assertThrows(StreamsException.class, collector::flush);
        assertTrue(expected.getCause() instanceof TimeoutException);
    }

    @Test
    public void shouldNotThrowStreamsExceptionOnFlushIfASendFailedWithContinueExceptionHandler() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, AlwaysContinueProductionExceptionHandler.class.getName());
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new Exception());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.flush();
    }

    @Test
    public void shouldThrowStreamsExceptionOnCloseIfASendFailedWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new Exception());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        try {
            collector.close();
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* ok */ }
    }

    @Test
    public void shouldNotThrowStreamsExceptionOnCloseIfASendFailedWithContinueExceptionHandler() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, AlwaysContinueProductionExceptionHandler.class.getName());
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new Exception());
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.close();
    }

    @Test
    public void shouldThrowStreamsExceptionOnEOSInitializeTimeout() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        assertThrows(StreamsException.class, () ->
            new RecordCollectorImpl(
                taskId,
                new StreamsConfig(props),
                logContext,
                streamsMetrics,
                null,
                id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                    @Override
                    public void initTransactions() {
                        throw new TimeoutException("test");
                    }
                }
            )
        );
    }

    @Test
    public void shouldThrowMigrateExceptionOnEOSProcessFenced() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    throw new KafkaException(new ProducerFencedException("boom"));
                }
            }
        );

        assertThrows(TaskMigratedException.class, () -> collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSProcessFenced() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new ProducerFencedException("boom"));
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        assertThrows(TaskMigratedException.class, () -> collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSProcessUknownPid() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, new OutOfOrderSequenceException("boom"));
                    return null;
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        assertThrows(TaskMigratedException.class, () -> collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnCommitFailed() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
                @Override
                public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
                    throw new CommitFailedException();
                }
            },
            id -> new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        assertThrows(TaskMigratedException.class, () -> collector.commit(null));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnCommitUnexpected() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
                @Override
                public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
                    throw new KafkaException();
                }
            },
            id -> new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        assertThrows(StreamsException.class, () -> collector.commit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(5L))));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSSendOffsetFenced() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets, final String consumerGroupId) {
                    throw new ProducerFencedException("boom");
                }
            }
        );

        assertThrows(TaskMigratedException.class, () -> collector.commit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(5L))));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSCommitFenced() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void commitTransaction() {
                    throw new ProducerFencedException("boom");
                }
            }
        );

        assertThrows(TaskMigratedException.class, () -> collector.commit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(5L))));
    }

    @Test
    public void shouldFailWithStreamsExceptionOnEOSSendOffsetUnexpected() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets, final String consumerGroupId) {
                    throw new KafkaException("boom");
                }
            }
        );

        assertThrows(StreamsException.class, () -> collector.commit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(5L))));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSCommitUnexpected() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void commitTransaction() {
                    throw new KafkaException("boom");
                }
            }
        );

        assertThrows(StreamsException.class, () -> collector.commit(Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(5L))));
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowIfTopicIsUnknownWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public List<PartitionInfo> partitionsFor(final String topic) {
                    return Collections.emptyList();
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowIfTopicIsUnknownWithContinueExceptionHandler() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, AlwaysContinueProductionExceptionHandler.class.getName());
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public List<PartitionInfo> partitionsFor(final String topic) {
                    return Collections.emptyList();
                }
            }
        );

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @Test
    public void testRecordHeaderPassThroughSerializer() {
        final CustomStringSerializer keySerializer = new CustomStringSerializer();
        final CustomStringSerializer valueSerializer = new CustomStringSerializer();
        keySerializer.configure(Collections.emptyMap(), true);

        final MockProducer<byte[], byte[]> mockProducer =
            new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> mockProducer
        );

        collector.send("topic1", "3", "0", new RecordHeaders(), null, keySerializer, valueSerializer, streamPartitioner);

        final List<ProducerRecord<byte[], byte[]>> recordHistory = mockProducer.history();
        for (final ProducerRecord<byte[], byte[]> sentRecord : recordHistory) {
            final Headers headers = sentRecord.headers();
            assertEquals(2, headers.toArray().length);
            assertEquals(new RecordHeader("key", "key".getBytes()), headers.lastHeader("key"));
            assertEquals(new RecordHeader("value", "value".getBytes()), headers.lastHeader("value"));
        }
    }

    private static class CustomStringSerializer extends StringSerializer {

        private boolean isKey;

        private CustomStringSerializer() {
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            this.isKey = isKey;
            super.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final String data) {
            if (isKey) {
                headers.add(new RecordHeader("key", "key".getBytes()));
            } else {
                headers.add(new RecordHeader("value", "value".getBytes()));
            }
            return serialize(topic, data);
        }
    }
}
