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
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordCollectorTest {

    private final TaskId taskId = new TaskId(0, 0);
    private final LogContext logContext = new LogContext("test ");
    private final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
    private final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig("test"));

    private final String topic = "topic";
    private final Cluster cluster = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        Arrays.asList(
            new PartitionInfo(topic, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topic, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topic, 2, Node.noNode(), new Node[0], new Node[0])
        ),
        Collections.emptySet(),
        Collections.emptySet()
    );

    private final StringSerializer stringSerializer = new StringSerializer();
    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private final MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final StreamPartitioner<String, Object> streamPartitioner = (topic, key, value, numPartitions) -> Integer.parseInt(key) % numPartitions;

    private RecordCollectorImpl collector;

    @Before
    public void setup() {
        collector = new RecordCollectorImpl(taskId, streamsConfig, logContext, streamsMetrics, consumer, id -> producer);
    }

    @After
    public void cleanup() {
        collector.close();
    }

    @Test
    public void shouldSendToSpecificPartition() {
        final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", headers, 1, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", headers, 1, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", headers, 2, null, stringSerializer, stringSerializer);

        Map<TopicPartition, Long> offsets = collector.offsets();

        assertEquals(2L, (long) offsets.get(new TopicPartition(topic, 0)));
        assertEquals(1L, (long) offsets.get(new TopicPartition(topic, 1)));
        assertEquals(0L, (long) offsets.get(new TopicPartition(topic, 2)));
        assertEquals(6, producer.history().size());

        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 1, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", headers, 2, null, stringSerializer, stringSerializer);

        offsets = collector.offsets();

        assertEquals(3L, (long) offsets.get(new TopicPartition(topic, 0)));
        assertEquals(2L, (long) offsets.get(new TopicPartition(topic, 1)));
        assertEquals(1L, (long) offsets.get(new TopicPartition(topic, 2)));
        assertEquals(9, producer.history().size());
    }

    @Test
    public void shouldSendWithPartitioner() {
        final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "9", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "27", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "81", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "243", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "28", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "82", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "244", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.send(topic, "245", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final Map<TopicPartition, Long> offsets = collector.offsets();

        assertEquals(4L, (long) offsets.get(new TopicPartition(topic, 0)));
        assertEquals(2L, (long) offsets.get(new TopicPartition(topic, 1)));
        assertEquals(0L, (long) offsets.get(new TopicPartition(topic, 2)));
        assertEquals(9, producer.history().size());

        // returned offsets should not be modified
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        assertThrows(UnsupportedOperationException.class, () -> offsets.put(topicPartition, 50L));
    }

    @Test
    public void shouldSendWithNoPartition() {
        final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

        collector.send(topic, "3", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "9", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "27", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "81", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "243", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "28", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "82", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "244", "0", headers, null, null, stringSerializer, stringSerializer);
        collector.send(topic, "245", "0", headers, null, null, stringSerializer, stringSerializer);

        final Map<TopicPartition, Long> offsets = collector.offsets();

        // with mock producer without specific partition, we would use default producer partitioner with murmur hash
        assertEquals(3L, (long) offsets.get(new TopicPartition(topic, 0)));
        assertEquals(2L, (long) offsets.get(new TopicPartition(topic, 1)));
        assertEquals(1L, (long) offsets.get(new TopicPartition(topic, 2)));
        assertEquals(9, producer.history().size());
    }

    @Test
    public void shouldUpdateOffsetsUponCompletion() {
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<>(cluster, false, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
        );

        Map<TopicPartition, Long> offsets = collector.offsets();

        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 1, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 2, null, stringSerializer, stringSerializer);

        assertEquals(Collections.emptyMap(), offsets);

        collector.flush();

        offsets = collector.offsets();
        assertEquals((Long) 0L, offsets.get(new TopicPartition(topic, 0)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition(topic, 1)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition(topic, 2)));
    }

    @Test
    public void shouldThrowStreamsExceptionOnSendFatalException() {
        final KafkaException exception = new KafkaException();
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    throw exception;
                }
            }
        );

        final StreamsException thrown = assertThrows(StreamsException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnProducerFencedException() {
        final KafkaException exception = new ProducerFencedException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
                    throw new KafkaException(exception);
                }
            }
        );

        final TaskMigratedException thrown = assertThrows(TaskMigratedException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnUnknownProducerIdException() {
        final KafkaException exception = new UnknownProducerIdException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
                    throw new KafkaException(exception);
                }
            }
        );

        final TaskMigratedException thrown = assertThrows(TaskMigratedException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentCallWhenProducerFencedInCallback() {
        final KafkaException exception = new ProducerFencedException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, exception);
                    return null;
                }
            }
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        TaskMigratedException thrown = assertThrows(TaskMigratedException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(TaskMigratedException.class, collector::flush);
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(TaskMigratedException.class, collector::close);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentCallWhenProducerUnknownInCallback() {
        final KafkaException exception = new UnknownProducerIdException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
                    callback.onCompletion(null, exception);
                    return null;
                }
            });

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        TaskMigratedException thrown = assertThrows(TaskMigratedException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(TaskMigratedException.class, collector::flush);
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(TaskMigratedException.class, collector::close);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentCallIfASendFailsWithDefaultExceptionHandler() {
        final KafkaException exception = new KafkaException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            streamsConfig,
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                    callback.onCompletion(null, exception);
                    return null;
                }
            }
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        StreamsException thrown = assertThrows(StreamsException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(StreamsException.class, collector::flush);
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(StreamsException.class, collector::close);
        assertEquals(exception, thrown.getCause());
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

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.flush();

        final Metric metric = metrics.metrics().get(new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            Utils.mkMap(Utils.mkEntry("thread-id", Thread.currentThread().getName()), Utils.mkEntry("task-id", taskId.toString()))));
        assertEquals(1.0, metric.metricValue());

        final List<String> messages = logCaptureAppender.getMessages();
        assertTrue(messages.get(messages.size() - 1).endsWith("Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded."));
        LogCaptureAppender.unregister(logCaptureAppender);

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.flush();
        collector.close();
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentCallIfFatalEvenWithContinueExceptionHandler() {
        final KafkaException exception = new AuthenticationException("KABOOM!");
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
                    callback.onCompletion(null, exception);
                    return null;
                }
            }
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        StreamsException thrown = assertThrows(StreamsException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(StreamsException.class, collector::flush);
        assertEquals(exception, thrown.getCause());

        thrown = assertThrows(StreamsException.class, collector::close);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldRethrowOnEOSInitializeTimeout() {
        final KafkaException exception = new TimeoutException("KABOOM!");
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        final RecordCollector recordCollector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void initTransactions() {
                    throw exception;
                }
            }
        );

        final TimeoutException thrown = assertThrows(TimeoutException.class, recordCollector::initialize);
        assertEquals(exception, thrown);
    }

    @Test
    public void shouldThrowStreamsExceptionOnEOSInitializeError() {
        final KafkaException exception = new KafkaException("KABOOM!");
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        final RecordCollector recordCollector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            null,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void initTransactions() {
                    throw exception;
                }
            }
        );

        final StreamsException thrown = assertThrows(StreamsException.class, recordCollector::initialize);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowMigrateExceptionOnEOSFirstSendProducerFenced() {
        final KafkaException exception = new ProducerFencedException("KABOOM!");
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
                public synchronized void beginTransaction() {
                    throw exception;
                }
            }
        );

        final TaskMigratedException thrown = assertThrows(TaskMigratedException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowMigrateExceptionOnEOSFirstSendFatal() {
        final KafkaException exception = new KafkaException("KABOOM!");
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
                public synchronized void beginTransaction() {
                    throw exception;
                }
            }
        );

        final StreamsException thrown = assertThrows(StreamsException.class, () ->
            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
        assertEquals(exception, thrown.getCause());
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

        assertThrows(StreamsException.class, () -> collector.commit(null));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSBeginTxnFenced() {
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
                public void beginTransaction() {
                    throw new ProducerFencedException("KABOOM!");
                }
            }
        );

        assertThrows(TaskMigratedException.class, () -> collector.commit(null));
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
                    throw new ProducerFencedException("KABOOM!");
                }
            }
        );
        collector.initialize();

        assertThrows(TaskMigratedException.class, () -> collector.commit(null));
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
                    throw new ProducerFencedException("KABOOM!");
                }
            }
        );
        collector.initialize();

        assertThrows(TaskMigratedException.class, () -> collector.commit(Collections.emptyMap()));
    }

    @Test
    public void shouldFailWithMigrateExceptionOnEOSBeginTxnUnexpected() {
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
                public void beginTransaction() {
                    throw new KafkaException("KABOOM!");
                }
            }
        );

        assertThrows(StreamsException.class, () -> collector.commit(null));
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
                    throw new KafkaException("KABOOM!");
                }
            }
        );
        collector.initialize();

        assertThrows(StreamsException.class, () -> collector.commit(null));
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
                    throw new KafkaException("KABOOM!");
                }
            }
        );
        collector.initialize();

        assertThrows(StreamsException.class, () -> collector.commit(Collections.emptyMap()));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEOSCloseFatalException() {
        final KafkaException exception = new KafkaException();
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void close() {
                    throw exception;
                }
            }
        );

        final StreamsException thrown = assertThrows(StreamsException.class, collector::close);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldNotAbortTxnOnEOSCloseIfNothingSent() {
        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void abortTransaction() {
                    functionCalled.set(true);
                    super.abortTransaction();
                }
            }
        );

        collector.close();

        assertFalse(functionCalled.get());
    }

    @Test
    public void shouldNotAbortTxnOnEOSCloseIfCommitted() {
        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void abortTransaction() {
                    functionCalled.set(true);
                    super.abortTransaction();
                }
            }
        );
        collector.initialize();
        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.commit(Collections.emptyMap());

        collector.close();

        assertFalse(functionCalled.get());
    }

    @Test
    public void shouldThrowStreamsExceptionOnEOSAbortTxnFatalException() {
        final KafkaException exception = new KafkaException();
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void abortTransaction() {
                    throw exception;
                }
            }
        );
        collector.initialize();
        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(StreamsException.class, collector::close);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldSwallowOnEOSAbortTxnFatalException() {
        final Properties props = StreamsTestUtils.getStreamsConfig("test");
        props.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        final RecordCollector collector = new RecordCollectorImpl(
            taskId,
            new StreamsConfig(props),
            logContext,
            streamsMetrics,
            consumer,
            id -> new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                @Override
                public void abortTransaction() {
                    throw new ProducerFencedException("KABOOM!");
                }
            }
        );
        collector.initialize();

        // this call is to begin an inflight txn
        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.close();
    }

    @Test
    public void shouldThrowIfTopicIsUnknownOnSendWithPartitioner() {
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

        final StreamsException thrown = assertThrows(StreamsException.class, () -> collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
        assertTrue(thrown.getMessage().startsWith("Could not get partition information for topic"));
    }

    @Test
    public void shouldPassThroughRecordHeaderToSerializer() {
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

        collector.send(topic, "3", "0", new RecordHeaders(), null, keySerializer, valueSerializer, streamPartitioner);

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
