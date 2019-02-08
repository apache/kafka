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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Sum;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.AlwaysContinueProductionExceptionHandler;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecordCollectorTest {

    private final LogContext logContext = new LogContext("test ");

    private final List<PartitionInfo> infos = Arrays.asList(
        new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0])
    );

    private final Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
        Collections.emptySet(), Collections.emptySet());


    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private final StringSerializer stringSerializer = new StringSerializer();

    private final StreamPartitioner<String, Object> streamPartitioner = (topic, key, value, numPartitions) -> {
        return Integer.parseInt(key) % numPartitions;
    };

    @Test
    public void testSpecificPartition() {

        final RecordCollectorImpl collector = new RecordCollectorImpl(
            "RecordCollectorTest-TestSpecificPartition",
            new LogContext("RecordCollectorTest-TestSpecificPartition "),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records")
        );
        collector.init(new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer));

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
            "RecordCollectorTest-TestStreamPartitioner",
            new LogContext("RecordCollectorTest-TestStreamPartitioner "),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records")
        );
        collector.init(new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer));

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

    @SuppressWarnings("unchecked")
    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionOnAnyExceptionButProducerFencedException() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                throw new KafkaException();
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionOnSubsequentCallIfASendFailsWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        try {
            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* ok */ }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotThrowStreamsExceptionOnSubsequentCallIfASendFailsWithContinueExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new AlwaysContinueProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRecordSkippedMetricAndLogWarningIfSendFailsWithContinueExceptionHandler() {
        final Metrics metrics = new Metrics();
        final Sensor sensor = metrics.sensor("skipped-records");
        final LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister();
        final MetricName metricName = new MetricName("name", "group", "description", Collections.emptyMap());
        sensor.add(metricName, new Sum());
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new AlwaysContinueProductionExceptionHandler(),
            sensor);
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });
        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        assertEquals(1.0, metrics.metrics().get(metricName).metricValue());
        assertTrue(logCaptureAppender.getMessages().contains("test Error sending records topic=[topic1] and partition=[0]; The exception handler chose to CONTINUE processing in spite of this error. Enable TRACE logging to view failed messages key and value."));
        LogCaptureAppender.unregister(logCaptureAppender);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionOnFlushIfASendFailedWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        try {
            collector.flush();
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* ok */ }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotThrowStreamsExceptionOnFlushIfASendFailedWithContinueExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new AlwaysContinueProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.flush();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionOnCloseIfASendFailedWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        try {
            collector.close();
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* ok */ }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotThrowStreamsExceptionOnCloseIfASendFailedWithContinueExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new AlwaysContinueProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
                callback.onCompletion(null, new Exception());
                return null;
            }
        });

        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        collector.close();
    }

    @SuppressWarnings("unchecked")
    @Test(expected = StreamsException.class)
    public void shouldThrowIfTopicIsUnknownWithDefaultExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.emptyList();
            }

        });
        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = StreamsException.class)
    public void shouldThrowIfTopicIsUnknownWithContinueExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            "test",
            logContext,
            new AlwaysContinueProductionExceptionHandler(),
            new Metrics().sensor("skipped-records"));
        collector.init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
            @Override
            public List<PartitionInfo> partitionsFor(final String topic) {
                return Collections.emptyList();
            }

        });
        collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
    }

    @Test
    public void testRecordHeaderPassThroughSerializer() {
        final CustomStringSerializer keySerializer = new CustomStringSerializer();
        final CustomStringSerializer valueSerializer = new CustomStringSerializer();
        keySerializer.configure(Collections.emptyMap(), true);

        final RecordCollectorImpl collector = new RecordCollectorImpl(
                "test",
                logContext,
                new DefaultProductionExceptionHandler(),
                new Metrics().sensor("skipped-records")
        );
        final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(cluster, true, new DefaultPartitioner(),
                byteArraySerializer, byteArraySerializer);
        collector.init(mockProducer);

        collector.send("topic1", "3", "0", new RecordHeaders(), null, keySerializer, valueSerializer, streamPartitioner);

        final List<ProducerRecord<byte[], byte[]>> recordHistory = mockProducer.history();
        for (final ProducerRecord<byte[], byte[]> sentRecord : recordHistory) {
            final Headers headers = sentRecord.headers();
            assertEquals(2, headers.toArray().length);
            assertEquals(new RecordHeader("key", "key".getBytes()), headers.lastHeader("key"));
            assertEquals(new RecordHeader("value", "value".getBytes()), headers.lastHeader("value"));
        }
    }

    @Test
    public void testShouldNotThrowNPEOnCloseIfProducerIsNotInitialized() {
        final RecordCollectorImpl collector = new RecordCollectorImpl(
                "NoNPE",
                logContext,
                new DefaultProductionExceptionHandler(),
                new Metrics().sensor("skipped-records")
        );

        collector.close();
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
