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
import org.apache.kafka.clients.producer.Producer;
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
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.AlwaysContinueProductionExceptionHandler;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.MockClientSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordCollectorTest {

    private final LogContext logContext = new LogContext("test ");
    private final TaskId taskId = new TaskId(0, 0);
    private final ProductionExceptionHandler productionExceptionHandler = new DefaultProductionExceptionHandler();
    private final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
    private final StreamsConfig config = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    ));
    private final StreamsConfig eosConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    ));

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

    private final StreamPartitioner<String, Object> streamPartitioner =
        (topic, key, value, numPartitions) -> Integer.parseInt(key) % numPartitions;

    private MockProducer<byte[], byte[]> mockProducer;
    private StreamsProducer streamsProducer;

    private RecordCollectorImpl collector;

    @Before
    public void setup() {
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        clientSupplier.setCluster(cluster);
        streamsProducer = new StreamsProducer(
            config,
            "threadId",
            clientSupplier,
            null,
        null,
            logContext
        );
        mockProducer = clientSupplier.producers.get(0);
        collector = new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            productionExceptionHandler,
            streamsMetrics);
    }

    @After
    public void cleanup() {
        collector.closeClean();
    }

    @Test
    public void shouldSendToSpecificPartition() {
        final Headers headers = new RecordHeaders(new Header[] {new RecordHeader("key", "value".getBytes())});

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
        assertEquals(6, mockProducer.history().size());

        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 1, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", headers, 2, null, stringSerializer, stringSerializer);

        offsets = collector.offsets();

        assertEquals(3L, (long) offsets.get(new TopicPartition(topic, 0)));
        assertEquals(2L, (long) offsets.get(new TopicPartition(topic, 1)));
        assertEquals(1L, (long) offsets.get(new TopicPartition(topic, 2)));
        assertEquals(9, mockProducer.history().size());
    }

    @Test
    public void shouldSendWithPartitioner() {
        final Headers headers = new RecordHeaders(new Header[] {new RecordHeader("key", "value".getBytes())});

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
        assertEquals(9, mockProducer.history().size());

        // returned offsets should not be modified
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        assertThrows(UnsupportedOperationException.class, () -> offsets.put(topicPartition, 50L));
    }

    @Test
    public void shouldSendWithNoPartition() {
        final Headers headers = new RecordHeaders(new Header[] {new RecordHeader("key", "value".getBytes())});

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
        assertEquals(9, mockProducer.history().size());
    }

    @Test
    public void shouldUpdateOffsetsUponCompletion() {
        Map<TopicPartition, Long> offsets = collector.offsets();

        collector.send(topic, "999", "0", null, 0, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 1, null, stringSerializer, stringSerializer);
        collector.send(topic, "999", "0", null, 2, null, stringSerializer, stringSerializer);

        assertEquals(Collections.<TopicPartition, Long>emptyMap(), offsets);

        collector.flush();

        offsets = collector.offsets();
        assertEquals((Long) 0L, offsets.get(new TopicPartition(topic, 0)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition(topic, 1)));
        assertEquals((Long) 0L, offsets.get(new TopicPartition(topic, 2)));
    }

    @Test
    public void shouldPassThroughRecordHeaderToSerializer() {
        final CustomStringSerializer keySerializer = new CustomStringSerializer();
        final CustomStringSerializer valueSerializer = new CustomStringSerializer();
        keySerializer.configure(Collections.emptyMap(), true);

        collector.send(topic, "3", "0", new RecordHeaders(), null, keySerializer, valueSerializer, streamPartitioner);

        final List<ProducerRecord<byte[], byte[]>> recordHistory = mockProducer.history();
        for (final ProducerRecord<byte[], byte[]> sentRecord : recordHistory) {
            final Headers headers = sentRecord.headers();
            assertEquals(2, headers.toArray().length);
            assertEquals(new RecordHeader("key", "key".getBytes()), headers.lastHeader("key"));
            assertEquals(new RecordHeader("value", "value".getBytes()), headers.lastHeader("value"));
        }
    }

    @Test
    public void shouldForwardFlushToStreamsProducer() {
        final StreamsProducer streamsProducer = mock(StreamsProducer.class);
        expect(streamsProducer.eosEnabled()).andReturn(false);
        streamsProducer.flush();
        expectLastCall();
        replay(streamsProducer);

        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            productionExceptionHandler,
            streamsMetrics);

        collector.flush();

        verify(streamsProducer);
    }

    @Test
    public void shouldForwardFlushToStreamsProducerEosEnabled() {
        final StreamsProducer streamsProducer = mock(StreamsProducer.class);
        expect(streamsProducer.eosEnabled()).andReturn(true);
        streamsProducer.flush();
        expectLastCall();
        replay(streamsProducer);

        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            productionExceptionHandler,
            streamsMetrics);

        collector.flush();

        verify(streamsProducer);
    }

    @Test
    public void shouldNotAbortTxOnCloseCleanIfEosEnabled() {
        final StreamsProducer streamsProducer = mock(StreamsProducer.class);
        expect(streamsProducer.eosEnabled()).andReturn(true);
        replay(streamsProducer);

        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            productionExceptionHandler,
            streamsMetrics);

        collector.closeClean();

        verify(streamsProducer);
    }

    @Test
    public void shouldAbortTxOnCloseDirtyIfEosEnabled() {
        final StreamsProducer streamsProducer = mock(StreamsProducer.class);
        expect(streamsProducer.eosEnabled()).andReturn(true);
        streamsProducer.abortTransaction();
        replay(streamsProducer);

        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            productionExceptionHandler,
            streamsMetrics);

        collector.closeDirty();

        verify(streamsProducer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldThrowInformativeStreamsExceptionOnKeyClassCastException() {
        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> this.collector.send(
                "topic",
                "key",
                "value",
                new RecordHeaders(),
                0,
                0L,
                (Serializer) new LongSerializer(), // need to add cast to trigger `ClassCastException`
                new StringSerializer())
        );

        assertThat(expected.getCause(), instanceOf(ClassCastException.class));
        assertThat(
            expected.getMessage(),
            equalTo(
                "ClassCastException while producing data to topic topic. " +
                    "A serializer (key: org.apache.kafka.common.serialization.LongSerializer / value: org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual key or value type (key type: java.lang.String / value type: java.lang.String). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters " +
                    "(for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with `Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).")
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldThrowInformativeStreamsExceptionOnKeyAndNullValueClassCastException() {
        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> this.collector.send(
                "topic",
                "key",
                null,
                new RecordHeaders(),
                0,
                0L,
                (Serializer) new LongSerializer(), // need to add cast to trigger `ClassCastException`
                new StringSerializer())
        );

        assertThat(expected.getCause(), instanceOf(ClassCastException.class));
        assertThat(
            expected.getMessage(),
            equalTo(
                "ClassCastException while producing data to topic topic. " +
                    "A serializer (key: org.apache.kafka.common.serialization.LongSerializer / value: org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual key or value type (key type: java.lang.String / value type: unknown because value is null). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters " +
                    "(for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with `Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).")
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldThrowInformativeStreamsExceptionOnValueClassCastException() {
        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> this.collector.send(
                "topic",
                "key",
                "value",
                new RecordHeaders(),
                0,
                0L,
                new StringSerializer(),
                (Serializer) new LongSerializer()) // need to add cast to trigger `ClassCastException`
        );

        assertThat(expected.getCause(), instanceOf(ClassCastException.class));
        assertThat(
            expected.getMessage(),
            equalTo(
                "ClassCastException while producing data to topic topic. " +
                    "A serializer (key: org.apache.kafka.common.serialization.StringSerializer / value: org.apache.kafka.common.serialization.LongSerializer) " +
                    "is not compatible to the actual key or value type (key type: java.lang.String / value type: java.lang.String). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters " +
                    "(for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with `Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).")
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldThrowInformativeStreamsExceptionOnValueAndNullKeyClassCastException() {
        final StreamsException expected = assertThrows(
            StreamsException.class,
            () -> this.collector.send(
                "topic",
                null,
                "value",
                new RecordHeaders(),
                0,
                0L,
                new StringSerializer(),
                (Serializer) new LongSerializer()) // need to add cast to trigger `ClassCastException`
        );

        assertThat(expected.getCause(), instanceOf(ClassCastException.class));
        assertThat(
            expected.getMessage(),
            equalTo(
                "ClassCastException while producing data to topic topic. " +
                    "A serializer (key: org.apache.kafka.common.serialization.StringSerializer / value: org.apache.kafka.common.serialization.LongSerializer) " +
                    "is not compatible to the actual key or value type (key type: unknown because key is null / value type: java.lang.String). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters " +
                    "(for example if using the DSL, `#to(String topic, Produced<K, V> produced)` with `Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).")
        );
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentSendWhenProducerFencedInCallback() {
        testThrowTaskMigratedExceptionOnSubsequentSend(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentSendWhenInvalidEpochInCallback() {
        testThrowTaskMigratedExceptionOnSubsequentSend(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnSubsequentSend(final RuntimeException exception) {
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            productionExceptionHandler,
            streamsMetrics
        );
        collector.initialize();

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class, () ->
                                             collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentFlushWhenProducerFencedInCallback() {
        testThrowTaskMigratedExceptionOnSubsequentFlush(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentFlushWhenInvalidEpochInCallback() {
        testThrowTaskMigratedExceptionOnSubsequentFlush(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnSubsequentFlush(final RuntimeException exception) {
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            productionExceptionHandler,
            streamsMetrics
        );
        collector.initialize();

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final TaskMigratedException thrown = assertThrows(TaskMigratedException.class, collector::flush);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentCloseWhenProducerFencedInCallback() {
        testThrowTaskMigratedExceptionOnSubsequentClose(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnSubsequentCloseWhenInvalidEpochInCallback() {
        testThrowTaskMigratedExceptionOnSubsequentClose(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnSubsequentClose(final RuntimeException exception) {
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            productionExceptionHandler,
            streamsMetrics
        );
        collector.initialize();

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final TaskMigratedException thrown = assertThrows(TaskMigratedException.class, collector::closeClean);
        assertEquals(exception, thrown.getCause());
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentSendIfASendFailsWithDefaultExceptionHandler() {
        final KafkaException exception = new KafkaException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            productionExceptionHandler,
            streamsMetrics
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());
        assertThat(
            thrown.getMessage(),
            equalTo("Error encountered sending record to topic topic for task 0_0 due to:" +
                "\norg.apache.kafka.common.KafkaException: KABOOM!" +
                "\nException handler choose to FAIL the processing, no more records would be sent.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentFlushIfASendFailsWithDefaultExceptionHandler() {
        final KafkaException exception = new KafkaException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            productionExceptionHandler,
            streamsMetrics
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(StreamsException.class, collector::flush);
        assertEquals(exception, thrown.getCause());
        assertThat(
            thrown.getMessage(),
            equalTo("Error encountered sending record to topic topic for task 0_0 due to:" +
                        "\norg.apache.kafka.common.KafkaException: KABOOM!" +
                        "\nException handler choose to FAIL the processing, no more records would be sent.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentCloseIfASendFailsWithDefaultExceptionHandler() {
        final KafkaException exception = new KafkaException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            productionExceptionHandler,
            streamsMetrics
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(StreamsException.class, collector::closeClean);
        assertEquals(exception, thrown.getCause());
        assertThat(
            thrown.getMessage(),
            equalTo("Error encountered sending record to topic topic for task 0_0 due to:" +
                        "\norg.apache.kafka.common.KafkaException: KABOOM!" +
                        "\nException handler choose to FAIL the processing, no more records would be sent.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentSendIfFatalEvenWithContinueExceptionHandler() {
        final KafkaException exception = new AuthenticationException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            new AlwaysContinueProductionExceptionHandler(),
            streamsMetrics
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertEquals(exception, thrown.getCause());
        assertThat(
            thrown.getMessage(),
            equalTo("Error encountered sending record to topic topic for task 0_0 due to:" +
                        "\norg.apache.kafka.common.errors.AuthenticationException: KABOOM!" +
                        "\nWritten offsets would not be recorded and no more records would be sent since this is a fatal error.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentFlushIfFatalEvenWithContinueExceptionHandler() {
        final KafkaException exception = new AuthenticationException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            new AlwaysContinueProductionExceptionHandler(),
            streamsMetrics
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(StreamsException.class, collector::flush);
        assertEquals(exception, thrown.getCause());
        assertThat(
            thrown.getMessage(),
            equalTo("Error encountered sending record to topic topic for task 0_0 due to:" +
                        "\norg.apache.kafka.common.errors.AuthenticationException: KABOOM!" +
                        "\nWritten offsets would not be recorded and no more records would be sent since this is a fatal error.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnSubsequentCloseIfFatalEvenWithContinueExceptionHandler() {
        final KafkaException exception = new AuthenticationException("KABOOM!");
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(exception),
            new AlwaysContinueProductionExceptionHandler(),
            streamsMetrics
        );

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

        final StreamsException thrown = assertThrows(StreamsException.class, collector::closeClean);
        assertEquals(exception, thrown.getCause());
        assertThat(
            thrown.getMessage(),
            equalTo("Error encountered sending record to topic topic for task 0_0 due to:" +
                        "\norg.apache.kafka.common.errors.AuthenticationException: KABOOM!" +
                        "\nWritten offsets would not be recorded and no more records would be sent since this is a fatal error.")
        );
    }

    @Test
    public void shouldNotThrowStreamsExceptionOnSubsequentCallIfASendFailsWithContinueExceptionHandler() {
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            getExceptionalStreamsProducer(new Exception()),
            new AlwaysContinueProductionExceptionHandler(),
            streamsMetrics
        );

        try (final LogCaptureAppender logCaptureAppender =
                 LogCaptureAppender.createAndRegister(RecordCollectorImpl.class)) {

            collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.flush();

            final List<String> messages = logCaptureAppender.getMessages();
            final StringBuilder errorMessage = new StringBuilder("Messages received:");
            for (final String error : messages) {
                errorMessage.append("\n - ").append(error);
            }
            assertTrue(
                errorMessage.toString(),
                messages.get(messages.size() - 1)
                    .endsWith("Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.")
            );
        }

        final Metric metric = streamsMetrics.metrics().get(new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", Thread.currentThread().getName()),
                mkEntry("task-id", taskId.toString())
            )
        ));
        assertEquals(1.0, metric.metricValue());

        collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        collector.flush();
        collector.closeClean();
    }

    @Test
    public void shouldNotAbortTxnOnEOSCloseDirtyIfNothingSent() {
        final AtomicBoolean functionCalled = new AtomicBoolean(false);
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            new StreamsProducer(
                eosConfig,
                "threadId",
                new MockClientSupplier() {
                    @Override
                    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                        return new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                            @Override
                            public void abortTransaction() {
                                functionCalled.set(true);
                            }
                        };
                    }
                },
                taskId,
                null,
                logContext
            ),
            productionExceptionHandler,
            streamsMetrics
        );

        collector.closeDirty();
        assertFalse(functionCalled.get());
    }

    @Test
    public void shouldThrowIfTopicIsUnknownOnSendWithPartitioner() {
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            new StreamsProducer(
                config,
                "threadId",
                new MockClientSupplier() {
                    @Override
                    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                        return new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                            @Override
                            public List<PartitionInfo> partitionsFor(final String topic) {
                                return Collections.emptyList();
                            }
                        };
                    }
                },
                null,
                null,
                logContext
            ),
            productionExceptionHandler,
            streamsMetrics
        );
        collector.initialize();

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> collector.send(topic, "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner)
        );
        assertThat(
            thrown.getMessage(),
            equalTo("Could not get partition information for topic topic for task 0_0." +
                " This can happen if the topic does not exist.")
        );
    }

    @Test
    public void shouldNotCloseInternalProducerForEOS() {
        final RecordCollector collector = new RecordCollectorImpl(
            logContext,
            taskId,
            new StreamsProducer(
                eosConfig,
                "threadId",
                new MockClientSupplier() {
                    @Override
                    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                        return mockProducer;
                    }
                },
                taskId,
                null,
                logContext
            ),
            productionExceptionHandler,
            streamsMetrics
        );

        collector.closeClean();

        // Flush should not throw as producer is still alive.
        streamsProducer.flush();
    }

    @Test
    public void shouldNotCloseInternalProducerForNonEOS() {
        collector.closeClean();

        // Flush should not throw as producer is still alive.
        streamsProducer.flush();
    }

    private StreamsProducer getExceptionalStreamsProducer(final Exception exception) {
        return new StreamsProducer(
            config,
            "threadId",
            new MockClientSupplier() {
                @Override
                public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                    return new MockProducer<byte[], byte[]>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer) {
                        @Override
                        public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
                            callback.onCompletion(null, exception);
                            return null;
                        }
                    };
                }
            },
            null,
            null,
            logContext
        );
    }

    private static class CustomStringSerializer extends StringSerializer {
        private boolean isKey;

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
