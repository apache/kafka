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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StreamsProducerTest {
    private static final double BUFFER_POOL_WAIT_TIME = 1;
    private static final double FLUSH_TME = 2;
    private static final double TXN_INIT_TIME = 3;
    private static final double TXN_BEGIN_TIME = 4;
    private static final double TXN_SEND_OFFSETS_TIME = 5;
    private static final double TXN_COMMIT_TIME = 6;
    private static final double TXN_ABORT_TIME = 7;
    private static final double METADATA_WAIT_TIME = 8;

    private final LogContext logContext = new LogContext("test ");
    private final String topic = "topic";
    private final Cluster cluster = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        Collections.singletonList(new PartitionInfo(topic, 0, Node.noNode(), new Node[0], new Node[0])),
        Collections.emptySet(),
        Collections.emptySet()
    );

    private final Time mockTime = mock(Time.class);

    @SuppressWarnings("unchecked")
    final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);
    private final StreamsProducer streamsProducerWithMock = new StreamsProducer(
        mockedProducer,
        AT_LEAST_ONCE,
        mockTime,
        logContext
    );
    private final StreamsProducer eosStreamsProducerWithMock = new StreamsProducer(
        mockedProducer,
        EXACTLY_ONCE_V2,
        mockTime,
        logContext
    );

    private final MockProducer<byte[], byte[]> nonEosMockProducer
        = new MockProducer<>(cluster, true, new org.apache.kafka.clients.producer.RoundRobinPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());
    private final MockProducer<byte[], byte[]> eosMockProducer
        = new MockProducer<>(cluster, true, new org.apache.kafka.clients.producer.RoundRobinPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());

    private StreamsProducer nonEosStreamsProducer;
    private StreamsProducer eosStreamsProducer;


    private final ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, 0, 0L, new byte[0], new byte[0], new RecordHeaders());

    private final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mkMap(
        mkEntry(new TopicPartition(topic, 0), new OffsetAndMetadata(0L, null))
    );

    @BeforeEach
    public void before() {
        nonEosStreamsProducer =
            new StreamsProducer(
                nonEosMockProducer,
                AT_LEAST_ONCE,
                mockTime,
                logContext
            );

        eosStreamsProducer =
            new StreamsProducer(
                eosMockProducer,
                EXACTLY_ONCE_V2,
                mockTime,
                logContext
            );
        eosStreamsProducer.initTransaction();
        when(mockTime.nanoseconds()).thenReturn(Time.SYSTEM.nanoseconds());
    }



    // common tests (non-EOS and EOS)

    // functional tests

    @Test
    public void shouldResetTransactionInFlightOnClose() {
        // given:
        eosStreamsProducer.send(
            new ProducerRecord<>("topic", new byte[1]), (metadata, error) -> { });
        assertTrue(eosStreamsProducer.transactionInFlight());

        // when:
        eosStreamsProducer.close();

        // then:
        assertFalse(eosStreamsProducer.transactionInFlight());
    }

    @Test
    public void shouldResetTransactionInFlightOnReset() {
        // given:
        eosStreamsProducer.send(new ProducerRecord<>("topic", new byte[1]), (metadata, error) -> { });
        assertTrue(eosStreamsProducer.transactionInFlight());

        // when:
        eosStreamsProducer.resetProducer(null);

        // then:
        assertFalse(eosStreamsProducer.transactionInFlight());
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        when(mockedProducer.partitionsFor(topic)).thenReturn(expectedPartitionInfo);

        final List<PartitionInfo> partitionInfo = streamsProducerWithMock.partitionsFor(topic);

        assertSame(expectedPartitionInfo, partitionInfo);
    }

    @Test
    public void shouldForwardCallToFlush() {
        streamsProducerWithMock.flush();
        verify(mockedProducer).flush();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldForwardCallToMetrics() {
        final Map metrics = new HashMap<>();
        when(mockedProducer.metrics()).thenReturn(metrics);

        assertSame(metrics, streamsProducerWithMock.metrics());
    }

    @Test
    public void shouldForwardCallToClose() {
        streamsProducerWithMock.close();
        verify(mockedProducer).close();
    }

    // error handling tests

    @Test
    public void shouldFailIfProcessingModeIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                mockedProducer,
                null,
                mockTime,
                logContext
            )
        );

        assertEquals("processingMode cannot be null", thrown.getMessage());
    }

    @Test
    public void shouldFailIfProducerIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                null,
                AT_LEAST_ONCE,
                mockTime,
                logContext
            )
        );

        assertEquals("producer cannot be null", thrown.getMessage());
    }

    @Test
    public void shouldFailIfTimeIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                mockedProducer,
                AT_LEAST_ONCE,
                null,
                logContext
            )
        );

        assertEquals("time cannot be null", thrown.getMessage());
    }

    @Test
    public void shouldFailIfLogContextIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                mockedProducer,
                AT_LEAST_ONCE,
                mockTime,
                null
            )
        );

        assertEquals("logContext cannot be null", thrown.getMessage());
    }

    @Test
    public void shouldFailOnResetProducerForAtLeastOnce() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.resetProducer(null)
        );

        assertEquals("Expected EOS to be enabled, but processing mode is at_least_once", thrown.getMessage());
    }


    // non-EOS tests

    // functional tests

    @Test
    public void shouldNotInitTxIfEosDisable() {
        assertFalse(nonEosMockProducer.transactionInitialized());
    }

    @Test
    public void shouldNotBeginTxOnSendIfEosDisable() {
        nonEosStreamsProducer.send(record, null);
        assertFalse(nonEosMockProducer.transactionInFlight());
    }

    @Test
    public void shouldForwardRecordOnSend() {
        nonEosStreamsProducer.send(record, null);
        assertEquals(1, nonEosMockProducer.history().size());
        assertEquals(record, nonEosMockProducer.history().get(0));
    }

    // error handling tests

    @Test
    public void shouldFailOnInitTxIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::initTransaction
        );

        assertEquals("Exactly-once is not enabled [test]", thrown.getMessage());
    }

    @Test
    public void shouldThrowStreamsExceptionOnSendError() {
        nonEosMockProducer.sendException  = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertEquals(nonEosMockProducer.sendException, thrown.getCause());
        assertEquals("Error encountered trying to send record to topic topic [test]", thrown.getMessage());
    }

    @Test
    public void shouldFailOnSendFatal() {
        nonEosMockProducer.sendException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertEquals("KABOOM!", thrown.getMessage());
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldFailOnCommitIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertEquals("Exactly-once is not enabled [test]", thrown.getMessage());
    }

    @Test
    public void shouldFailOnAbortIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::abortTransaction
        );

        assertEquals("Exactly-once is not enabled [test]", thrown.getMessage());
    }


    // EOS tests

    // functional tests

    @Test
    public void shouldInitTxOnEos() {
        assertTrue(eosMockProducer.transactionInitialized());
    }

    @Test
    public void shouldBeginTxOnEosSend() {
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());
    }

    @Test
    public void shouldContinueTxnSecondEosSend() {
        eosStreamsProducer.send(record, null);
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());
        assertEquals(2, eosMockProducer.uncommittedRecords().size());
    }

    @Test
    public void shouldForwardRecordButNotCommitOnEosSend() {
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());
        assertTrue(eosMockProducer.history().isEmpty());
        assertEquals(1, eosMockProducer.uncommittedRecords().size());
        assertEquals(record, eosMockProducer.uncommittedRecords().get(0));
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldBeginTxOnEosCommit() {
        eosStreamsProducerWithMock.initTransaction();
        eosStreamsProducerWithMock.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
        verify(mockedProducer).sendOffsetsToTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        verify(mockedProducer).commitTransaction();
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        assertTrue(eosMockProducer.sentOffsets());
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldCommitTxOnEosCommit() {
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());

        eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        assertFalse(eosMockProducer.transactionInFlight());
        assertTrue(eosMockProducer.uncommittedRecords().isEmpty());
        assertTrue(eosMockProducer.uncommittedOffsets().isEmpty());
        assertEquals(1, eosMockProducer.history().size());
        assertEquals(record, eosMockProducer.history().get(0));
        assertEquals(1, eosMockProducer.consumerGroupOffsetsHistory().size());
        assertEquals(offsetsAndMetadata, eosMockProducer.consumerGroupOffsetsHistory().get(0).get("appId"));
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldCommitTxWithConsumerGroupMetadataOnEosCommit() {
        when(mockedProducer.send(record, null)).thenReturn(null);

        final StreamsProducer streamsProducer = new StreamsProducer(
            mockedProducer,
            EXACTLY_ONCE_V2,
            mockTime,
            logContext
        );
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);
        streamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
        verify(mockedProducer).sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        verify(mockedProducer).commitTransaction();
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldAbortTxOnEosAbort() {
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());
        assertEquals(1, eosMockProducer.uncommittedRecords().size());
        assertEquals(record, eosMockProducer.uncommittedRecords().get(0));

        eosStreamsProducer.abortTransaction();

        assertFalse(eosMockProducer.transactionInFlight());
        assertTrue(eosMockProducer.uncommittedRecords().isEmpty());
        assertTrue(eosMockProducer.uncommittedOffsets().isEmpty());
        assertTrue(eosMockProducer.history().isEmpty());
        assertTrue(eosMockProducer.consumerGroupOffsetsHistory().isEmpty());
    }

    @Test
    public void shouldSkipAbortTxOnEosAbortIfNotTxInFlight() {
        eosStreamsProducerWithMock.initTransaction();
        eosStreamsProducerWithMock.abortTransaction();

        verify(mockedProducer).initTransactions();
    }

    // error handling tests

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new TimeoutException("KABOOM!");

        final StreamsProducer streamsProducer = new StreamsProducer(
            nonEosMockProducer,
            EXACTLY_ONCE_V2,
            mockTime,
            logContext
        );

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertEquals("KABOOM!", thrown.getMessage());
    }

    @Test
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForEos() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid auto-init Tx
        final StreamsProducer streamsProducer =
            new StreamsProducer(
                nonEosMockProducer,
                EXACTLY_ONCE_V2,
                mockTime,
                logContext
            );

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> streamsProducer.send(record, null)
        );

        assertEquals("MockProducer hasn't been initialized for transactions.", thrown.getMessage());
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosInitError() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new KafkaException("KABOOM!");

        final StreamsProducer streamsProducer = new StreamsProducer(
            nonEosMockProducer,
            EXACTLY_ONCE_V2,
            mockTime,
            logContext
        );

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertEquals(nonEosMockProducer.initTransactionException, thrown.getCause());
        assertEquals("Error encountered trying to initialize transactions [test]", thrown.getMessage());
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new RuntimeException("KABOOM!");

        final StreamsProducer streamsProducer = new StreamsProducer(
            nonEosMockProducer,
            EXACTLY_ONCE_V2,
            mockTime,
            logContext
        );

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            streamsProducer::initTransaction
        );

        assertEquals("KABOOM!", thrown.getMessage());
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnFenced() {
        eosMockProducer.fenceProducer();

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.send(null, null)
        );

        assertEquals(
            "Producer got fenced trying to begin a new transaction [test];" +
                " it means all tasks belonging to this thread should be migrated.",
            thrown.getMessage());
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnError() {
        eosMockProducer.beginTransactionException = new KafkaException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.send(null, null));

        assertEquals(eosMockProducer.beginTransactionException, thrown.getCause());
        assertEquals("Error encountered trying to begin a new transaction [test]", thrown.getMessage());
    }

    @Test
    public void shouldFailOnEosBeginTxnFatal() {
        eosMockProducer.beginTransactionException = new RuntimeException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosStreamsProducer.send(null, null));

        assertEquals("KABOOM!", thrown.getMessage());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendProducerFenced() {
        testThrowTaskMigratedExceptionOnEosSend(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendPInvalidPidMapping() {
        testThrowTaskMigratedExceptionOnEosSend(new InvalidPidMappingException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendInvalidEpoch() {
        testThrowTaskMigratedExceptionOnEosSend(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnEosSend(final RuntimeException exception) {
        // we need to mimic that `send()` always wraps error in a KafkaException
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.send(record, null)
        );

        assertEquals(exception, thrown.getCause());
        assertEquals(
            "Producer got fenced trying to send a record [test];" +
                " it means all tasks belonging to this thread should be migrated.",
            thrown.getMessage());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendUnknownPid() {
        final UnknownProducerIdException exception = new UnknownProducerIdException("KABOOM!");
        // we need to mimic that `send()` always wraps error in a KafkaException
        eosMockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.send(record, null)
        );

        assertEquals(exception, thrown.getCause());
        assertEquals(
            "Producer got fenced trying to send a record [test];" +
                " it means all tasks belonging to this thread should be migrated.",
            thrown.getMessage());
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetProducerFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        testThrowTaskMigrateExceptionOnEosSendOffset(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetInvalidPidMapping() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        testThrowTaskMigrateExceptionOnEosSendOffset(new InvalidPidMappingException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetInvalidEpoch() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        testThrowTaskMigrateExceptionOnEosSendOffset(new InvalidProducerEpochException("KABOOM!"));
    }

    @SuppressWarnings("removal")
    private void testThrowTaskMigrateExceptionOnEosSendOffset(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.sendOffsetsToTransactionException = exception;

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertEquals(eosMockProducer.sendOffsetsToTransactionException, thrown.getCause());
        assertEquals(
            "Producer got fenced trying to add offsets to a transaction [test];" +
                " it means all tasks belonging to this thread should be migrated.",
            thrown.getMessage());
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldThrowStreamsExceptionOnEosSendOffsetError() {
        eosMockProducer.sendOffsetsToTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertEquals(eosMockProducer.sendOffsetsToTransactionException, thrown.getCause());
        assertEquals("Error encountered trying to add offsets to a transaction [test]", thrown.getMessage());
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldFailOnEosSendOffsetFatal() {
        eosMockProducer.sendOffsetsToTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertEquals("KABOOM!", thrown.getMessage());
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithProducerFenced() {
        testThrowTaskMigratedExceptionOnEos(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithInvalidPidMapping() {
        testThrowTaskMigratedExceptionOnEos(new InvalidPidMappingException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithInvalidEpoch() {
        testThrowTaskMigratedExceptionOnEos(new InvalidProducerEpochException("KABOOM!"));
    }

    @SuppressWarnings("removal")
    private void testThrowTaskMigratedExceptionOnEos(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.commitTransactionException = exception;

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertEquals(eosMockProducer.commitTransactionException, thrown.getCause());
        assertEquals(
            "Producer got fenced trying to commit a transaction [test];" +
                " it means all tasks belonging to this thread should be migrated.",
            thrown.getMessage());
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosMockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertEquals(eosMockProducer.commitTransactionException, thrown.getCause());
        assertEquals("Error encountered trying to commit a transaction [test]", thrown.getMessage());
    }

    @SuppressWarnings("removal")
    @Test
    public void shouldFailOnEosCommitTxFatal() {
        eosMockProducer.commitTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertEquals("KABOOM!", thrown.getMessage());
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxProducerFenced() {
        testSwallowExceptionOnEosAbortTx(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxInvalidPidMapping() {
        testSwallowExceptionOnEosAbortTx(new InvalidPidMappingException("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxInvalidEpoch() {
        testSwallowExceptionOnEosAbortTx(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testSwallowExceptionOnEosAbortTx(final RuntimeException exception) {
        when(mockedProducer.send(record, null)).thenReturn(null);
        doThrow(exception).when(mockedProducer).abortTransaction();

        eosStreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosStreamsProducerWithMock.send(record, null);
        eosStreamsProducerWithMock.abortTransaction();

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosAbortTxError() {
        eosMockProducer.abortTransactionException = new KafkaException("KABOOM!");
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);

        final StreamsException thrown = assertThrows(StreamsException.class, eosStreamsProducer::abortTransaction);

        assertEquals(eosMockProducer.abortTransactionException, thrown.getCause());
        assertEquals("Error encounter trying to abort a transaction [test]", thrown.getMessage());
    }

    @Test
    public void shouldFailOnEosAbortTxFatal() {
        eosMockProducer.abortTransactionException = new RuntimeException("KABOOM!");
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);

        final RuntimeException thrown = assertThrows(RuntimeException.class, eosStreamsProducer::abortTransaction);

        assertEquals("KABOOM!", thrown.getMessage());
    }


    // EOS test

    // functional tests

    @Test
    public void shouldCloseExistingProducerOnResetProducer() {
        eosStreamsProducer.resetProducer(null);

        assertTrue(eosMockProducer.closed());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetNewProducerOnResetProducer() {
        final Producer<byte[], byte[]> newProducer = mock(Producer.class);
        eosStreamsProducer.resetProducer(newProducer);

        assertEquals(newProducer, eosStreamsProducer.kafkaProducer());
    }

    @Test
    public void shouldResetTransactionInitializedOnResetProducer() {
        final StreamsProducer streamsProducer = new StreamsProducer(
            mockedProducer,
            EXACTLY_ONCE_V2,
            mockTime,
            logContext
        );
        streamsProducer.initTransaction();

        when(mockedProducer.metrics()).thenReturn(Collections.emptyMap());

        streamsProducer.resetProducer(mockedProducer);
        streamsProducer.initTransaction();

        verify(mockedProducer).close();

        // streamsProducer.resetProducer() should reset 'transactionInitialized' field so that subsequent call of the
        // streamsProducer.initTransactions() method can start new transaction.
        // Therefore, mockedProducer.initTransactions() is expected to be called twice.
        verify(mockedProducer, times(2)).initTransactions();
    }

    @Test
    public void shouldComputeTotalBlockedTime() {
        setProducerMetrics(
            nonEosMockProducer,
            BUFFER_POOL_WAIT_TIME,
            FLUSH_TME,
            TXN_INIT_TIME,
            TXN_BEGIN_TIME,
            TXN_SEND_OFFSETS_TIME,
            TXN_COMMIT_TIME,
            TXN_ABORT_TIME,
            METADATA_WAIT_TIME
        );

        final double expectedTotalBlocked = BUFFER_POOL_WAIT_TIME + FLUSH_TME + TXN_INIT_TIME +
            TXN_BEGIN_TIME + TXN_SEND_OFFSETS_TIME +  TXN_COMMIT_TIME + TXN_ABORT_TIME +
            METADATA_WAIT_TIME;
        assertEquals(expectedTotalBlocked, nonEosStreamsProducer.totalBlockedTime(), 0.01);
    }

    @Test
    public void shouldComputeTotalBlockedTimeAfterReset() {
        setProducerMetrics(
            eosMockProducer,
            BUFFER_POOL_WAIT_TIME,
            FLUSH_TME,
            TXN_INIT_TIME,
            TXN_BEGIN_TIME,
            TXN_SEND_OFFSETS_TIME,
            TXN_COMMIT_TIME,
            TXN_ABORT_TIME,
            METADATA_WAIT_TIME
        );
        final double expectedTotalBlocked = BUFFER_POOL_WAIT_TIME + FLUSH_TME + TXN_INIT_TIME +
            TXN_BEGIN_TIME + TXN_SEND_OFFSETS_TIME +  TXN_COMMIT_TIME + TXN_ABORT_TIME +
            METADATA_WAIT_TIME;
        assertEquals(expectedTotalBlocked, eosStreamsProducer.totalBlockedTime());
        final long closeStart = 1L;
        final long closeDelay = 1L;
        when(mockTime.nanoseconds()).thenReturn(closeStart).thenReturn(closeStart + closeDelay);
        eosStreamsProducer.resetProducer(eosMockProducer);
        setProducerMetrics(
            eosMockProducer,
            BUFFER_POOL_WAIT_TIME,
            FLUSH_TME,
            TXN_INIT_TIME,
            TXN_BEGIN_TIME,
            TXN_SEND_OFFSETS_TIME,
            TXN_COMMIT_TIME,
            TXN_ABORT_TIME,
            METADATA_WAIT_TIME
        );

        assertEquals(2 * expectedTotalBlocked + closeDelay, eosStreamsProducer.totalBlockedTime(), 0.01);
    }

    private MetricName metricName(final String name) {
        return new MetricName(name, "", "", Collections.emptyMap());
    }

    private void addMetric(
        final MockProducer<?, ?> producer,
        final String name,
        final double value) {
        final MetricName metricName = metricName(name);
        producer.setMockMetrics(metricName, new Metric() {
            @Override
            public MetricName metricName() {
                return metricName;
            }

            @Override
            public Object metricValue() {
                return value;
            }
        });
    }

    private void setProducerMetrics(
        final MockProducer<?, ?> producer,
        final double bufferPoolWaitTime,
        final double flushTime,
        final double txnInitTime,
        final double txnBeginTime,
        final double txnSendOffsetsTime,
        final double txnCommitTime,
        final double txnAbortTime,
        final double metadataWaitTime) {
        addMetric(producer, "bufferpool-wait-time-ns-total", bufferPoolWaitTime);
        addMetric(producer, "flush-time-ns-total", flushTime);
        addMetric(producer, "txn-init-time-ns-total", txnInitTime);
        addMetric(producer, "txn-begin-time-ns-total", txnBeginTime);
        addMetric(producer, "txn-send-offsets-time-ns-total", txnSendOffsetsTime);
        addMetric(producer, "txn-commit-time-ns-total", txnCommitTime);
        addMetric(producer, "txn-abort-time-ns-total", txnAbortTime);
        addMetric(producer, "metadata-wait-time-ns-total", metadataWaitTime);
    }
}
