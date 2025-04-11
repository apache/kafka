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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
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
        assertThat(eosStreamsProducer.transactionInFlight(), is(true));

        // when:
        eosStreamsProducer.close();

        // then:
        assertThat(eosStreamsProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldResetTransactionInFlightOnReset() {
        // given:
        eosStreamsProducer.send(new ProducerRecord<>("topic", new byte[1]), (metadata, error) -> { });
        assertThat(eosStreamsProducer.transactionInFlight(), is(true));

        // when:
        eosStreamsProducer.resetProducer(null);

        // then:
        assertThat(eosStreamsProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        when(mockedProducer.partitionsFor(topic)).thenReturn(expectedPartitionInfo);

        final List<PartitionInfo> partitionInfo = streamsProducerWithMock.partitionsFor(topic);

        assertThat(partitionInfo, sameInstance(expectedPartitionInfo));
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

        assertThat(thrown.getMessage(), is("processingMode cannot be null"));
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

        assertThat(thrown.getMessage(), is("producer cannot be null"));
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

        assertThat(thrown.getMessage(), is("time cannot be null"));
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

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldFailOnResetProducerForAtLeastOnce() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.resetProducer(null)
        );

        assertThat(thrown.getMessage(), is("Expected EOS to be enabled, but processing mode is at_least_once"));
    }


    // non-EOS tests

    // functional tests

    @Test
    public void shouldNotInitTxIfEosDisable() {
        assertThat(nonEosMockProducer.transactionInitialized(), is(false));
    }

    @Test
    public void shouldNotBeginTxOnSendIfEosDisable() {
        nonEosStreamsProducer.send(record, null);
        assertThat(nonEosMockProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldForwardRecordOnSend() {
        nonEosStreamsProducer.send(record, null);
        assertThat(nonEosMockProducer.history().size(), is(1));
        assertThat(nonEosMockProducer.history().get(0), is(record));
    }

    // error handling tests

    @Test
    public void shouldFailOnInitTxIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("Exactly-once is not enabled [test]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnSendError() {
        nonEosMockProducer.sendException  = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(nonEosMockProducer.sendException));
        assertThat(thrown.getMessage(), is("Error encountered trying to send record to topic topic [test]"));
    }

    @Test
    public void shouldFailOnSendFatal() {
        nonEosMockProducer.sendException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldFailOnCommitIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getMessage(), is("Exactly-once is not enabled [test]"));
    }

    @Test
    public void shouldFailOnAbortIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::abortTransaction
        );

        assertThat(thrown.getMessage(), is("Exactly-once is not enabled [test]"));
    }


    // EOS tests

    // functional tests

    @Test
    public void shouldInitTxOnEos() {
        assertThat(eosMockProducer.transactionInitialized(), is(true));
    }

    @Test
    public void shouldBeginTxOnEosSend() {
        eosStreamsProducer.send(record, null);
        assertThat(eosMockProducer.transactionInFlight(), is(true));
    }

    @Test
    public void shouldContinueTxnSecondEosSend() {
        eosStreamsProducer.send(record, null);
        eosStreamsProducer.send(record, null);
        assertThat(eosMockProducer.transactionInFlight(), is(true));
        assertThat(eosMockProducer.uncommittedRecords().size(), is(2));
    }

    @Test
    public void shouldForwardRecordButNotCommitOnEosSend() {
        eosStreamsProducer.send(record, null);
        assertThat(eosMockProducer.transactionInFlight(), is(true));
        assertThat(eosMockProducer.history().isEmpty(), is(true));
        assertThat(eosMockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosMockProducer.uncommittedRecords().get(0), is(record));
    }

    @Test
    public void shouldBeginTxOnEosCommit() {
        eosStreamsProducerWithMock.initTransaction();
        eosStreamsProducerWithMock.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
        verify(mockedProducer).sendOffsetsToTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        verify(mockedProducer).commitTransaction();
    }

    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        assertThat(eosMockProducer.sentOffsets(), is(true));
    }

    @Test
    public void shouldCommitTxOnEosCommit() {
        eosStreamsProducer.send(record, null);
        assertThat(eosMockProducer.transactionInFlight(), is(true));

        eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        assertThat(eosMockProducer.transactionInFlight(), is(false));
        assertThat(eosMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosMockProducer.history().size(), is(1));
        assertThat(eosMockProducer.history().get(0), is(record));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().size(), is(1));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().get(0).get("appId"), is(offsetsAndMetadata));
    }

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

    @Test
    public void shouldAbortTxOnEosAbort() {
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);
        assertThat(eosMockProducer.transactionInFlight(), is(true));
        assertThat(eosMockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosMockProducer.uncommittedRecords().get(0), is(record));

        eosStreamsProducer.abortTransaction();

        assertThat(eosMockProducer.transactionInFlight(), is(false));
        assertThat(eosMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosMockProducer.history().isEmpty(), is(true));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().isEmpty(), is(true));
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

        assertThat(thrown.getMessage(), is("KABOOM!"));
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

        assertThat(thrown.getMessage(), is("MockProducer hasn't been initialized for transactions."));
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

        assertThat(thrown.getCause(), is(nonEosMockProducer.initTransactionException));
        assertThat(thrown.getMessage(), is("Error encountered trying to initialize transactions [test]"));
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

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnFenced() {
        eosMockProducer.fenceProducer();

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.send(null, null)
        );

        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to begin a new transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnError() {
        eosMockProducer.beginTransactionException = new KafkaException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.send(null, null));

        assertThat(thrown.getCause(), is(eosMockProducer.beginTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to begin a new transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosBeginTxnFatal() {
        eosMockProducer.beginTransactionException = new RuntimeException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosStreamsProducer.send(null, null));

        assertThat(thrown.getMessage(), is("KABOOM!"));
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

        assertThat(thrown.getCause(), is(exception));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to send a record [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
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

        assertThat(thrown.getCause(), is(exception));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to send a record [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
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

    private void testThrowTaskMigrateExceptionOnEosSendOffset(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.sendOffsetsToTransactionException = exception;

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosMockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosSendOffsetError() {
        eosMockProducer.sendOffsetsToTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosMockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosSendOffsetFatal() {
        eosMockProducer.sendOffsetsToTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
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

    private void testThrowTaskMigratedExceptionOnEos(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.commitTransactionException = exception;

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosMockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosMockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosMockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosCommitTxFatal() {
        eosMockProducer.commitTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getMessage(), is("KABOOM!"));
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

        assertThat(thrown.getCause(), is(eosMockProducer.abortTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encounter trying to abort a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosAbortTxFatal() {
        eosMockProducer.abortTransactionException = new RuntimeException("KABOOM!");
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);

        final RuntimeException thrown = assertThrows(RuntimeException.class, eosStreamsProducer::abortTransaction);

        assertThat(thrown.getMessage(), is("KABOOM!"));
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

        assertThat(eosStreamsProducer.kafkaProducer(), is(newProducer));
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
        assertThat(nonEosStreamsProducer.totalBlockedTime(), closeTo(expectedTotalBlocked, 0.01));
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
        assertThat(eosStreamsProducer.totalBlockedTime(), equalTo(expectedTotalBlocked));
        final long closeStart = 1L;
        final long clodeDelay = 1L;
        when(mockTime.nanoseconds()).thenReturn(closeStart).thenReturn(closeStart + clodeDelay);
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

        assertThat(
            eosStreamsProducer.totalBlockedTime(),
            closeTo(2 * expectedTotalBlocked + clodeDelay, 0.01)
        );
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
