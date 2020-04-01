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
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.StreamThread.ProcessingMode.AT_LEAST_ONCE;
import static org.apache.kafka.streams.processor.internals.StreamThread.ProcessingMode.EXACTLY_ONCE_ALPHA;
import static org.apache.kafka.streams.processor.internals.StreamThread.ProcessingMode.EXACTLY_ONCE_BETA;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

public class StreamsProducerTest {

    private final LogContext logContext = new LogContext("test ");
    private final String topic = "topic";
    private final Cluster cluster = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        Collections.singletonList(new PartitionInfo(topic, 0, Node.noNode(), new Node[0], new Node[0])),
        Collections.emptySet(),
        Collections.emptySet()
    );

    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mkMap(
        mkEntry(new TopicPartition(topic, 0), new OffsetAndMetadata(0L, null))
    );

    private final MockProducer<byte[], byte[]> nonEosMockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer nonEosStreamsProducer =
        new StreamsProducer(nonEosMockProducer, AT_LEAST_ONCE, logContext);

    private final MockProducer<byte[], byte[]> eosAlphaMockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer eosAlphaStreamsProducer =
        new StreamsProducer(eosAlphaMockProducer, EXACTLY_ONCE_ALPHA, logContext);

    private final MockProducer<byte[], byte[]> eosBetaMockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer eosBetaStreamsProducer =
        new StreamsProducer(eosBetaMockProducer, EXACTLY_ONCE_BETA, logContext);

    private final ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, 0, 0L, new byte[0], new byte[0], new RecordHeaders());

    @Before
    public void before() {
        eosAlphaStreamsProducer.initTransaction();
    }



    // common tests (non-EOS and EOS-alpha/beta)

    // functional tests

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(producer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, AT_LEAST_ONCE, logContext);

        final List<PartitionInfo> partitionInfo = streamsProducer.partitionsFor(topic);

        assertThat(partitionInfo, sameInstance(expectedPartitionInfo));
        verify(producer);
    }

    @Test
    public void shouldForwardCallToFlush() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.flush();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, AT_LEAST_ONCE, logContext);

        streamsProducer.flush();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldFailIfProducerIsNullForAtLeastOnce() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(null, AT_LEAST_ONCE, logContext)
        );

        assertThat(thrown.getMessage(), is("producer cannot be null"));
    }

    @Test
    public void shouldFailIfProcessingModeIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(nonEosMockProducer, null, logContext)
        );

        assertThat(thrown.getMessage(), is("processingMode cannot be null"));
    }

    @Test
    public void shouldFailIfProducerIsNullForExactlyOnceAlpha() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(null, EXACTLY_ONCE_ALPHA, logContext)
        );

        assertThat(thrown.getMessage(), is("producer cannot be null"));
    }

    @Test
    public void shouldFailIfProducerIsNullForExactlyOnceBeta() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(null, EXACTLY_ONCE_BETA, logContext)
        );

        assertThat(thrown.getMessage(), is("producer cannot be null"));
    }

    @Test
    public void shouldFailIfLogContextIsNullForAtLeastOnce() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(nonEosMockProducer, AT_LEAST_ONCE, null)
        );

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldFailIfLogContextIsNullForExactlyOnceAlpha() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_ALPHA, null)
        );

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldFailIfLogContextIsNullForExactlyOnceBeta() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_BETA, null)
        );

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldFailOnResetProducerForAtLeastOnce() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.resetProducer(nonEosMockProducer)
        );

        assertThat(thrown.getMessage(), is("Exactly-once beta is not enabled [test]"));
    }

    @Test
    public void shouldFailOnResetProducerForExactlyOnceAlpha() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> eosAlphaStreamsProducer.resetProducer(eosAlphaMockProducer)
        );

        assertThat(thrown.getMessage(), is("Exactly-once beta is not enabled [test]"));
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


    // EOS tests (alpha and beta)

    // functional tests

    @Test
    public void shouldEnableEosIfEosAlphaEnabled() {
        assertThat(eosAlphaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldEnableEosIfEosBetaEnabled() {
        assertThat(eosBetaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldInitTxOnEos() {
        assertThat(eosAlphaMockProducer.transactionInitialized(), is(true));
    }

    @Test
    public void shouldBeginTxOnEosSend() {
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
    }

    @Test
    public void shouldContinueTxnSecondEosSend() {
        eosAlphaStreamsProducer.send(record, null);
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedRecords().size(), is(2));
    }

    @Test
    public void shouldForwardRecordButNotCommitOnEosSend() {
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
        assertThat(eosAlphaMockProducer.history().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosAlphaMockProducer.uncommittedRecords().get(0), is(record));
    }

    @Test
    public void shouldBeginTxOnEosCommit() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        producer.commitTransaction();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, EXACTLY_ONCE_ALPHA, logContext);
        streamsProducer.initTransaction();

        streamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        verify(producer);
    }

    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
    }

    @Test
    public void shouldCommitTxOnEosCommit() {
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));

        eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        assertThat(eosAlphaMockProducer.transactionInFlight(), is(false));
        assertThat(eosAlphaMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.history().size(), is(1));
        assertThat(eosAlphaMockProducer.history().get(0), is(record));
        assertThat(eosAlphaMockProducer.consumerGroupOffsetsHistory().size(), is(1));
        assertThat(eosAlphaMockProducer.consumerGroupOffsetsHistory().get(0).get("appId"), is(offsetsAndMetadata));
    }

    @Test
    public void shouldCommitTxWithApplicationIdOnEosAlphaCommit() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        expectLastCall();
        producer.beginTransaction();
        expectLastCall();
        expect(producer.send(record, null)).andReturn(null);
        producer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        producer.commitTransaction();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, EXACTLY_ONCE_ALPHA, logContext);
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(producer);
    }

    @Test
    public void shouldCommitTxWithConsumerGroupMetadataOnEosBetaCommit() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        expectLastCall();
        producer.beginTransaction();
        expectLastCall();
        expect(producer.send(record, null)).andReturn(null);
        producer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        producer.commitTransaction();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, EXACTLY_ONCE_ALPHA, logContext);
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(producer);
    }

    @Test
    public void shouldAbortTxOnEosAbort() {
        // call `send()` to start a transaction
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosAlphaMockProducer.uncommittedRecords().get(0), is(record));

        eosAlphaStreamsProducer.abortTransaction();

        assertThat(eosAlphaMockProducer.transactionInFlight(), is(false));
        assertThat(eosAlphaMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.history().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.consumerGroupOffsetsHistory().isEmpty(), is(true));
    }

    @Test
    public void shouldSkipAbortTxOnEosAbortIfNotTxInFlight() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, EXACTLY_ONCE_ALPHA, logContext);
        streamsProducer.initTransaction();

        streamsProducer.abortTransaction();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new TimeoutException("KABOOM!");
        final StreamsProducer streamsProducer =
            new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_ALPHA, logContext);

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceAlpha() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_ALPHA, logContext);

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> streamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("MockProducer hasn't been initialized for transactions."));
    }

    @Test
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceBeta() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_BETA, logContext);

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> streamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("MockProducer hasn't been initialized for transactions."));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosInitError() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new KafkaException("KABOOM!");
        final StreamsProducer streamsProducer =
            new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_ALPHA, logContext);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getCause(), is(nonEosMockProducer.initTransactionException));
        assertThat(thrown.getMessage(), is("Error encountered trying to initialize transactions [test]"));
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new RuntimeException("KABOOM!");
        final StreamsProducer streamsProducer =
            new StreamsProducer(nonEosMockProducer, EXACTLY_ONCE_ALPHA, logContext);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnFenced() {
        eosAlphaMockProducer.fenceProducer();

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.send(null, null)
        );

        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to begin a new transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnError() {
        eosAlphaMockProducer.beginTransactionException = new KafkaException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosAlphaStreamsProducer.send(null, null));

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.beginTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to begin a new transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosBeginTxnFatal() {
        eosAlphaMockProducer.beginTransactionException = new RuntimeException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosAlphaStreamsProducer.send(null, null));

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        final ProducerFencedException exception = new ProducerFencedException("KABOOM!");
        // we need to mimic that `send()` always wraps error in a KafkaException
        eosAlphaMockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.send(record, null)
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
        eosAlphaMockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(exception));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to send a record [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.sendOffsetsToTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosAlphaStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosSendOffsetError() {
        eosAlphaMockProducer.sendOffsetsToTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosAlphaStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosSendOffsetFatal() {
        eosAlphaMockProducer.sendOffsetsToTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosAlphaStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosCommitTxFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.commitTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosAlphaMockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxTimeout() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.commitTransactionException = new TimeoutException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosAlphaMockProducer.commitTransactionException));
        assertThat(thrown.getMessage(), is("Timed out trying to commit a transaction [test]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosAlphaMockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosAlphaMockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosCommitTxFatal() {
        eosAlphaMockProducer.commitTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxFenced() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        producer.beginTransaction();
        expect(producer.send(record, null)).andReturn(null);
        producer.abortTransaction();
        expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, EXACTLY_ONCE_ALPHA, logContext);
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.abortTransaction();

        verify(producer);
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosAbortTxError() {
        eosAlphaMockProducer.abortTransactionException = new KafkaException("KABOOM!");
        // call `send()` to start a transaction
        eosAlphaStreamsProducer.send(record, null);

        final StreamsException thrown = assertThrows(StreamsException.class, eosAlphaStreamsProducer::abortTransaction);

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.abortTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encounter trying to abort a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosAbortTxFatal() {
        eosAlphaMockProducer.abortTransactionException = new RuntimeException("KABOOM!");
        // call `send()` to start a transaction
        eosAlphaStreamsProducer.send(record, null);

        final RuntimeException thrown = assertThrows(RuntimeException.class, eosAlphaStreamsProducer::abortTransaction);

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }


    // EOS beta test

    // functional tests

    @Test
    public void shouldSetSetNewProducerOnResetProducer() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        eosBetaStreamsProducer.resetProducer(producer);

        assertThat(eosBetaStreamsProducer.kafkaProducer(), is(producer));
    }

    @Test
    public void shouldResetTransactionInitializedOnResetProducer() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        eosBetaStreamsProducer.initTransaction();
        eosBetaStreamsProducer.resetProducer(producer);

        producer.initTransactions();
        expectLastCall();
        replay(producer);

        eosBetaStreamsProducer.initTransaction();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldFailIfProducerIsNullOnReInitializeProducer() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> eosBetaStreamsProducer.resetProducer(null)
        );

        assertThat(thrown.getMessage(), is("producer cannot be null"));
    }

}
