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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.test.MockClientSupplier;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StreamsProducerTest {

    private final LogContext logContext = new LogContext("test ");
    private final String topic = "topic";

    private final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mkMap(
        mkEntry(new TopicPartition(topic, 0), new OffsetAndMetadata(0L, null))
    );

    private final MockClientSupplier nonEosMockClientSupplier = new MockClientSupplier();
    private final MockClientSupplier eosMockClientSupplier = new MockClientSupplier();
    private final Map<String, Object> eosProducerConfig = mkMap(mkEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "appId-TX"));

    private StreamsProducer nonEosStreamsProducer;
    private StreamsProducer eosStreamsProducer;
    private MockProducer<byte[], byte[]> nonEosMockProducer;
    private MockProducer<byte[], byte[]> eosMockProducer;

    private final ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, 0, 0L, new byte[0], new byte[0], new RecordHeaders());

    @Before
    public void before() {
        nonEosStreamsProducer =
            new StreamsProducer(nonEosMockClientSupplier, Collections.emptyMap(), null, logContext);
        eosMockClientSupplier.setApplicationIdForProducer("appId");
        eosStreamsProducer =
            new StreamsProducer(eosMockClientSupplier, eosProducerConfig, "appId", logContext);

        nonEosMockProducer = nonEosMockClientSupplier.producers.get(0);
        eosMockProducer = eosMockClientSupplier.producers.get(0);

        eosStreamsProducer.initTransaction();
    }

    // generic tests (non-EOS and EOS)

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final KafkaClientSupplier mockedSupplier = mock(KafkaClientSupplier.class);
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        expect(mockedSupplier.getProducer(Collections.emptyMap())).andReturn(mockedProducer);
        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(mockedProducer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(mockedSupplier, mockedProducer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(mockedSupplier, Collections.emptyMap(), null, logContext);

        final List<PartitionInfo> partitionInfo = streamsProducer.partitionsFor(topic);

        assertThat(partitionInfo, sameInstance(expectedPartitionInfo));
        verify(mockedProducer);
    }

    @Test
    public void shouldForwardCallToFlush() {
        final KafkaClientSupplier mockedSupplier = mock(KafkaClientSupplier.class);
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        expect(mockedSupplier.getProducer(Collections.emptyMap())).andReturn(mockedProducer);
        mockedProducer.flush();
        expectLastCall();
        replay(mockedSupplier, mockedProducer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(mockedSupplier, Collections.emptyMap(), null, logContext);

        streamsProducer.flush();

        verify(mockedProducer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldForwardCallToMetrics() {
        final KafkaClientSupplier mockedSupplier = mock(KafkaClientSupplier.class);
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        expect(mockedSupplier.getProducer(Collections.emptyMap())).andReturn(mockedProducer);
        final Map expected = new HashMap<>();
        expect(mockedProducer.metrics()).andReturn(expected);
        replay(mockedSupplier, mockedProducer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(mockedSupplier, Collections.emptyMap(), null, new LogContext());

        final Map<MetricName, ? extends Metric> metrics = streamsProducer.metrics();

        verify(mockedProducer);

        assertThat(expected, equalTo(metrics));
    }

    @Test
    public void shouldCloseKafkaProducerOnClose() {
        nonEosStreamsProducer.close();
        assertTrue(nonEosMockProducer.closed());

        eosStreamsProducer.close();
        assertTrue(eosMockProducer.closed());
    }

    // error handling tests

    @Test
    public void shouldFailIfClientSupplierIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(null, Collections.emptyMap(), null, logContext)
        );

        assertThat(thrown.getMessage(), is("clientSupplier cannot be null"));
    }

    @Test
    public void shouldFailIfProducerConfigIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(nonEosMockClientSupplier, null, null, logContext)
        );

        assertThat(thrown.getMessage(), is("producerConfig cannot be null"));
    }

    @Test
    public void shouldFailIfLogContextIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(nonEosMockClientSupplier, Collections.emptyMap(), null, null)
        );

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnCloseError() {
        nonEosMockProducer.closeException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            nonEosStreamsProducer::close
        );

        assertThat(thrown.getCause(), is(nonEosMockProducer.closeException));
        assertThat(thrown.getMessage(), is("Error encountered trying to close the producer [test]"));
    }

    @Test
    public void shouldFailOnCloseFatal() {
        nonEosMockProducer.closeException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            nonEosStreamsProducer::close
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
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

        assertThat(thrown.getMessage(), is("EOS is disabled [test]"));
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
    public void shouldFailOnCommitFatalIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.commitTransaction(null)
        );

        assertThat(thrown.getMessage(), is("EOS is disabled [test]"));
    }

    @Test
    public void shouldFailOnAbortFatalIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::abortTransaction
        );

        assertThat(thrown.getMessage(), is("EOS is disabled [test]"));
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
        final KafkaClientSupplier mockedSupplier = mock(KafkaClientSupplier.class);
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        expect(mockedSupplier.getProducer(eosProducerConfig)).andReturn(mockedProducer);
        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        mockedProducer.sendOffsetsToTransaction(offsetsAndMetadata, "appId");
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedSupplier, mockedProducer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(mockedSupplier, eosProducerConfig, "appId", logContext);
        streamsProducer.initTransaction();

        streamsProducer.commitTransaction(offsetsAndMetadata);

        verify(mockedProducer);
    }

    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosStreamsProducer.commitTransaction(offsetsAndMetadata);
        assertThat(eosMockProducer.sentOffsets(), is(true));
    }

    @Test
    public void shouldCommitTxOnEosCommit() {
        eosStreamsProducer.send(record, null);
        assertThat(eosMockProducer.transactionInFlight(), is(true));

        eosStreamsProducer.commitTransaction(offsetsAndMetadata);

        assertThat(eosMockProducer.transactionInFlight(), is(false));
        assertThat(eosMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosMockProducer.history().size(), is(1));
        assertThat(eosMockProducer.history().get(0), is(record));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().size(), is(1));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().get(0).get("appId"), is(offsetsAndMetadata));
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
        final KafkaClientSupplier mockedSupplier = mock(KafkaClientSupplier.class);
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        expect(mockedSupplier.getProducer(eosProducerConfig)).andReturn(mockedProducer);
        mockedProducer.initTransactions();
        expectLastCall();
        replay(mockedSupplier, mockedProducer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(mockedSupplier, eosProducerConfig, "appId", logContext);
        streamsProducer.initTransaction();

        streamsProducer.abortTransaction();

        verify(mockedProducer);
    }

    // error handling tests

    @Test
    public void shouldFailIfApplicationIdIsNull() {
        final IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> new StreamsProducer(eosMockClientSupplier, eosProducerConfig, null, null)
        );

        assertThat(
            thrown.getMessage(),
            is("producerConfig contains `transactional.id`, hence, applicationId cannot be null")
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(eosMockClientSupplier, eosProducerConfig, "appId", logContext);
        eosMockClientSupplier.producers.get(1).initTransactionException = new TimeoutException("KABOOM!");

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosInitError() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(eosMockClientSupplier, eosProducerConfig, "appId", logContext);
        final KafkaException exception = new KafkaException("KABOOM!");
        eosMockClientSupplier.producers.get(1).initTransactionException = exception;

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getCause(), is(exception));
        assertThat(thrown.getMessage(), is("Error encountered trying to initialize transactions [test]"));
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(eosMockClientSupplier, eosProducerConfig, "appId", logContext);
        eosMockClientSupplier.producers.get(1).initTransactionException = new RuntimeException("KABOOM!");

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
    public void shouldThrowTaskMigratedExceptionOnEosSendFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        final ProducerFencedException exception = new ProducerFencedException("KABOOM!");
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
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.sendOffsetsToTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosStreamsProducer.commitTransaction(null)
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
            () -> eosStreamsProducer.commitTransaction(null)
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
            () -> eosStreamsProducer.commitTransaction(null)
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosCommitTxFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.commitTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
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
    public void shouldThrowStreamsExceptionOnEosCommitTxTimeout() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.commitTransactionException = new TimeoutException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
        );

        assertThat(eosMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosMockProducer.commitTransactionException));
        assertThat(thrown.getMessage(), is("Timed out trying to commit a transaction [test]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosMockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
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
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
        );

        assertThat(eosMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxFenced() {
        final KafkaClientSupplier mockedSupplier = mock(KafkaClientSupplier.class);
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        expect(mockedSupplier.getProducer(eosProducerConfig)).andReturn(mockedProducer);
        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.abortTransaction();
        expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        replay(mockedSupplier, mockedProducer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(mockedSupplier, eosProducerConfig, "appId", logContext);
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.abortTransaction();

        verify(mockedProducer);
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
}
