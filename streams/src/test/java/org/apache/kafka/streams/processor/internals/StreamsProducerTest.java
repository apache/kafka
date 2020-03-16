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

    private final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer aloStreamsProducer =
        new StreamsProducer(mockProducer, false, logContext, null);

    private final MockProducer<byte[], byte[]> eosMockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer eosStreamsProducer =
        new StreamsProducer(eosMockProducer, true, logContext, "appId");

    private final ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, 0, 0L, new byte[0], new byte[0], new RecordHeaders());

    @Before
    public void before() {
        eosStreamsProducer.initTransaction();
    }

    @Test
    public void shouldFailIfProducerIsNull() {
        {
            final NullPointerException thrown = assertThrows(
                NullPointerException.class,
                () -> new StreamsProducer(null, false, logContext, null)
            );

            assertThat(thrown.getMessage(), is("producer cannot be null"));
        }

        {
            final NullPointerException thrown = assertThrows(
                NullPointerException.class,
                () -> new StreamsProducer(null, true, logContext, "appId")
            );

            assertThat(thrown.getMessage(), is("producer cannot be null"));
        }
    }

    @Test
    public void shouldNotInitTxIfEosDisable() {
        assertThat(mockProducer.transactionInitialized(), is(false));
    }

    @Test
    public void shouldNotBeginTxOnSendIfEosDisable() {
        aloStreamsProducer.send(record, null);
        assertThat(mockProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldForwardRecordOnSend() {
        aloStreamsProducer.send(record, null);
        assertThat(mockProducer.history().size(), is(1));
        assertThat(mockProducer.history().get(0), is(record));
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(producer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, false, logContext, null);

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
            new StreamsProducer(producer, false, logContext, null);

        streamsProducer.flush();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldFailOnInitTxIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            aloStreamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("EOS is disabled [test, alo]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnSendError() {
        mockProducer.sendException  = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> aloStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(mockProducer.sendException));
        assertThat(thrown.getMessage(), is("Error encountered sending record to topic topic [test, alo]"));
        assertThat(thrown.getCause(), is(mockProducer.sendException));
    }

    @Test
    public void shouldFailOnSendFatal() {
        mockProducer.sendException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> aloStreamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldFailOnCommitIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> aloStreamsProducer.commitTransaction(null)
        );

        assertThat(thrown.getMessage(), is("EOS is disabled [test, alo]"));
    }

    @Test
    public void shouldFailOnAbortIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            aloStreamsProducer::abortTransaction
        );

        assertThat(thrown.getMessage(), is("EOS is disabled [test, alo]"));
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
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(offsetsAndMetadata, "appId");
        producer.commitTransaction();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, true, logContext, "appId");
        streamsProducer.initTransaction();

        streamsProducer.commitTransaction(offsetsAndMetadata);

        verify(producer);
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
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, true, logContext, "appId");
        streamsProducer.initTransaction();

        streamsProducer.abortTransaction();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new TimeoutException("KABOOM!");
        final StreamsProducer streamsProducer =
            new StreamsProducer(mockProducer, true, logContext, "appId");

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosInitError() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new KafkaException("KABOOM!");
        final StreamsProducer streamsProducer =
            new StreamsProducer(mockProducer, true, logContext, "appId");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getCause(), is(mockProducer.initTransactionException));
        assertThat(thrown.getMessage(), is("Error encountered while initializing transactions [test, eos]"));
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new RuntimeException("KABOOM!");
        final StreamsProducer streamsProducer =
            new StreamsProducer(mockProducer, true, logContext, "appId");

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
            is("Producer get fenced trying to begin a new transaction [test, eos];" +
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
            is("Producer encounter unexpected error trying to begin a new transaction [test, eos]")
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
            is("Producer cannot send records anymore since it got fenced [test, eos];" +
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
            is("Producer cannot send records anymore since it got fenced [test, eos];" +
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
            is("Producer get fenced trying to commit a transaction [test, eos];" +
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
            is("Producer encounter unexpected error trying to commit a transaction [test, eos]")
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
            is("Producer get fenced trying to commit a transaction [test, eos];" +
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
        assertThat(thrown.getMessage(), is("Timed out while committing a transaction [test, eos]"));
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
            is("Producer encounter unexpected error trying to commit a transaction [test, eos]")
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
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        producer.beginTransaction();
        expect(producer.send(record, null)).andReturn(null);
        producer.abortTransaction();
        expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        replay(producer);

        final StreamsProducer streamsProducer =
            new StreamsProducer(producer, true, logContext, "appId");
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.abortTransaction();

        verify(producer);
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
            is("Producer encounter unexpected error trying to abort a transaction [test, eos]")
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
