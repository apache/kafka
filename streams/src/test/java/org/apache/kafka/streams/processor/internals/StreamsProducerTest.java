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
import org.apache.kafka.streams.processor.TaskId;
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
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
    private final TaskId taskId = new TaskId(0, 0);
    private final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mkMap(
        mkEntry(new TopicPartition(topic, 0), new OffsetAndMetadata(0L, null))
    );

    private final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer streamsProducer = new StreamsProducer(logContext, mockProducer);

    private final MockProducer<byte[], byte[]> eosMockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer eosStreamsProducer = new StreamsProducer(logContext, eosMockProducer, "appId", taskId);

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
                () -> new StreamsProducer(logContext, null)
            );

            assertThat(thrown.getMessage(), equalTo("producer cannot be null"));
        }

        {
            final NullPointerException thrown = assertThrows(
                NullPointerException.class,
                () -> new StreamsProducer(logContext, null, "appId", taskId)
            );

            assertThat(thrown.getMessage(), equalTo("producer cannot be null"));
        }
    }

    @Test
    public void shouldThrowIfIncorrectlyInitialized() {
        {
            final IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> new StreamsProducer(logContext, mockProducer, null, taskId)
            );
            assertThat(thrown.getMessage(), equalTo("applicationId and taskId must either be both null or both be not null"));
        }

        {
            final IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> new StreamsProducer(logContext, mockProducer, "appId", null)
            );
            assertThat(thrown.getMessage(), equalTo("applicationId and taskId must either be both null or both be not null"));
        }
    }

    // non-eos tests

    // functional tests

    @Test
    public void shouldNotInitTxIfEosDisable() {
        assertFalse(mockProducer.transactionInitialized());
    }

    @Test
    public void shouldNotBeginTxOnSendIfEosDisable() {
        streamsProducer.send(record, null);
        assertFalse(mockProducer.transactionInFlight());
    }

    @Test
    public void shouldForwardRecordOnSend() {
        streamsProducer.send(record, null);
        assertThat(mockProducer.history().size(), equalTo(1));
        assertThat(mockProducer.history().get(0), equalTo(record));
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(producer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(producer);

        final StreamsProducer streamsProducer = new StreamsProducer(logContext, producer);

        final List<PartitionInfo> partitionInfo = streamsProducer.partitionsFor(topic);

        assertSame(expectedPartitionInfo, partitionInfo);
        verify(producer);
    }

    @Test
    public void shouldForwardCallToFlush() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.flush();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer = new StreamsProducer(logContext, producer);

        streamsProducer.flush();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldFailOnInitTxIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), equalTo("EOS is disabled"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnSendError() {
        mockProducer.sendException  = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> streamsProducer.send(record, null)
        );

        assertEquals(mockProducer.sendException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Error encountered sending record to topic topic due to:\norg.apache.kafka.common.KafkaException: KABOOM!"));
    }

    @Test
    public void shouldFailOnSendFatal() {
        mockProducer.sendException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> streamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
    }

    @Test
    public void shouldFailOnCommitIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> streamsProducer.commitTransaction(null)
        );

        assertThat(thrown.getMessage(), equalTo("EOS is disabled"));
    }

    @Test
    public void shouldFailOnAbortIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            streamsProducer::abortTransaction
        );

        assertThat(thrown.getMessage(), equalTo("EOS is disabled"));
    }

    @Test
    public void shouldNotCloseProducerIfEosDisabled() {
        mockProducer.closeException = new KafkaException("KABOOM!");
        streamsProducer.close();

        assertFalse(mockProducer.closed());
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
        assertThat(eosMockProducer.uncommittedRecords().size(), equalTo(2));
    }

    @Test
    public void shouldForwardRecordButNotCommitOnEosSend() {
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());
        assertTrue(eosMockProducer.history().isEmpty());
        assertThat(eosMockProducer.uncommittedRecords().size(), equalTo(1));
        assertThat(eosMockProducer.uncommittedRecords().get(0), equalTo(record));
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

        final StreamsProducer streamsProducer = new StreamsProducer(logContext, producer, "appId", taskId);
        streamsProducer.initTransaction();

        streamsProducer.commitTransaction(offsetsAndMetadata);

        verify(producer);
    }

    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosStreamsProducer.commitTransaction(offsetsAndMetadata);
        assertTrue(eosMockProducer.sentOffsets());
    }

    @Test
    public void shouldCommitTxOnEosCommit() {
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());

        eosStreamsProducer.commitTransaction(offsetsAndMetadata);

        assertFalse(eosMockProducer.transactionInFlight());
        assertTrue(eosMockProducer.uncommittedRecords().isEmpty());
        assertTrue(eosMockProducer.uncommittedOffsets().isEmpty());
        assertThat(eosMockProducer.history().size(), equalTo(1));
        assertThat(eosMockProducer.history().get(0), equalTo(record));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().size(), equalTo(1));
        assertThat(eosMockProducer.consumerGroupOffsetsHistory().get(0).get("appId"), equalTo(offsetsAndMetadata));
    }

    @Test
    public void shouldAbortTxOnEosAbort() {
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);
        assertTrue(eosMockProducer.transactionInFlight());
        assertThat(eosMockProducer.uncommittedRecords().size(), equalTo(1));
        assertThat(eosMockProducer.uncommittedRecords().get(0), equalTo(record));

        eosStreamsProducer.abortTransaction();

        assertFalse(eosMockProducer.transactionInFlight());
        assertTrue(eosMockProducer.uncommittedRecords().isEmpty());
        assertTrue(eosMockProducer.uncommittedOffsets().isEmpty());
        assertTrue(eosMockProducer.history().isEmpty());
        assertTrue(eosMockProducer.consumerGroupOffsetsHistory().isEmpty());
    }

    @Test
    public void shouldSkipAbortTxOnEosAbortIfNotTxInFlight() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);

        producer.initTransactions();
        expectLastCall();
        replay(producer);

        final StreamsProducer streamsProducer = new StreamsProducer(logContext, producer, "appId", taskId);
        streamsProducer.initTransaction();

        streamsProducer.abortTransaction();

        verify(producer);
    }

    // error handling tests

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new TimeoutException("KABOOM!");
        final StreamsProducer streamsProducer = new StreamsProducer(logContext, mockProducer, "appId", taskId);

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosInitError() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new KafkaException("KABOOM!");
        final StreamsProducer streamsProducer = new StreamsProducer(logContext, mockProducer, "appId", taskId);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertEquals(mockProducer.initTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Error encountered while initializing transactions for task 0_0"));
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new RuntimeException("KABOOM!");
        final StreamsProducer streamsProducer = new StreamsProducer(logContext, mockProducer, "appId", taskId);

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnFenced() {
        eosMockProducer.fenceProducer();

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.send(null, null)
        );

        assertThat(thrown.getMessage(), equalTo("Producer get fenced trying to begin a new transaction; it means all tasks belonging to this thread should be migrated."));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnError() {
        eosMockProducer.beginTransactionException = new KafkaException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.send(null, null));

        assertEquals(eosMockProducer.beginTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer encounter unexpected error trying to begin a new transaction for task 0_0"));
    }

    @Test
    public void shouldFailOnEosBeginTxnFatal() {
        eosMockProducer.beginTransactionException = new RuntimeException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosStreamsProducer.send(null, null));

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
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

        assertEquals(exception, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer cannot send records anymore since it got fenced; it means all tasks belonging to this thread should be migrated."));
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
        assertThat(thrown.getMessage(), equalTo("Producer cannot send records anymore since it got fenced; it means all tasks belonging to this thread should be migrated."));
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

        assertEquals(eosMockProducer.sendOffsetsToTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer get fenced trying to commit a transaction; it means all tasks belonging to this thread should be migrated."));
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

        assertEquals(eosMockProducer.sendOffsetsToTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer encounter unexpected error trying to commit a transaction for task 0_0"));
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

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosCommitTxFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.commitTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertEquals(eosMockProducer.commitTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer get fenced trying to commit a transaction; it means all tasks belonging to this thread should be migrated."));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxTimeout() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosMockProducer.commitTransactionException = new TimeoutException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertEquals(eosMockProducer.commitTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Timed out while committing a transaction for task " + taskId));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosMockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertEquals(eosMockProducer.commitTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer encounter unexpected error trying to commit a transaction for task 0_0"));
    }

    @Test
    public void shouldFailOnEosCommitTxFatal() {
        eosMockProducer.commitTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosStreamsProducer.commitTransaction(offsetsAndMetadata)
        );

        assertTrue(eosMockProducer.sentOffsets());
        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
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

        final StreamsProducer streamsProducer = new StreamsProducer(logContext, producer, "appId", taskId);
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

        assertEquals(eosMockProducer.abortTransactionException, thrown.getCause());
        assertThat(thrown.getMessage(), equalTo("Producer encounter unexpected error trying to abort a transaction for task 0_0"));
    }

    @Test
    public void shouldFailOnEosAbortTxFatal() {
        eosMockProducer.abortTransactionException = new RuntimeException("KABOOM!");
        // call `send()` to start a transaction
        eosStreamsProducer.send(record, null);

        final RuntimeException thrown = assertThrows(RuntimeException.class, eosStreamsProducer::abortTransaction);

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
    }

    @Test
    public void shouldFailOnCloseFatal() {
        eosMockProducer.closeException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            eosStreamsProducer::close
        );

        assertThat(thrown.getMessage(), equalTo("KABOOM!"));
    }

    @Test
    public void shouldCloseProducerIfEosEnabled() {
        eosStreamsProducer.close();

        final RuntimeException thrown = assertThrows(IllegalStateException.class, () -> eosStreamsProducer.send(record, null));

        assertThat(thrown.getMessage(), equalTo("MockProducer is already closed."));
    }
}
