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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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
    private final StreamsProducer nonEosStreamsProducer = new StreamsProducer(mockProducer, null, logContext);

    private final MockProducer<byte[], byte[]> eosMockProducer = new MockProducer<>(
        cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer);
    private final StreamsProducer eosStreamsProducer = new StreamsProducer(eosMockProducer, "appId", logContext);

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
                () -> new StreamsProducer(null, null, logContext)
            );

            assertThat(thrown.getMessage(), is("producer cannot be null"));
        }

        {
            final NullPointerException thrown = assertThrows(
                NullPointerException.class,
                () -> new StreamsProducer(null, "appId", logContext)
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
        nonEosStreamsProducer.send(record, null);
        assertThat(mockProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldForwardRecordOnSend() {
        nonEosStreamsProducer.send(record, null);
        assertThat(mockProducer.history().size(), is(1));
        assertThat(mockProducer.history().get(0), is(record));
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(mockedProducer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, null, logContext);

        final List<PartitionInfo> partitionInfo = streamsProducer.partitionsFor(topic);

        assertThat(partitionInfo, sameInstance(expectedPartitionInfo));
        verify(mockedProducer);
    }

    @Test
    public void shouldForwardCallToFlush() {
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        mockedProducer.flush();
        expectLastCall();
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, null, logContext);

        streamsProducer.flush();

        verify(mockedProducer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldForwardCallToMetrics() {
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        final Map expected = new HashMap<>();
        expect(mockedProducer.metrics()).andReturn(expected);
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, null, new LogContext());

        final Map<MetricName, ? extends Metric> metrics = streamsProducer.metrics();

        verify(mockedProducer);

        assertThat(expected, equalTo(metrics));
    }

    @Test
    public void shouldCloseKafkaProducerOnClose() {
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        mockedProducer.close();
        expectLastCall();
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, null, new LogContext());
        streamsProducer.close();

        verify(mockedProducer);
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
        mockProducer.sendException  = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(mockProducer.sendException));
        assertThat(thrown.getMessage(), is("Error encountered trying to send record to topic topic [test]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnCloseError() {
        mockProducer.closeException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            nonEosStreamsProducer::close
        );

        assertThat(thrown.getCause(), is(mockProducer.closeException));
        assertThat(thrown.getMessage(), is("Error encountered trying to close the producer [test]"));
    }

    @Test
    public void shouldFailOnSendFatal() {
        mockProducer.sendException = new RuntimeException("KABOOM!");

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

    @Test
    public void shouldFailOnCloseFatal() {
        mockProducer.closeException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            nonEosStreamsProducer::close
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
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
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        mockedProducer.sendOffsetsToTransaction(offsetsAndMetadata, "appId");
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, "appId", logContext);
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
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        mockedProducer.initTransactions();
        expectLastCall();
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, "appId", logContext);
        streamsProducer.initTransaction();

        streamsProducer.abortTransaction();

        verify(mockedProducer);
    }

    // error handling tests

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new TimeoutException("KABOOM!");
        final StreamsProducer streamsProducer = new StreamsProducer(mockProducer, "appId", logContext);

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
        final StreamsProducer streamsProducer = new StreamsProducer(mockProducer, "appId", logContext);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getCause(), is(mockProducer.initTransactionException));
        assertThat(thrown.getMessage(), is("Error encountered trying to initialize transactions [test]"));
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        // use `mockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        mockProducer.initTransactionException = new RuntimeException("KABOOM!");
        final StreamsProducer streamsProducer = new StreamsProducer(mockProducer, "appId", logContext);

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
        final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);

        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.abortTransaction();
        expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(mockedProducer, "appId", logContext);
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
