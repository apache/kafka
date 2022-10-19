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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.test.MockClientSupplier;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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

    private final StreamsConfig nonEosConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"))
    );

    private final StreamsConfig eosV2Config = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2))
    );

    private final Time mockTime = mock(Time.class);

    final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);
    final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
        @Override
        public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
            return mockedProducer;
        }
    };
    final StreamsProducer streamsProducerWithMock = new StreamsProducer(
        nonEosConfig,
        "threadId",
        clientSupplier,
        null,
        logContext,
        mockTime
    );
    final StreamsProducer eosV2StreamsProducerWithMock = new StreamsProducer(
        eosV2Config,
        "threadId-StreamThread-0",
        clientSupplier,
        UUID.randomUUID(),
        logContext,
        mockTime
    );

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private StreamsProducer nonEosStreamsProducer;
    private MockProducer<byte[], byte[]> nonEosMockProducer;

    private final MockClientSupplier eosV2MockClientSupplier = new MockClientSupplier();
    private StreamsProducer eosV2StreamsProducer;
    private MockProducer<byte[], byte[]> eosV2MockProducer;

    private final ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, 0, 0L, new byte[0], new byte[0], new RecordHeaders());

    private final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mkMap(
        mkEntry(new TopicPartition(topic, 0), new OffsetAndMetadata(0L, null))
    );

    @Before
    public void before() {
        mockClientSupplier.setCluster(cluster);
        nonEosStreamsProducer =
            new StreamsProducer(
                nonEosConfig,
                "threadId-StreamThread-0",
                mockClientSupplier,
                null,
                logContext,
                mockTime
            );
        nonEosMockProducer = mockClientSupplier.producers.get(0);

        eosV2MockClientSupplier.setCluster(cluster);
        eosV2MockClientSupplier.setApplicationIdForProducer("appId");
        eosV2StreamsProducer =
            new StreamsProducer(
                eosV2Config,
                "threadId-StreamThread-0",
                eosV2MockClientSupplier,
                UUID.randomUUID(),
                logContext,
                mockTime
            );
        eosV2StreamsProducer.initTransaction();
        eosV2MockProducer = eosV2MockClientSupplier.producers.get(0);
        expect(mockTime.nanoseconds()).andAnswer(Time.SYSTEM::nanoseconds).anyTimes();
        replay(mockTime);
    }

    // common tests (non-EOS and EOS)

    // functional tests

    @Test
    public void shouldCreateProducer() {
        assertThat(mockClientSupplier.producers.size(), is(1));
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(mockedProducer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(mockedProducer);

        final List<PartitionInfo> partitionInfo = streamsProducerWithMock.partitionsFor(topic);

        assertThat(partitionInfo, sameInstance(expectedPartitionInfo));
        verify(mockedProducer);
    }

    @Test
    public void shouldForwardCallToFlush() {
        mockedProducer.flush();
        expectLastCall();
        replay(mockedProducer);

        streamsProducerWithMock.flush();

        verify(mockedProducer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldForwardCallToMetrics() {
        final Map metrics = new HashMap<>();
        expect(mockedProducer.metrics()).andReturn(metrics);
        replay(mockedProducer);

        assertSame(metrics, streamsProducerWithMock.metrics());

        verify(mockedProducer);
    }

    @Test
    public void shouldForwardCallToClose() {
        mockedProducer.close();
        expectLastCall();
        replay(mockedProducer);

        streamsProducerWithMock.close();

        verify(mockedProducer);
    }

    // error handling tests

    @Test
    public void shouldFailIfStreamsConfigIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                null,
                "threadId",
                mockClientSupplier,
                UUID.randomUUID(),
                logContext,
                mockTime)
        );

        assertThat(thrown.getMessage(), is("config cannot be null"));
    }

    @Test
    public void shouldFailIfThreadIdIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                nonEosConfig,
                null,
                mockClientSupplier,
                UUID.randomUUID(),
                logContext,
                mockTime)
        );

        assertThat(thrown.getMessage(), is("threadId cannot be null"));
    }

    @Test
    public void shouldFailIfClientSupplierIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                nonEosConfig,
                "threadId",
                null,
                UUID.randomUUID(),
                logContext,
                mockTime)
        );

        assertThat(thrown.getMessage(), is("clientSupplier cannot be null"));
    }

    @Test
    public void shouldFailIfLogContextIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                nonEosConfig,
                "threadId",
                mockClientSupplier,
                UUID.randomUUID(),
                null,
                mockTime)
        );

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldFailOnResetProducerForAtLeastOnce() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.resetProducer()
        );

        assertThat(thrown.getMessage(), is("Expected eos-v2 to be enabled, but the processing mode was AT_LEAST_ONCE"));
    }

    // non-EOS tests

    // functional tests

    @Test
    public void shouldNotSetTransactionIdIfEosDisabled() {
        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        expect(mockConfig.getProducerConfigs("threadId-producer")).andReturn(mock(Map.class));
        expect(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.AT_LEAST_ONCE).anyTimes();
        replay(mockConfig);

        new StreamsProducer(
            mockConfig,
            "threadId",
            mockClientSupplier,
            null,
            logContext,
            mockTime
        );
    }

    @Test
    public void shouldNotHaveEosEnabledIfEosDisabled() {
        assertThat(nonEosStreamsProducer.eosEnabled(), is(false));
    }

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
    public void shouldEnableEosIfEosV2Enabled() {
        assertThat(eosV2StreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldSetTransactionIdUsingProcessIdIfEosV2Enabled() {
        final UUID processId = UUID.randomUUID();

        final Map<String, Object> mockMap = mock(Map.class);
        expect(mockMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "appId-" + processId + "-0")).andReturn(null);
        expect(mockMap.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).andReturn("appId-" + processId);

        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        expect(mockConfig.getProducerConfigs("threadId-StreamThread-0-producer")).andReturn(mockMap);
        expect(mockConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("appId");
        expect(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.EXACTLY_ONCE_V2).anyTimes();

        replay(mockMap, mockConfig);

        new StreamsProducer(
            mockConfig,
            "threadId-StreamThread-0",
            eosV2MockClientSupplier,
            processId,
            logContext,
            mockTime
        );

        verify(mockMap);
    }

    @Test
    public void shouldHaveEosEnabledIfEosV2Enabled() {
        assertThat(eosV2StreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldInitTxOnEos() {
        assertThat(eosV2MockProducer.transactionInitialized(), is(true));
    }

    @Test
    public void shouldBeginTxOnEosSend() {
        eosV2StreamsProducer.send(record, null);
        assertThat(eosV2MockProducer.transactionInFlight(), is(true));
    }

    @Test
    public void shouldContinueTxnSecondEosSend() {
        eosV2StreamsProducer.send(record, null);
        eosV2StreamsProducer.send(record, null);
        assertThat(eosV2MockProducer.transactionInFlight(), is(true));
        assertThat(eosV2MockProducer.uncommittedRecords().size(), is(2));
    }

    @Test
    public void shouldForwardRecordButNotCommitOnEosSend() {
        eosV2StreamsProducer.send(record, null);
        assertThat(eosV2MockProducer.transactionInFlight(), is(true));
        assertThat(eosV2MockProducer.history().isEmpty(), is(true));
        assertThat(eosV2MockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosV2MockProducer.uncommittedRecords().get(0), is(record));
    }

    @Test
    public void shouldBeginTxOnEosCommit() {
        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        mockedProducer.sendOffsetsToTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        eosV2StreamsProducerWithMock.initTransaction();

        eosV2StreamsProducerWithMock.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer);
    }

    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosV2StreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        assertThat(eosV2MockProducer.sentOffsets(), is(true));
    }

    @Test
    public void shouldCommitTxOnEosCommit() {
        eosV2StreamsProducer.send(record, null);
        assertThat(eosV2MockProducer.transactionInFlight(), is(true));

        eosV2StreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        assertThat(eosV2MockProducer.transactionInFlight(), is(false));
        assertThat(eosV2MockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosV2MockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosV2MockProducer.history().size(), is(1));
        assertThat(eosV2MockProducer.history().get(0), is(record));
        assertThat(eosV2MockProducer.consumerGroupOffsetsHistory().size(), is(1));
        assertThat(eosV2MockProducer.consumerGroupOffsetsHistory().get(0).get("appId"), is(offsetsAndMetadata));
    }

    @Test
    public void shouldCommitTxWithApplicationIdOnEosV2Commit() {
        mockedProducer.initTransactions();
        expectLastCall();
        mockedProducer.beginTransaction();
        expectLastCall();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        eosV2StreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosV2StreamsProducerWithMock.send(record, null);

        eosV2StreamsProducerWithMock.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer);
    }

    @Test
    public void shouldCommitTxWithConsumerGroupMetadataOnEosV2Commit() {
        mockedProducer.initTransactions();
        expectLastCall();
        mockedProducer.beginTransaction();
        expectLastCall();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosV2Config,
            "threadId-StreamThread-0",
            clientSupplier,
            UUID.randomUUID(),
            logContext,
            mockTime
        );
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer);
    }

    @Test
    public void shouldAbortTxOnEosAbort() {
        // call `send()` to start a transaction
        eosV2StreamsProducer.send(record, null);
        assertThat(eosV2MockProducer.transactionInFlight(), is(true));
        assertThat(eosV2MockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosV2MockProducer.uncommittedRecords().get(0), is(record));

        eosV2StreamsProducer.abortTransaction();

        assertThat(eosV2MockProducer.transactionInFlight(), is(false));
        assertThat(eosV2MockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosV2MockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosV2MockProducer.history().isEmpty(), is(true));
        assertThat(eosV2MockProducer.consumerGroupOffsetsHistory().isEmpty(), is(true));
    }

    @Test
    public void shouldSkipAbortTxOnEosAbortIfNotTxInFlight() {
        mockedProducer.initTransactions();
        expectLastCall();
        replay(mockedProducer);

        eosV2StreamsProducerWithMock.initTransaction();

        eosV2StreamsProducerWithMock.abortTransaction();

        verify(mockedProducer);
    }

    // error handling tests

    @Test
    public void shouldFailIfProcessIdNullForEosV2() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                eosV2Config,
                "threadId",
                mockClientSupplier,
                null,
                logContext,
                mockTime)
        );

        assertThat(thrown.getMessage(), is("processId cannot be null for exactly-once v2"));
    }

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new TimeoutException("KABOOM!");
        final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return nonEosMockProducer;
            }
        };

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosV2Config,
            "threadId-StreamThread-0",
            clientSupplier,
            UUID.randomUUID(),
            logContext,
            mockTime
        );

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceV2() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(
                eosV2Config,
                "threadId-StreamThread-0",
                eosV2MockClientSupplier,
                UUID.randomUUID(),
                logContext,
                mockTime
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
        final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return nonEosMockProducer;
            }
        };

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosV2Config,
            "threadId-StreamThread-0",
            clientSupplier,
            UUID.randomUUID(),
            logContext,
            mockTime
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
        final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return nonEosMockProducer;
            }
        };

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosV2Config,
            "threadId-StreamThread-0",
            clientSupplier,
            UUID.randomUUID(),
            logContext,
            mockTime
        );

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnFenced() {
        eosV2MockProducer.fenceProducer();

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosV2StreamsProducer.send(null, null)
        );

        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to begin a new transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnError() {
        eosV2MockProducer.beginTransactionException = new KafkaException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosV2StreamsProducer.send(null, null));

        assertThat(thrown.getCause(), is(eosV2MockProducer.beginTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to begin a new transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosBeginTxnFatal() {
        eosV2MockProducer.beginTransactionException = new RuntimeException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosV2StreamsProducer.send(null, null));

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendProducerFenced() {
        testThrowTaskMigratedExceptionOnEosSend(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendInvalidEpoch() {
        testThrowTaskMigratedExceptionOnEosSend(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnEosSend(final RuntimeException exception) {
        // we need to mimic that `send()` always wraps error in a KafkaException
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosV2MockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosV2StreamsProducer.send(record, null)
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
        eosV2MockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosV2StreamsProducer.send(record, null)
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
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetInvalidEpoch() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        testThrowTaskMigrateExceptionOnEosSendOffset(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigrateExceptionOnEosSendOffset(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosV2MockProducer.sendOffsetsToTransactionException = exception;

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosV2StreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosV2MockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosSendOffsetError() {
        eosV2MockProducer.sendOffsetsToTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosV2StreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosV2MockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosSendOffsetFatal() {
        eosV2MockProducer.sendOffsetsToTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosV2StreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithProducerFenced() {
        testThrowTaskMigratedExceptionOnEos(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithInvalidEpoch() {
        testThrowTaskMigratedExceptionOnEos(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnEos(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosV2MockProducer.commitTransactionException = exception;

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosV2StreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosV2MockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosV2MockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosV2MockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosV2StreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosV2MockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosV2MockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosCommitTxFatal() {
        eosV2MockProducer.commitTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosV2StreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosV2MockProducer.sentOffsets(), is(true));
        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxProducerFenced() {
        testSwallowExceptionOnEosAbortTx(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxInvalidEpoch() {
        testSwallowExceptionOnEosAbortTx(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testSwallowExceptionOnEosAbortTx(final RuntimeException exception) {
        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.abortTransaction();
        expectLastCall().andThrow(exception);
        replay(mockedProducer);

        eosV2StreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosV2StreamsProducerWithMock.send(record, null);

        eosV2StreamsProducerWithMock.abortTransaction();

        verify(mockedProducer);
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosAbortTxError() {
        eosV2MockProducer.abortTransactionException = new KafkaException("KABOOM!");
        // call `send()` to start a transaction
        eosV2StreamsProducer.send(record, null);

        final StreamsException thrown = assertThrows(StreamsException.class, eosV2StreamsProducer::abortTransaction);

        assertThat(thrown.getCause(), is(eosV2MockProducer.abortTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encounter trying to abort a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosAbortTxFatal() {
        eosV2MockProducer.abortTransactionException = new RuntimeException("KABOOM!");
        // call `send()` to start a transaction
        eosV2StreamsProducer.send(record, null);

        final RuntimeException thrown = assertThrows(RuntimeException.class, eosV2StreamsProducer::abortTransaction);

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }


    // EOS test

    // functional tests

    @Test
    public void shouldCloseExistingProducerOnResetProducer() {
        eosV2StreamsProducer.resetProducer();

        assertTrue(eosV2MockProducer.closed());
    }

    @Test
    public void shouldSetNewProducerOnResetProducer() {
        eosV2StreamsProducer.resetProducer();

        assertThat(eosV2MockClientSupplier.producers.size(), is(2));
        assertThat(eosV2StreamsProducer.kafkaProducer(), is(eosV2MockClientSupplier.producers.get(1)));
    }

    @Test
    public void shouldResetTransactionInitializedOnResetProducer() {
        final StreamsProducer streamsProducer = new StreamsProducer(
            eosV2Config,
            "threadId-StreamThread-0",
            clientSupplier,
            UUID.randomUUID(),
            logContext,
            mockTime
        );
        streamsProducer.initTransaction();

        reset(mockedProducer);
        mockedProducer.close();
        mockedProducer.initTransactions();
        expectLastCall();
        expect(mockedProducer.metrics()).andReturn(Collections.emptyMap()).anyTimes();
        replay(mockedProducer);

        streamsProducer.resetProducer();
        streamsProducer.initTransaction();

        verify(mockedProducer);
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
            eosV2MockProducer,
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
        assertThat(eosV2StreamsProducer.totalBlockedTime(), equalTo(expectedTotalBlocked));
        reset(mockTime);
        final long closeStart = 1L;
        final long clodeDelay = 1L;
        expect(mockTime.nanoseconds()).andReturn(closeStart).andReturn(closeStart + clodeDelay);
        replay(mockTime);
        eosV2StreamsProducer.resetProducer();
        setProducerMetrics(
            eosV2MockClientSupplier.producers.get(1),
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
            eosV2StreamsProducer.totalBlockedTime(),
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
