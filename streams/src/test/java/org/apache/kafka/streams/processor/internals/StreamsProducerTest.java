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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.MockClientSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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

    @SuppressWarnings("deprecation")
    private final StreamsConfig eosAlphaConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE))
    );

    private final StreamsConfig eosBetaConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2))
    );

    private final Time mockTime = mock(Time.class);

    @SuppressWarnings("unchecked")
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
        null,
        logContext,
        mockTime
    );
    final StreamsProducer eosAlphaStreamsProducerWithMock = new StreamsProducer(
        eosAlphaConfig,
        "threadId",
        clientSupplier,
        new TaskId(0, 0),
        null,
        logContext,
        mockTime
    );

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private StreamsProducer nonEosStreamsProducer;
    private MockProducer<byte[], byte[]> nonEosMockProducer;

    private final MockClientSupplier eosAlphaMockClientSupplier = new MockClientSupplier();
    private StreamsProducer eosAlphaStreamsProducer;
    private MockProducer<byte[], byte[]> eosAlphaMockProducer;

    private final MockClientSupplier eosBetaMockClientSupplier = new MockClientSupplier();
    private StreamsProducer eosBetaStreamsProducer;
    private MockProducer<byte[], byte[]> eosBetaMockProducer;

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
                null,
                logContext,
                mockTime
            );
        nonEosMockProducer = mockClientSupplier.producers.get(0);

        eosAlphaMockClientSupplier.setCluster(cluster);
        eosAlphaMockClientSupplier.setApplicationIdForProducer("appId");
        eosAlphaStreamsProducer =
            new StreamsProducer(
                eosAlphaConfig,
                "threadId-StreamThread-0",
                eosAlphaMockClientSupplier,
                new TaskId(0, 0),
                null,
                logContext,
                mockTime
            );
        eosAlphaStreamsProducer.initTransaction();
        eosAlphaMockProducer = eosAlphaMockClientSupplier.producers.get(0);

        eosBetaMockClientSupplier.setCluster(cluster);
        eosBetaMockClientSupplier.setApplicationIdForProducer("appId");
        eosBetaStreamsProducer =
            new StreamsProducer(
                eosBetaConfig,
                "threadId-StreamThread-0",
                eosBetaMockClientSupplier,
                null,
                UUID.randomUUID(),
                logContext,
                mockTime
            );
        eosBetaStreamsProducer.initTransaction();
        eosBetaMockProducer = eosBetaMockClientSupplier.producers.get(0);
        when(mockTime.nanoseconds()).thenReturn(Time.SYSTEM.nanoseconds());
    }



    // common tests (non-EOS and EOS-alpha/beta)

    // functional tests

    @Test
    public void shouldResetTransactionInFlightOnClose() {
        // given:
        eosBetaStreamsProducer.send(
            new ProducerRecord<>("topic", new byte[1]), (metadata, error) -> { });
        assertThat(eosBetaStreamsProducer.transactionInFlight(), is(true));

        // when:
        eosBetaStreamsProducer.close();

        // then:
        assertThat(eosBetaStreamsProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldResetTransactionInFlightOnReset() {
        // given:
        eosBetaStreamsProducer.send(
            new ProducerRecord<>("topic", new byte[1]), (metadata, error) -> { });
        assertThat(eosBetaStreamsProducer.transactionInFlight(), is(true));

        // when:
        eosBetaStreamsProducer.resetProducer();

        // then:
        assertThat(eosBetaStreamsProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldCreateProducer() {
        assertThat(mockClientSupplier.producers.size(), is(1));
        assertThat(eosAlphaMockClientSupplier.producers.size(), is(1));
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
    public void shouldFailIfStreamsConfigIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                null,
                "threadId",
                mockClientSupplier,
                new TaskId(0, 0),
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
                new TaskId(0, 0),
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
                new TaskId(0, 0),
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
                new TaskId(0, 0),
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

    @Test
    public void shouldFailOnResetProducerForExactlyOnceAlpha() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> eosAlphaStreamsProducer.resetProducer()
        );

        assertThat(thrown.getMessage(), is("Expected eos-v2 to be enabled, but the processing mode was EXACTLY_ONCE_ALPHA"));
    }


    // non-EOS tests

    // functional tests

    @Test
    public void shouldNotSetTransactionIdIfEosDisabled() {
        final Map<String, Object> producerConfig = new HashMap<>();
        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        when(mockConfig.getProducerConfigs("threadId-producer")).thenReturn(producerConfig);
        when(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).thenReturn(StreamsConfig.AT_LEAST_ONCE);

        new StreamsProducer(
            mockConfig,
            "threadId",
            mockClientSupplier,
            null,
            null,
            logContext,
            mockTime
        );

        assertFalse(producerConfig.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
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

    @SuppressWarnings("deprecation")
    @Test
    public void shouldSetTransactionIdUsingTaskIdIfEosAlphaEnabled() {
        final Map<String, Object> producerConfig = new HashMap<>();
        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        when(mockConfig.getProducerConfigs("threadId-0_0-producer")).thenReturn(producerConfig);
        when(mockConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("appId");
        when(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).thenReturn(StreamsConfig.EXACTLY_ONCE);

        new StreamsProducer(
            mockConfig,
            "threadId",
            eosAlphaMockClientSupplier,
            new TaskId(0, 0),
            null,
            logContext,
            mockTime
        );

        assertEquals("appId-0_0", producerConfig.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    public void shouldSetTransactionIdUsingProcessIdIfEosV2Enabled() {
        final UUID processId = UUID.randomUUID();
        final Map<String, Object> producerConfig = new HashMap<>();
        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        when(mockConfig.getProducerConfigs("threadId-StreamThread-0-producer")).thenReturn(producerConfig);
        when(mockConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("appId");
        when(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).thenReturn(StreamsConfig.EXACTLY_ONCE_V2);

        new StreamsProducer(
            mockConfig,
            "threadId-StreamThread-0",
            eosAlphaMockClientSupplier,
            null,
            processId,
            logContext,
            mockTime
        );

        assertEquals("appId-" + processId + "-0", producerConfig.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    public void shouldNotHaveEosEnabledIfEosAlphaEnable() {
        assertThat(eosAlphaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldHaveEosEnabledIfEosBetaEnabled() {
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
        eosAlphaStreamsProducerWithMock.initTransaction();
        eosAlphaStreamsProducerWithMock.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
        verify(mockedProducer).sendOffsetsToTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        verify(mockedProducer).commitTransaction();
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
        when(mockedProducer.send(record, null)).thenReturn(null);

        eosAlphaStreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosAlphaStreamsProducerWithMock.send(record, null);
        eosAlphaStreamsProducerWithMock.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
        verify(mockedProducer).sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        verify(mockedProducer).commitTransaction();
    }

    @Test
    public void shouldCommitTxWithConsumerGroupMetadataOnEosBetaCommit() {
        when(mockedProducer.send(record, null)).thenReturn(null);

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosBetaConfig,
            "threadId-StreamThread-0",
            clientSupplier,
            null,
            UUID.randomUUID(),
            logContext,
            mockTime
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
        eosAlphaStreamsProducerWithMock.initTransaction();
        eosAlphaStreamsProducerWithMock.abortTransaction();

        verify(mockedProducer).initTransactions();
    }

    // error handling tests

    @Test
    public void shouldFailIfTaskIdIsNullForEosAlpha() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                eosAlphaConfig,
                "threadId",
                mockClientSupplier,
                null,
                UUID.randomUUID(),
                logContext,
                mockTime)
        );

        assertThat(thrown.getMessage(), is("taskId cannot be null for exactly-once alpha"));
    }

    @Test
    public void shouldFailIfProcessIdNullForEosBeta() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                eosBetaConfig,
                "threadId",
                mockClientSupplier,
                new TaskId(0, 0),
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
            eosAlphaConfig,
            "threadId",
            clientSupplier,
            new TaskId(0, 0),
            null,
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
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceAlpha() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(
                eosAlphaConfig,
                "threadId",
                eosAlphaMockClientSupplier,
                new TaskId(0, 0),
                null,
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
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceBeta() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(
                eosBetaConfig,
                "threadId-StreamThread-0",
                eosBetaMockClientSupplier,
                null,
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
            eosAlphaConfig,
            "threadId",
            clientSupplier,
            new TaskId(0, 0),
            null,
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
            eosAlphaConfig,
            "threadId",
            clientSupplier,
            new TaskId(0, 0),
            null,
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
        eosAlphaMockProducer.sendOffsetsToTransactionException = exception;

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
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithProducerFenced() {
        testThrowTaskMigratedExceptionOnEos(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosCommitWithInvalidEpoch() {
        testThrowTaskMigratedExceptionOnEos(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testThrowTaskMigratedExceptionOnEos(final RuntimeException exception) {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.commitTransactionException = exception;

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
    public void shouldSwallowExceptionOnEosAbortTxProducerFenced() {
        testSwallowExceptionOnEosAbortTx(new ProducerFencedException("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxInvalidEpoch() {
        testSwallowExceptionOnEosAbortTx(new InvalidProducerEpochException("KABOOM!"));
    }

    private void testSwallowExceptionOnEosAbortTx(final RuntimeException exception) {
        when(mockedProducer.send(record, null)).thenReturn(null);
        doThrow(exception).when(mockedProducer).abortTransaction();

        eosAlphaStreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosAlphaStreamsProducerWithMock.send(record, null);
        eosAlphaStreamsProducerWithMock.abortTransaction();

        verify(mockedProducer).initTransactions();
        verify(mockedProducer).beginTransaction();
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
    public void shouldCloseExistingProducerOnResetProducer() {
        eosBetaStreamsProducer.resetProducer();

        assertTrue(eosBetaMockProducer.closed());
    }

    @Test
    public void shouldSetNewProducerOnResetProducer() {
        eosBetaStreamsProducer.resetProducer();

        assertThat(eosBetaMockClientSupplier.producers.size(), is(2));
        assertThat(eosBetaStreamsProducer.kafkaProducer(), is(eosBetaMockClientSupplier.producers.get(1)));
    }

    @Test
    public void shouldResetTransactionInitializedOnResetProducer() {
        final StreamsProducer streamsProducer = new StreamsProducer(
            eosBetaConfig,
            "threadId-StreamThread-0",
            clientSupplier,
            null,
            UUID.randomUUID(),
            logContext,
            mockTime
        );
        streamsProducer.initTransaction();

        when(mockedProducer.metrics()).thenReturn(Collections.emptyMap());

        streamsProducer.resetProducer();
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
            eosBetaMockProducer,
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
        assertThat(eosBetaStreamsProducer.totalBlockedTime(), equalTo(expectedTotalBlocked));
        final long closeStart = 1L;
        final long clodeDelay = 1L;
        when(mockTime.nanoseconds()).thenReturn(closeStart).thenReturn(closeStart + clodeDelay);
        eosBetaStreamsProducer.resetProducer();
        setProducerMetrics(
            eosBetaMockClientSupplier.producers.get(1),
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
            eosBetaStreamsProducer.totalBlockedTime(),
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
