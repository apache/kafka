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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.test.faultproxy.FaultRule;
import org.apache.kafka.test.faultproxy.KafkaProtocolFaultProxy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A commit whose EndTxn outlives {@code max.block.ms} corrupts the task. It is closed dirty, which drops the records it
 * fetched but did not process yet, and revived with its input partitions marked to be reset to the committed offsets.
 * The rebalance enforced because of the corruption completes before the revived task is initialized, so the revived
 * task is closed and replaced by a new one, which has to do that reset. Otherwise the new task starts from the
 * consumer position, and the next commit moves the input offset past the dropped records, which never reach the store.
 */
@Tag("integration")
@Timeout(600)
public class CommitTimeoutOffsetResetIntegrationTest {

    private static final int NUM_RECORDS = 1_500;
    private static final String INPUT_TOPIC = "input";
    private static final TopicPartition INPUT_PARTITION = new TopicPartition(INPUT_TOPIC, 0);
    private static final String STORE = "store";
    private static final String CLIENT_ID = "instance";
    private static final String PRODUCER_CLIENT_ID = CLIENT_ID + "-StreamThread-1-producer";
    private static final long PRODUCER_MAX_BLOCK_MS = 2_000L;
    // outlasts the commit, but not the retry of the pending commit in TaskManager#handleCorruption, so the transaction
    // commits and the producer is usable again, as when the transaction coordinator becomes reachable again
    private static final Duration END_TXN_DELAY = Duration.ofMillis(3 * PRODUCER_MAX_BLOCK_MS / 2);
    // slow enough that the commit lands while most of the fetched records are still buffered in the task
    private static final long PROCESSING_DELAY_MS = 2L;
    private static final long WAIT_MS = 120_000L;

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private Admin admin;
    private KafkaStreams streams;
    private String appId;

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        appId = "commit-timeout-offset-reset-" + safeUniqueTestName(testInfo);
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
        admin = cluster.createAdminClient();
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        if (admin != null) {
            admin.close();
        }
        if (proxy != null) {
            proxy.close();
        }
        cluster.stop();
    }

    @Test
    public void shouldNotSkipInputRecordsIfRevivedTaskIsReplacedBeforeInitialization() throws Exception {
        // all input is there before the first poll, so the first commit happens while most of it is still buffered
        produceUniqueKeys();
        final FaultRule commitTimeout =
            proxy.delayOn(ApiKeys.END_TXN, END_TXN_DELAY).forClient(PRODUCER_CLIENT_ID).once();

        streams = streams();
        final AtomicInteger rebalances = new AtomicInteger();
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.REBALANCING) {
                rebalances.incrementAndGet();
            }
        });
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

        TestUtils.waitForCondition(
            () -> committedInputOffset() == NUM_RECORDS,
            WAIT_MS,
            "the input offset was never committed up to the end of the " + NUM_RECORDS + " input records"
        );
        final int storedKeys = countStoredKeys();

        assertAll(
            () -> assertEquals(
                1,
                commitTimeout.timesTriggered(),
                "the EndTxn delay should fire exactly once to time out the commit"
            ),
            () -> assertTrue(
                rebalances.get() >= 2,
                "the corrupted task should have triggered a rebalance after the initial one, saw " + rebalances.get()
            ),
            () -> assertEquals(
                NUM_RECORDS,
                storedKeys,
                "the input offset was committed up to the end, but only " + storedKeys + " of the " + NUM_RECORDS
                    + " input keys reached " + STORE
            )
        );
    }

    private KafkaStreams streams() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .peek((key, value) -> Utils.sleep(PROCESSING_DELAY_MS))
            .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));
        return new KafkaStreams(builder.build(), streamsConfig());
    }

    private Properties streamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // only the classic protocol enforces a rebalance on corruption, which then replaces the revived task
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // a commit that outlives max.block.ms is what TaskExecutor turns into a TaskCorruptedException; it stays below
        // the default transaction.timeout.ms, so the coordinator does not abort the transaction first
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), PRODUCER_MAX_BLOCK_MS);
        return props;
    }

    private void produceUniqueKeys() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
            TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class))) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                producer.send(new ProducerRecord<>(INPUT_TOPIC, 0, "key-" + i, "value"));
            }
            producer.flush();
        }
    }

    private long committedInputOffset() throws Exception {
        final OffsetAndMetadata committed = admin.listConsumerGroupOffsets(appId)
            .partitionsToOffsetAndMetadata()
            .get()
            .get(INPUT_PARTITION);
        return committed == null ? -1L : committed.offset();
    }

    private int countStoredKeys() throws Exception {
        final ReadOnlyKeyValueStore<String, String> store =
            IntegrationTestUtils.getStore(STORE, streams, QueryableStoreTypes.keyValueStore());
        int count = 0;
        try (KeyValueIterator<String, String> all = store.all()) {
            while (all.hasNext()) {
                all.next();
                count++;
            }
        }
        return count;
    }
}
