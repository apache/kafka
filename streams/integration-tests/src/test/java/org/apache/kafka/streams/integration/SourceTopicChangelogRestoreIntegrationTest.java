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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * KAFKA-20416: a KTable that reuses its source topic as the changelog, written back to by the same task, under EOS.
 */
@Tag("integration")
@Timeout(600)
public class SourceTopicChangelogRestoreIntegrationTest {

    private static final int NUM_PRELOADED_KEYS = 5_000;
    private static final int NUM_EVENTS = 100;
    private static final String TABLE_TOPIC = "vehicle-state";
    private static final String EVENTS_TOPIC = "vehicle-events";
    private static final String TABLE_STORE = "shared-vehicle-store";
    private static final String EVENT_COUNT_STORE = "event-counts";
    private static final TopicPartition TABLE_PARTITION = new TopicPartition(TABLE_TOPIC, 0);
    private static final TaskId TASK = new TaskId(0, 0);
    private static final long PRODUCER_MAX_BLOCK_MS = 2_000L;
    private static final Duration COMMIT_DELAY = Duration.ofMillis(PRODUCER_MAX_BLOCK_MS + 1_000L);
    private static final Duration RESTORE_FETCH_DELAY = Duration.ofSeconds(2);
    private static final long WAIT_MS = 120_000L;

    private final Map<String, String> stateDirs = new HashMap<>();
    private final List<KafkaStreams> streamsToClose = new ArrayList<>();
    private final Map<KafkaStreams, AtomicInteger> rebalanceCounts = new HashMap<>();
    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String appId;

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        appId = "source-changelog-" + safeUniqueTestName(testInfo);
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        cluster.createTopic(TABLE_TOPIC, 1, 1);
        cluster.createTopic(EVENTS_TOPIC, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());

        final List<KeyValue<String, String>> vehicles = new ArrayList<>();
        for (final String key : preloadedKeys()) {
            vehicles.add(KeyValue.pair(key, "initial-state"));
        }
        produce(TABLE_TOPIC, vehicles);
    }

    @AfterEach
    public void tearDown() {
        streamsToClose.forEach(streams -> streams.close(Duration.ofSeconds(30)));
        if (proxy != null) {
            proxy.close();
        }
        cluster.stop();
    }

    @Test
    public void shouldFullyRestoreSourceTopicChangelogStoreAfterTaskCorruptionWithStandbyReplica() throws Exception {
        final KafkaStreams instanceA = start("instance-a");
        waitForTableKeys(instanceA, false, preloadedKeys());
        // enough updates on the dedicated changelog that a wiped copy of the task is not caught up
        final List<KeyValue<String, String>> events = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
            events.add(KeyValue.pair("event-" + i, "update"));
        }
        produce(EVENTS_TOPIC, events);
        waitForTableKeys(instanceA, false, keysOf(events));

        final KafkaStreams instanceB = start("instance-b");
        waitForPlacement(instanceA, instanceB);
        waitForTableKeys(instanceB, true, keysOf(events));

        // corrupt instance-a's active task while its restore consumer is stalled
        final int rebalancesBeforeCorruption = rebalanceCounts.get(instanceA).get();
        proxy.delayOn(ApiKeys.FETCH, RESTORE_FETCH_DELAY).forClient(restoreConsumer("instance-a")).everyTime();
        final FaultRule commitTimeout =
            proxy.delayOn(ApiKeys.TXN_OFFSET_COMMIT, COMMIT_DELAY).forClient(producer("instance-a")).once();
        produce(EVENTS_TOPIC, List.of(KeyValue.pair("event-trigger", "update")));
        TestUtils.waitForCondition(
            () -> rebalanceCounts.get(instanceA).get() > rebalancesBeforeCorruption,
            WAIT_MS,
            "instance-a never rebalanced after its task was corrupted"
        );
        // the stale offsets can make the wiped copy look caught up, so the active may move away or come straight back
        final boolean activeCameStraightBack = waitForStablePlacement(instanceA, instanceB) == instanceA;
        proxy.clearFaults();

        produce(EVENTS_TOPIC, List.of(KeyValue.pair("marker-2", "update")));
        waitForTableKeys(instanceA, true, List.of("marker-2"));
        if (!activeCameStraightBack) {
            closeAndLeaveGroup(instanceB);
            waitForPlacement(instanceA, null);
        }
        final int restoredOnInstanceA = countPresentKeys(instanceA, false, preloadedKeys());

        assertAll(
            () -> assertEquals(
                1,
                commitTimeout.timesTriggered(),
                "the commit timeout should fire exactly once to corrupt instance-a's active task"
            ),
            () -> assertEquals(
                NUM_PRELOADED_KEYS,
                restoredOnInstanceA,
                "instance-a restored " + restoredOnInstanceA + " of the " + NUM_PRELOADED_KEYS + " records in "
                    + TABLE_PARTITION + " into " + TABLE_STORE + " after its task was corrupted"
            )
        );
    }

    // Streams does not leave the group on a plain close, so the other instance would wait out session.timeout.ms to take over
    private static void closeAndLeaveGroup(final KafkaStreams streams) {
        streams.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
            .withTimeout(Duration.ofSeconds(60)));
    }

    private KafkaStreams start(final String clientId) throws Exception {
        final KafkaStreams streams = new KafkaStreams(topology(), streamsConfig(clientId));
        final AtomicInteger rebalances = new AtomicInteger();
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.REBALANCING) {
                rebalances.incrementAndGet();
            }
        });
        rebalanceCounts.put(streams, rebalances);
        streamsToClose.add(streams);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    private static Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            TABLE_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(TABLE_STORE)
        );
        builder.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(EVENT_COUNT_STORE), Serdes.String(), Serdes.Long())
                .withCachingDisabled()
        );

        builder.stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .process(VehicleStateProcessor::new, TABLE_STORE, EVENT_COUNT_STORE)
            .to(TABLE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final Properties topologyConfig = new Properties();
        topologyConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        return builder.build(topologyConfig);
    }

    private Properties streamsConfig(final String clientId) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDirs.computeIfAbsent(clientId, id -> TestUtils.tempDirectory().getPath()));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 50L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // an offset commit that outlives max.block.ms is what TaskExecutor turns into a TaskCorruptedException
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), PRODUCER_MAX_BLOCK_MS);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 60_000);
        // small restore fetches keep a delayed restore consumer from catching up while a step is waiting on it
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), 1024);
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), 1024);
        return props;
    }

    private void produce(final String topic, final List<KeyValue<String, String>> records) throws Exception {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // one record per batch, so a restore fetch capped by max.partition.fetch.bytes only returns a handful of records
        producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, producerConfig, cluster.time);
    }

    private static void waitForPlacement(final KafkaStreams active, final KafkaStreams standby) throws Exception {
        TestUtils.waitForCondition(
            () -> hostsTask(active, true) && (standby == null || hostsTask(standby, false)),
            WAIT_MS,
            "task " + TASK + " never reached the expected active/standby placement"
        );
    }

    private static KafkaStreams waitForStablePlacement(final KafkaStreams first, final KafkaStreams second) throws Exception {
        final AtomicReference<KafkaStreams> active = new AtomicReference<>();
        final AtomicLong observedSinceMs = new AtomicLong();
        TestUtils.waitForCondition(
            () -> {
                final KafkaStreams current;
                if (hostsTask(first, true) && hostsTask(second, false)) {
                    current = first;
                } else if (hostsTask(second, true) && hostsTask(first, false)) {
                    current = second;
                } else {
                    current = null;
                }
                if (current == null || current != active.get()) {
                    active.set(current);
                    observedSinceMs.set(System.currentTimeMillis());
                    return false;
                }
                // a cooperative handoff briefly shows the old placement, so require it to hold before relying on it
                return System.currentTimeMillis() - observedSinceMs.get() >= 2_000L;
            },
            WAIT_MS,
            "task " + TASK + " never settled on one active and one standby"
        );
        return active.get();
    }

    private static boolean hostsTask(final KafkaStreams streams, final boolean active) {
        return streams.state() == KafkaStreams.State.RUNNING && streams.metadataForLocalThreads().stream()
            .flatMap(thread -> (active ? thread.activeTasks() : thread.standbyTasks()).stream())
            .anyMatch(task -> task.taskId().equals(TASK));
    }

    private static void waitForTableKeys(final KafkaStreams streams, final boolean standby, final List<String> keys) throws Exception {
        TestUtils.waitForCondition(
            () -> countPresentKeys(streams, standby, keys) == keys.size(),
            WAIT_MS,
            () -> "the " + (standby ? "standby" : "active") + " table store never contained all " + keys.size() + " expected keys"
        );
    }

    private static int countPresentKeys(final KafkaStreams streams, final boolean standby, final List<String> keys) throws Exception {
        final ReadOnlyKeyValueStore<String, String> store =
            IntegrationTestUtils.getStore(TABLE_STORE, streams, standby, QueryableStoreTypes.keyValueStore());
        int present = 0;
        for (final String key : keys) {
            if (store.get(key) != null) {
                present++;
            }
        }
        return present;
    }

    private static List<String> preloadedKeys() {
        final List<String> keys = new ArrayList<>(NUM_PRELOADED_KEYS);
        for (int i = 0; i < NUM_PRELOADED_KEYS; i++) {
            keys.add("vehicle-" + i);
        }
        return keys;
    }

    private static List<String> keysOf(final List<KeyValue<String, String>> records) {
        final List<String> keys = new ArrayList<>(records.size());
        records.forEach(record -> keys.add(record.key));
        return keys;
    }

    private static String restoreConsumer(final String clientId) {
        return clientId + "-StateUpdater-1-restore-consumer";
    }

    private static String producer(final String clientId) {
        return clientId + "-StreamThread-1-producer";
    }

    private static final class VehicleStateProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private TimestampedKeyValueStore<String, String> vehicleStore;
        private KeyValueStore<String, Long> eventCounts;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
            vehicleStore = context.getStateStore(TABLE_STORE);
            eventCounts = context.getStateStore(EVENT_COUNT_STORE);
        }

        @Override
        public void process(final Record<String, String> record) {
            final ValueAndTimestamp<String> current = vehicleStore.get(record.key());
            final Long count = eventCounts.get(record.key());
            eventCounts.put(record.key(), count == null ? 1L : count + 1L);
            context.forward(record.withValue((current == null ? "created" : current.value()) + "|" + record.value()));
        }
    }
}
