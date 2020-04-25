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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.fail;

/**
 * Test the standby task fail over scenario. Never call commit but process a poison key that causes primary task failed.
 * For at least once, the poison record will be replicated to the standby task state.
 * In EOS, we should not hit the duplicate processing exception on the poison key, as the
 * restore consumer is also read committed.
 */
@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class StandbyTaskFailOverIntegrationTest {

    private final Logger log = LoggerFactory.getLogger(StandbyTaskFailOverIntegrationTest.class);

    private static final int NUM_BROKERS = 3;
    private static final Duration RETENTION = Duration.ofMillis(100_000);
    private static final Duration WINDOW_SIZE = Duration.ofMillis(100);
    private static final String STORE_NAME = "dedup-store";

    private final String appId = "test-app";
    private final String inputTopic = "input";
    private final String keyOne = "key_one";
    private final String poisonKey = "poison_key";
    private final int numThreads = 2;
    private KafkaStreams streamInstanceOne;
    private KafkaStreams streamInstanceTwo;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false"))
    );

    @Parameterized.Parameter
    public String eosConfig;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
            {StreamsConfig.AT_LEAST_ONCE},
            {StreamsConfig.EXACTLY_ONCE}
        });
    }

    @Before
    public void createTopics() throws Exception {
        cleanStateBeforeTest(CLUSTER);
        CLUSTER.createTopic(inputTopic, 1, 1);
    }

    @Test
    public void testStandbyTaskFailOver() throws Exception {

        CountDownLatch waitPoisonRecordReplication = new CountDownLatch(1);

        streamInstanceOne = getStreamInstance(1, waitPoisonRecordReplication);
        streamInstanceTwo = getStreamInstance(2, null);

        CountDownLatch threadDeaths = new CountDownLatch(3);
        streamInstanceOne.setUncaughtExceptionHandler((t, e) -> {
            if (e.getMessage().startsWith("Caught a duplicate key") && eosConfig.equals(StreamsConfig.EXACTLY_ONCE)) {
                fail("Should not hit duplicate key in EOS");
            }
            threadDeaths.countDown();
        });
        streamInstanceTwo.setUncaughtExceptionHandler((t, e) ->{
            if (e.getMessage().startsWith("Caught a duplicate key") && eosConfig.equals(StreamsConfig.EXACTLY_ONCE)) {
                fail("Should not hit duplicate key in EOS");
            }
            threadDeaths.countDown();
        });

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(keyOne, "value")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            10L);

        // Start instance one first to make sure it gets the task assignment.
        streamInstanceOne.start();
        waitForCondition(() -> streamInstanceOne.state().equals(KafkaStreams.State.RUNNING),
            "Stream instance one should be up and running by now");

        log.info("Stream instance one starts up");

        streamInstanceTwo.start();
        waitForCondition(() -> streamInstanceTwo.state().equals(KafkaStreams.State.RUNNING),
            "Stream instance two should be up and running by now");

        log.info("Stream instance two starts up, producing the poison record");
        // Produce the poison record
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(poisonKey, "value")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            20L);

        if (eosConfig.equals(StreamsConfig.AT_LEAST_ONCE)) {
            final QueryableStoreType<ReadOnlyWindowStore<String, String>> queryableStoreType = QueryableStoreTypes.windowStore();
            waitForCondition(() -> {
                ReadOnlyWindowStore<String, String> instanceTwoWindowStore =
                    streamInstanceTwo.store(StoreQueryParameters.fromNameAndType(STORE_NAME, queryableStoreType).enableStaleStores());

                final KeyValueIterator<Windowed<String>, String> iterator = instanceTwoWindowStore.all();
                while (iterator.hasNext()) {
                    String key = iterator.next().key.key();
                    if (key.equals(poisonKey)) {
                        waitPoisonRecordReplication.countDown();
                        return true;
                    }
                }
                return false;
            }, "Did not see poison key replicated to instance two");
        } else {
            // Wait sufficient time to make sure the data is not replicated.
            Thread.sleep(3000);
        }

        threadDeaths.await(15, TimeUnit.SECONDS);
    }

    private KafkaStreams getStreamInstance(final int instanceId, CountDownLatch waitPoisonRecordReplication) {
        WindowBytesStoreSupplier storeSupplier = Stores.persistentWindowStore(STORE_NAME,
            RETENTION,
            WINDOW_SIZE,
            true);
        StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.windowStoreBuilder(storeSupplier, Serdes.String(), Serdes.String())
                                                                     .withLoggingEnabled(Collections.emptyMap());

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder);

        builder.stream(inputTopic,
            Consumed.with(Serdes.String(), Serdes.String())).transform(
            () -> new Transformer<String, String, KeyValue<String, String>>() {
                private WindowStore<String, String> dedupStore;
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.dedupStore = (WindowStore<String, String>) context.getStateStore(STORE_NAME);
                }

                @Override
                public KeyValue<String, String> transform(String key, String value) {
                    long timestamp = context.timestamp();
                    final WindowStoreIterator<String> storeIterator = dedupStore.fetch(key, timestamp - WINDOW_SIZE.toMillis(), timestamp);
                    if (storeIterator.hasNext()) {
                        throw new IllegalStateException("Caught a duplicate key " + key);
                    }
                    dedupStore.put(key, value, timestamp);

                    return new KeyValue<>(key, value);
                }

                @Override
                public void close() {
                }
            }, STORE_NAME
        ).peek((key, value) -> {
            if (key.equals(poisonKey)) {
                try {
                    if (waitPoisonRecordReplication != null) {
                        waitPoisonRecordReplication.await(15_000L, TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException ignored) {
                }
                throw new IllegalStateException("Throw on key_two to trigger rebalance");
            }
        });

        return new KafkaStreams(builder.build(),
            props(numThreads, String.format("/tmp/kafka-streams/instance-%d/", instanceId)));
    }

    @After
    public void shutdown() {
        if (streamInstanceOne != null) {
            streamInstanceOne.close(Duration.ofSeconds(30L));
            streamInstanceOne.cleanUp();
        }

        if (streamInstanceTwo != null) {
            streamInstanceTwo.close(Duration.ofSeconds(30L));
            streamInstanceTwo.cleanUp();
        }
    }

    private Properties props(final int numThreads, final String stateDirPath) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        // Set commit interval long to avoid actually committing the record.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        return streamsConfiguration;
    }
}
