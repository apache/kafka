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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * This test asserts that when Kafka Streams is closing and shuts
 * down a StreamThread the closing of the GlobalStreamThread happens
 * after all the StreamThreads are completely stopped.
 *
 * The test validates the Processor still has access to the GlobalStateStore while closing.
 * Otherwise if the GlobalStreamThread were to close underneath the StreamThread
 * an exception would be thrown as the GlobalStreamThread closes all global stores on closing.
 */
@Timeout(600)
@Tag("integration")
public class GlobalThreadShutDownOrderTest {
    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG;

    static {
        BROKER_CONFIG = new Properties();
        BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
        BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
    }

    private final AtomicInteger closeCounter = new AtomicInteger(0);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private final MockTime mockTime = CLUSTER.time;
    private final String globalStore = "globalStore";
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String globalStoreTopic;
    private String streamTopic;
    private final List<Long> retrievedValuesList = new ArrayList<>();
    private boolean firstRecordProcessed;

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(testInfo);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);

        final Consumed<String, Long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());

        final KeyValueStoreBuilder<String, Long> storeBuilder = new KeyValueStoreBuilder<>(
            Stores.persistentKeyValueStore(globalStore),
            Serdes.String(),
            Serdes.Long(),
            mockTime);

        final ProcessorSupplier<String, Long, Void, Void> processorSupplier;
        processorSupplier = () -> new ContextualProcessor<String, Long, Void, Void>() {
            @Override
            public void process(final Record<String, Long> record) {
                final KeyValueStore<String, Long> stateStore =
                    context().getStateStore(storeBuilder.name());
                stateStore.put(
                    record.key(),
                    record.value()
                );
            }
        };

        builder.addGlobalStore(
            storeBuilder,
            globalStoreTopic,
            Consumed.with(Serdes.String(), Serdes.Long()),
            processorSupplier
        );

        builder
            .stream(streamTopic, stringLongConsumed)
            .process(() -> new GlobalStoreProcessor(globalStore));

    }

    @AfterEach
    public void after() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldFinishGlobalStoreOperationOnShutDown() throws Exception {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        populateTopics(globalStoreTopic);
        populateTopics(streamTopic);

        kafkaStreams.start();

        TestUtils.waitForCondition(
            () -> firstRecordProcessed,
            30000,
            "Has not processed record within 30 seconds");

        kafkaStreams.close(Duration.ofSeconds(30));

        final List<Long> expectedRetrievedValues = Arrays.asList(1L, 2L, 3L, 4L);
        assertEquals(expectedRetrievedValues, retrievedValuesList);
        assertEquals(1, closeCounter.get());
    }


    private void createTopics() throws Exception {
        streamTopic = "stream-topic";
        globalStoreTopic = "global-store-topic";
        CLUSTER.createTopics(streamTopic);
        CLUSTER.createTopic(globalStoreTopic);
    }


    private void populateTopics(final String topicName) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topicName,
            Arrays.asList(
                new KeyValue<>("A", 1L),
                new KeyValue<>("B", 2L),
                new KeyValue<>("C", 3L),
                new KeyValue<>("D", 4L)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                LongSerializer.class,
                new Properties()),
            mockTime);
    }


    private class GlobalStoreProcessor implements Processor<String, Long, Void, Void> {

        private KeyValueStore<String, Long> store;
        private final String storeName;

        GlobalStoreProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public void process(final Record<String, Long> record) {
            firstRecordProcessed = true;
        }


        @Override
        public void close() {
            closeCounter.getAndIncrement();
            final List<String> keys = Arrays.asList("A", "B", "C", "D");
            for (final String key : keys) {
                // need to simulate thread slow in closing
                Utils.sleep(1000);
                retrievedValuesList.add(store.get(key));
            }
        }
    }

}
