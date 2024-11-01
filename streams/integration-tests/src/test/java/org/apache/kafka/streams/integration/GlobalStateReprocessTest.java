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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Timeout(600)
@Tag("integration")
public class GlobalStateReprocessTest {
    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG;

    static {
        BROKER_CONFIG = new Properties();
        BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
        BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
    }

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
                    record.key() + "- this is the right value.",
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
    }

    @AfterEach
    public void after() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldReprocessWithUserProvidedStore() throws Exception {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        populateTopics(globalStoreTopic);

        kafkaStreams.start();

        TestUtils.waitForCondition(
            () -> !storeContents(kafkaStreams).isEmpty(),
            30000,
            "Has not processed record within 30 seconds");

        assertThat(storeContents(kafkaStreams).get(0), containsString("- this is the right value."));


        kafkaStreams.close();
        kafkaStreams.cleanUp();

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        TestUtils.waitForCondition(
            () -> !storeContents(kafkaStreams).isEmpty(),
            30000,
            "Has not processed record within 30 seconds");

        assertThat(storeContents(kafkaStreams).get(0), containsString("- this is the right value."));
    }

    private void createTopics() throws Exception {
        globalStoreTopic = "global-store-topic";
        CLUSTER.createTopic(globalStoreTopic);
    }

    private void populateTopics(final String topicName) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topicName,
            Collections.singletonList(new KeyValue<>("A", 1L)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                LongSerializer.class,
                new Properties()),
            mockTime);
    }

    private List<String> storeContents(final KafkaStreams streams) {
        final ArrayList<String> keySet = new ArrayList<>();
        final ReadOnlyKeyValueStore<String, Long> keyValueStore =
            streams.store(StoreQueryParameters.fromNameAndType(globalStore, QueryableStoreTypes.keyValueStore()));
        final KeyValueIterator<String, Long> range = keyValueStore.reverseAll();
        while (range.hasNext()) {
            keySet.add(range.next().key);
        }
        range.close();
        return keySet;
    }
}
