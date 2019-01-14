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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@Category({IntegrationTest.class})
public class GlobalThreadShutDownOrderTest {

    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG;

    static {
        BROKER_CONFIG = new Properties();
        BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
        BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
    }

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    private final MockTime mockTime = CLUSTER.time;
    private final String globalStore = "globalStore";
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String globalStoreTopic;
    private String streamTopic;
    private final List<Long> retrievedValuesList = new ArrayList<>();
    private boolean firstRecordProcessed;

    @Before
    public void before() throws Exception {
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = "global-thread-shutdown-test";
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        final Consumed<String, Long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());

        final KeyValueStoreBuilder<String, Long> storeBuilder = new KeyValueStoreBuilder<>(
            Stores.persistentKeyValueStore(globalStore),
            Serdes.String(),
            Serdes.Long(),
            mockTime);

        builder.addGlobalStore(
            storeBuilder,
            globalStoreTopic,
            Consumed.with(Serdes.String(), Serdes.Long()),
            new MockProcessorSupplier());

        builder
            .stream(streamTopic, stringLongConsumed)
            .process(() -> new GlobalStoreProcessor(globalStore));

    }

    @After
    public void whenShuttingDown() throws Exception {
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


    private class GlobalStoreProcessor extends AbstractProcessor<String, Long> {

        private KeyValueStore<String, Long> store;
        private final String storeName;

        GlobalStoreProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<String, Long>) context.getStateStore(storeName);
        }

        @Override
        public void process(final String key, final Long value) {
            firstRecordProcessed = true;
        }


        @Override
        public void close() {
            final List<String> keys = Arrays.asList("A", "B", "C", "D");
            for (final String key : keys) {
                // need to simulate thread slow in closing
                Utils.sleep(1000);
                retrievedValuesList.add(store.get(key));
            }
        }
    }

}
